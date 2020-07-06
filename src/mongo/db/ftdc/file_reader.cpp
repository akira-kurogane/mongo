/**
 * Copyright (C) 2015 MongoDB Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects
 * for all of the code used other than as permitted herein. If you modify
 * file(s) with this exception, you may extend this exception to your
 * version of the file(s), but you are not obligated to do so. If you do not
 * wish to do so, delete this exception statement from your version. If you
 * delete this exception statement from all source files in the program,
 * then also delete it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/ftdc/file_reader.h"

#include <boost/filesystem.hpp>
#include <fstream>

#include "mongo/base/data_range_cursor.h"
#include "mongo/base/data_type_validated.h"
#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/db/ftdc/config.h"
#include "mongo/db/ftdc/util.h"
#include "mongo/db/ftdc/block_compressor.h"
#include "mongo/db/jsobj.h"
#include "mongo/rpc/object_check.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/db/bson/dotted_path_support.h"

namespace dps = ::mongo::dotted_path_support;

namespace mongo {

FTDCFileReader::~FTDCFileReader() {
    _stream.close();
}

StatusWith<bool> FTDCFileReader::hasNext() {
    while (true) {
        if (_state == State::kNeedsDoc) {
            if (_stream.eof()) {
                return {false};
            }

            auto swDoc = readDocument();
            if (!swDoc.isOK()) {
                return swDoc.getStatus();
            }

            if (swDoc.getValue().isEmpty()) {
                return {false};
            }

            _parent = swDoc.getValue();

            // metadata or metrics?
            auto swType = FTDCBSONUtil::getBSONDocumentType(_parent);
            if (!swType.isOK()) {
                return swType.getStatus();
            }

            auto swId = FTDCBSONUtil::getBSONDocumentId(_parent);
            if (!swId.isOK()) {
                return swId.getStatus();
            }

            _dateId = swId.getValue();

            FTDCBSONUtil::FTDCType type = swType.getValue();

            if (type == FTDCBSONUtil::FTDCType::kMetadata) {
                _state = State::kMetadataDoc;

                auto swMetadata = FTDCBSONUtil::getBSONDocumentFromMetadataDoc(_parent);
                if (!swMetadata.isOK()) {
                    return swMetadata.getStatus();
                }

                _metadata = swMetadata.getValue();
            } else if (type == FTDCBSONUtil::FTDCType::kMetricChunk) {
                _state = State::kMetricChunk;

                auto swDocs = FTDCBSONUtil::getMetricsFromMetricDoc(_parent, &_decompressor);
                if (!swDocs.isOK()) {
                    return swDocs.getStatus();
                }

                _docs = swDocs.getValue();

                // There is always at least the reference document
                _pos = 0;
            }

            return {true};
        }

        // We previously returned a metadata document, now we need another document from disk
        if (_state == State::kMetadataDoc) {
            _state = State::kNeedsDoc;
            continue;
        }

        // If we have a metric chunk, return the next document in the chunk until the chunk is
        // exhausted
        if (_state == State::kMetricChunk) {
            if (_pos + 1 == _docs.size()) {
                _state = State::kNeedsDoc;
                continue;
            }

            _pos++;

            return {true};
        }
    }
}

std::tuple<FTDCBSONUtil::FTDCType, const BSONObj&, Date_t> FTDCFileReader::next() {
    dassert(_state == State::kMetricChunk || _state == State::kMetadataDoc);

    if (_state == State::kMetadataDoc) {
        return std::tuple<FTDCBSONUtil::FTDCType, const BSONObj&, Date_t>(
            FTDCBSONUtil::FTDCType::kMetadata, _metadata, _dateId);
    }

    if (_state == State::kMetricChunk) {
        return std::tuple<FTDCBSONUtil::FTDCType, const BSONObj&, Date_t>(
            FTDCBSONUtil::FTDCType::kMetricChunk, _docs[_pos], _dateId);
    }

    MONGO_UNREACHABLE;
}

StatusWith<BSONObj> FTDCFileReader::readDocument() {
    if (!_stream.is_open()) {
        return {ErrorCodes::FileNotOpen, "open() needs to be called first."};
    }

    char buf[sizeof(std::int32_t)];

    _stream.read(buf, sizeof(buf));

    if (sizeof(buf) != _stream.gcount()) {
        // Did we read exactly zero bytes and hit the eof?
        // Then return an empty document to indicate we are done reading the file.
        if (0 == _stream.gcount() && _stream.eof()) {
            return BSONObj();
        }

        return {ErrorCodes::FileStreamFailed,
                str::stream() << "Failed to read 4 bytes from file \'" << _file.generic_string()
                              << "\'"};
    }

    std::uint32_t bsonLength = ConstDataView(buf).read<LittleEndian<std::int32_t>>();

    // Reads past the end of the file will be caught below
    // The interim file sentinel is 8 bytes of zero.
    if (bsonLength == 0) {
        return BSONObj();
    }

    // Reads past the end of the file will be caught below
    if (bsonLength > _fileSize || bsonLength < BSONObj::kMinBSONLength) {
        return {ErrorCodes::InvalidLength,
                str::stream() << "Invalid BSON length found in file \'" << _file.generic_string()
                              << "\'"};
    }

    // Read the BSON document
    _buffer.resize(bsonLength);

    // Stuff the length into the front
    memcpy(_buffer.data(), buf, sizeof(std::int32_t));

    // Read the length - 4 bytes from the file
    std::int32_t readSize = bsonLength - sizeof(std::int32_t);

    _stream.read(_buffer.data() + sizeof(std::int32_t), readSize);

    if (readSize != _stream.gcount()) {
        return {ErrorCodes::FileStreamFailed,
                str::stream() << "Failed to read " << readSize << " bytes from file \'"
                              << _file.generic_string()
                              << "\'"};
    }

    ConstDataRange cdr(_buffer.data(), _buffer.data() + bsonLength);

    // TODO: Validated only validates objects based on a flag which is the default at the moment
    auto swl = cdr.read<Validated<BSONObj>>();
    if (!swl.isOK()) {
        return swl.getStatus();
    }

    return {swl.getValue().val};
}

Status FTDCFileReader::open(const boost::filesystem::path& file) {
    _stream.open(file.c_str(), std::ios_base::in | std::ios_base::binary);
    if (!_stream.is_open()) {
        return Status(ErrorCodes::FileStreamFailed, "Failed to open file " + file.generic_string());
    }

    _fileSize = boost::filesystem::file_size(file);

    _file = file;

    return Status::OK();
}

StatusWith<std::tuple<BSONObj, uint32_t, uint32_t, Date_t, unsigned long>>
extractRefDocEtcOnly(ConstDataRangeCursor& bdCDR) {

    // The "data" BinData field of a kMetricsChunk contains the following
    // - uint32_t length of the data after decompression
    // - compressed zlib data
    auto swUncompressedLength = bdCDR.readAndAdvance<LittleEndian<std::uint32_t>>();
    if (!swUncompressedLength.isOK()) {
        return {swUncompressedLength.getStatus()};
    }
    auto zlibUncompressedLength = swUncompressedLength.getValue();

    // Block compressor creates, and owns, the buffer the Zlib-
    // uncompressed data is written into. The buffer will have:
    // - One BSONObj (the ref doc)
    // - uint32_t metricCount
    // - uint32_t sampleCount
    // - VarInt data that will expand to (mC * sC) uint64_t values.
    BlockCompressor blockCompressor; // 'Block compressor' = Zlib wrapper class
    auto swUncompress = blockCompressor.uncompress(bdCDR, zlibUncompressedLength);
    if (!swUncompress.isOK()) {
        return {swUncompress.getStatus()};
    }

    // Post-zlib-decompression buffer ConstDataRangeCursor
    ConstDataRangeCursor pzCdc = swUncompress.getValue();

    auto swRef = pzCdc.readAndAdvance<Validated<BSONObj>>();
    if (!swRef.isOK()) {
        return {swRef.getStatus()};
    }
    BSONObj refDoc = swRef.getValue();

    // Read count of metrics
    auto swMetricsCount = pzCdc.readAndAdvance<LittleEndian<std::uint32_t>>();
    if (!swMetricsCount.isOK()) {
        return {swMetricsCount.getStatus()};
    }
    std::uint32_t metricsCount = swMetricsCount.getValue();

    // Read count of samples
    auto swSampleCount = pzCdc.readAndAdvance<LittleEndian<std::uint32_t>>();
    if (!swSampleCount.isOK()) {
        return {swSampleCount.getStatus()};
    }
    std::uint32_t sampleCount = swSampleCount.getValue();

    BSONElement startTSElem = dps::extractElementAtPath(refDoc, "start");
    if (startTSElem.eoo()) {
        return {ErrorCodes::KeyNotFound, "missing 'start' timestamp in a metric sample"};
    }

    BSONElement pidElem = dps::extractElementAtPath(refDoc, "serverStatus.pid");
    if (pidElem.eoo()) {
        return {ErrorCodes::KeyNotFound, "missing 'serverStatus.pid' in a metric sample"};
    }
    unsigned long tmpPid = static_cast<unsigned long>(pidElem.Long());

    //  TODO: proceed into the metrics array and cycle through first row (sampleCount
    //  items long) of VarInts, which are the "start" ts vals, to get the ts of the last
    //  metric chunk so we can return exact lastTs of the metric chunk

    auto result = std::make_tuple(refDoc.getOwned(), metricsCount, sampleCount, startTSElem.Date(), tmpPid);
    return {result};
}

StatusWith<FTDCProcessMetrics> FTDCFileReader::extractProcessMetricsHeaders() {
    FTDCProcessMetrics pm;
    Date_t metadataDocDtId = Date_t::min();
    FTDCFileSpan ftdcFileSpan = { _file, {Date_t::max(), Date_t::min()} };
    bool firstMCProcessed = false;

    std::stack<int> mcFO;
    bool seekMatchingPidMC = false;
    while (true) {
        mcFO.emplace(_stream.tellg()); //pos at start of current doc, before bson read
        StatusWith<BSONObj> swFTDCBsonDoc = readDocument();
        if (!swFTDCBsonDoc.isOK()) {
            return swFTDCBsonDoc.getStatus();
        }

        // Exit when no more docs
        if (swFTDCBsonDoc.getValue().isEmpty()) {
            break;
        }

        BSONElement tmpElem;

        auto ftdcDoc = swFTDCBsonDoc.getValue();
        // The extra 8 bytes is just in case there's an empty doc. Purely conjecture.
        // _stream.tellg() is now the start of the next doc, or 1 past end of file.
        bool isLastDoc = (_fileSize - 8u) <= static_cast<size_t>(_stream.tellg());

        // The Date_t "_id" field is in every FTDC file top-level doc. We'll use
        // the one in the metadata doc as a primary id for the file.
        Status sDateId = bsonExtractTypedField(ftdcDoc, "_id"/*kFTDCIdField*/, BSONType::Date, &tmpElem);
        if (!sDateId.isOK()) {
            return {sDateId};
        }
        auto dtId = tmpElem.Date();

        long long longIntVal;
        Status sType = bsonExtractIntegerField(ftdcDoc, "type"/*kFTDCTypeField*/, &longIntVal);
        if (!sType.isOK()) {
            return {sType};
        }
        FTDCBSONUtil::FTDCType ftdcType = static_cast<FTDCBSONUtil::FTDCType>(longIntVal);

        if (ftdcType == FTDCBSONUtil::FTDCType::kMetadata) {
            mcFO.pop(); //mcFO is just for metrics chunks
            dassert(metadataDocDtId == Date_t::min());
            metadataDocDtId = dtId;

            Status sMDExtract = bsonExtractTypedField(ftdcDoc, "doc"/*kFTDCDocField*/, BSONType::Object, &tmpElem);
            if (!sMDExtract.isOK()) {
                return {sMDExtract};
            }

            pm.metadataDoc = tmpElem.Obj().getOwned();

            BSONElement hpElem = dps::extractElementAtPath(pm.metadataDoc, "hostInfo.system.hostname");
            if (hpElem.eoo()) {
                return {ErrorCodes::KeyNotFound, "missing 'hostInfo.system.hostname' in metadata doc"};
            }
            pm.procId.hostport = hpElem.String();

        } else if (ftdcType == FTDCBSONUtil::FTDCType::kMetricChunk) {

            if (firstMCProcessed && !isLastDoc) {
                // Just skip. We won't need middle metric chunks in a file
                // Devnote: unless we decide later to get sampleCount sum and/or "start" ts value array
                continue;
            }

            Status sZlibBinData = bsonExtractTypedField(ftdcDoc, "data"/*kFTDCDataField*/, BSONType::BinData, &tmpElem);
            if (!sZlibBinData.isOK()) {
                return {sZlibBinData};
            }

            int binDataBSONElemLength;
            const char* buffer = tmpElem.binData(binDataBSONElemLength);
            if (binDataBSONElemLength < 0) {
                return {ErrorCodes::BadValue,
                        str::stream() << "Field " << "data"/*std::string(kFTDCDataField)*/ << " is not a BinData."};
            }
            ConstDataRangeCursor bdCDR({buffer, static_cast<size_t>(binDataBSONElemLength)});

            BSONObj refDoc;
            uint32_t metricsCount;
            uint32_t sampleCount;
            Date_t startTs;
            long unsigned tmpPid;

            auto swTsPidEtc = extractRefDocEtcOnly(bdCDR);
            if (!swTsPidEtc.isOK()) {
                return {swTsPidEtc.getStatus()};
            }
            std::tie(refDoc, metricsCount, sampleCount, startTs, tmpPid) = swTsPidEtc.getValue();

            if (!firstMCProcessed) {
                ftdcFileSpan.timespan.first = startTs;
                pm.procId.pid = tmpPid;
                firstMCProcessed = true;
            } else {

                if (tmpPid != pm.procId.pid) {
                    if (isLastDoc && sampleCount < 10) {
                        /**
                         * Workaround: Sometimes the first several samples of a new process
                         * are written into the previous process' file, as its last metrics chunk.
                         * (Speculation: maybe it is caused by a bug in the processing of
                         * metrics.interim files found after an unclean shutdown.)
                         */
                        mcFO.pop();
                        seekMatchingPidMC = true;
                        break;
                    } else {
                        std::stringstream pidChangeErrSS;
                        pidChangeErrSS << "FTDC metrics invariant check failed: serverStatus.pid changed from "
                                << pm.procId.pid << " to " << tmpPid << " mid-file, with " << sampleCount << " samples";
                        return {ErrorCodes::UnknownError, pidChangeErrSS.str()};
                    }
                }
            }
            
            if (isLastDoc) {
                pm.lastRefDoc = refDoc.getOwned();
                ftdcFileSpan.timespan.last = startTs + Milliseconds(static_cast<int64_t>(sampleCount) * 1000);
            }

        } else {
            MONGO_UNREACHABLE;
        }
    }

    /**
     * Special rewind to get info from second-to-last doc
     */
    if (seekMatchingPidMC) {
        while (mcFO.size()) {
            _stream.close();
            _stream.open(_file.c_str(), std::ios_base::in | std::ios_base::binary);
            _stream.seekg(mcFO.top());
            StatusWith<BSONObj> swFTDCBsonDoc = readDocument();
            if (!swFTDCBsonDoc.isOK()) {
                return swFTDCBsonDoc.getStatus();
            }

            dassert(!swFTDCBsonDoc.getValue().isEmpty());

            BSONElement tmpElem;

            auto ftdcDoc = swFTDCBsonDoc.getValue();

            long long longIntVal;
            Status sType = bsonExtractIntegerField(ftdcDoc, "type"/*kFTDCTypeField*/, &longIntVal);
            if (!sType.isOK()) {
                return {sType};
            }
            FTDCBSONUtil::FTDCType ftdcType = static_cast<FTDCBSONUtil::FTDCType>(longIntVal);

            dassert(ftdcType == FTDCBSONUtil::FTDCType::kMetricChunk);

            /*** Repeats 'if (ftdcType == FTDCBSONUtil::FTDCType::kMetricChunk) {' block above ***/
            Status sZlibBinData = bsonExtractTypedField(ftdcDoc, "data"/*kFTDCDataField*/, BSONType::BinData, &tmpElem);
            if (!sZlibBinData.isOK()) {
                return {sZlibBinData};
            }

            // The "data" BinData field's data value contains
            // - uint32_t length of the data after decompression
            // - compressed zlib data
            int binDataBSONElemLength;
            const char* buffer = tmpElem.binData(binDataBSONElemLength);
            if (binDataBSONElemLength < 0) {
                return {ErrorCodes::BadValue,
                        str::stream() << "Field " << "data"/*std::string(kFTDCDataField)*/ << " is not a BinData."};
            }
            ConstDataRangeCursor bdCDR({buffer, static_cast<size_t>(binDataBSONElemLength)});

            BSONObj refDoc;
            uint32_t metricsCount;
            uint32_t sampleCount;
            Date_t startTs;
            long unsigned tmpPid;

            auto swTsPidEtc = extractRefDocEtcOnly(bdCDR);
            if (!swTsPidEtc.isOK()) {
                return {swTsPidEtc.getStatus()};
            }
            std::tie(refDoc, metricsCount, sampleCount, startTs, tmpPid) = swTsPidEtc.getValue();
            /*******************************************************************/

            if (tmpPid == pm.procId.pid) { //found the most recent metrics chunk with the same pid as the first chunk's
                pm.lastRefDoc = refDoc.getOwned();
                ftdcFileSpan.timespan.last = startTs + Milliseconds(static_cast<int64_t>(sampleCount) * 1000);
                break;
            }
            mcFO.pop();
        }
    }

    //assert(pm.metadataDoc
    //assert ftdcFileSpan.first < ftdcFileSpan.last

    pm.filespans[metadataDocDtId] = ftdcFileSpan;

    return pm;
}

Status FTDCFileReader::extractTimeseries(FTDCMetricsSubset& mr) {
    //TODO
    //iterate the metric chunks
    //if overlap mr.timespan
    //  decompress zlib
    //  iterate refDoc, confirm which items match mr.keys, map that ordinal vs. the mr.keyRow
    //  iterate VarInt-packed values. "start" must be included, others depend on what is mr.keys
    //  find the pos of each new maximum start ts < next mr.stepMs() from mr.timespan.first. Put that in another pos array
    //  read VarInt values into mr.metrics
    //  Just advance() not readAndAdvance() if an unwanted metric 'metric'
    //  For wanted metrics:
    //    insert refDoc value into all cells that had a found "start" ts
    //    sum the deltas from the VarInt values by each step pos group
    //next metric chunk
    //
    //If next metric chunk has a closer "start" time to the rounded-up stepMs, just overwrite the one in the previous.
    return Status::OK();
}

}  // namespace mongo
