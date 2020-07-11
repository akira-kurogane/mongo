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
#include "mongo/db/ftdc/varint.h"
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

StatusWith<FTDCProcessMetrics> FTDCFileReader::extractProcessMetricsHeaders() {
    FTDCProcessMetrics pm;
    Date_t metadataDocDtId = Date_t::min();
    FTDCFileSpan ftdcFileSpan = { _file, {Date_t::max(), Date_t::min()} };
    bool firstMCProcessed = false;

    while (true) {
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
        bool isLastDoc = (_fileSize - 8u) <= static_cast<size_t>(_stream.tellg());

        // The Date_t "_id" field is in every FTDC file top-level doc.
        // We'll use the one in the metadata doc as a primary identifier
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

            /**
             * If no metadataDoc found yet OR dtId < metadataDoc then assume this metrics chunk
             * if from salvaged interim data of the previous server process; skip it.
             *
             * FTDCFileManager::recoverInterimFile() and ...::openArchiveFile() will salvage
             * metrics chunk docs (theoretically also metadata docs) and insert them before the
             * new process' metrics chunks. If we detect this just skip it. (The ideal solution
             * is that FTDCProcessMetrics objects can have an additional 'tail' file in their file
             * list they can use to reach their lost metric chunk tail; not attempting this now.)
             */
            if (metadataDocDtId == Date_t::min() || dtId < metadataDocDtId) {
                std::cout << "Skipping kMetricsChunk with date Id = " << dtId << " in file " << _file << " because " <<
                        "it is assumed to be interim data from previous server process\n";
                continue;

            }

            if (firstMCProcessed && !isLastDoc) { //i.e. a middle metric chunk
                // Just skip.
                // Devnote: unless we decide later to get exact sampleCount sum and/or "start" ts value array
                continue;
            }

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
            //std::uint32_t metricsCount = swMetricsCount.getValue();

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
            auto startTs = startTSElem.Date();

            BSONElement pidElem = dps::extractElementAtPath(refDoc, "serverStatus.pid");
            if (pidElem.eoo()) {
                return {ErrorCodes::KeyNotFound, "missing 'serverStatus.pid' in a metric sample"};
            }
            auto tmpPid = pm.procId.pid;
            tmpPid = static_cast<unsigned long>(pidElem.Long());

            if (!firstMCProcessed) {
                ftdcFileSpan.timespan.first = startTs;
                pm.procId.pid = tmpPid;
            } else {
                if (tmpPid != pm.procId.pid) {
                    return {ErrorCodes::UnknownError, "FTDC metrics invariant check failed: serverStatus.pid changed mid-file."};
                }
            }

            if (isLastDoc) {
                //TODO: cycle through first sampleCount VarInts, which are the "start" ts vals, to get the *exact* ts of the last metric chunk
                pm.lastRefDoc = refDoc.getOwned();
                ftdcFileSpan.timespan.last = startTs + Milliseconds(static_cast<int64_t>(sampleCount) * 1000);
            }

            firstMCProcessed = true;

        } else {
            MONGO_UNREACHABLE;
        }
    }

    //assert(pm.metadataDoc
    //assert ftdcFileSpan.first < ftdcFileSpan.last

    pm.filespans[metadataDocDtId] = ftdcFileSpan;

    return pm;
}

/**
 * Returns a map of flattened key name (eg. "opLatencies.reads.ops" vs. a
 * tuple of: Ordinal it should be in the metrics array; BSONType; BSON
 * element's value cast to the common uint64_t type used in the metrics array.
 */
std::map<std::string, std::tuple<size_t, BSONType, uint64_t>> _flattenedBSONDoc(const BSONObj& doc, size_t& ordCounter) {
    std::map<std::string, std::tuple<size_t, BSONType, uint64_t>> keys;
    BSONObjIterator itDoc(doc);
    while (itDoc.more()) {
        const BSONElement elem = itDoc.next();
        switch (elem.type()) {
            case Object:
            case Array: {
                auto childKeys = _flattenedBSONDoc(elem.Obj(), ordCounter);
                for (auto const& [ck, tpl] : childKeys) {
                    keys[elem.fieldName() + std::string(".") + ck] = tpl;
                }
            } break;

            // all numeric types are extracted as long (int64)
            // this supports the loose schema matching mentioned above,
            // but does create a range issue for doubles, and requires doubles to be integer
            case NumberDouble:
            case NumberInt:
            case NumberLong:
            case NumberDecimal:
                keys[elem.fieldName()] = std::make_tuple(ordCounter++, elem.type(), elem.numberLong());
                break;

            case Bool:
                keys[elem.fieldName()] = std::make_tuple(ordCounter++, elem.type(), static_cast<uint64_t>(elem.Bool()));
                break;

            case Date:
                keys[elem.fieldName()] = std::make_tuple(ordCounter++, elem.type(), static_cast<uint64_t>(elem.Date().toMillisSinceEpoch()));
                break;

            case bsonTimestamp:
                /**
                 * A timestamp is saved in two separate metrics. See the double
		 * emplaceBack() calls in "case bsonTimestamp:" in
		 * extractMetricsFromDocument(). First metric row is for the
		 * unix epoch secs; the second the mongodb timestamp increment
		 * field.
		 * (Putting it another way: these are the ".t" and ".i"
		 * subfields of a timestamp object in MongoDB Extended JSON.)
		 *
                 * The flattened key name will point to just the first one,
		 * for epoch seconds. N.b. we must increment ordCounter once
		 * more in this block to move past the second metric row for the
		 * timestamp increment metric.
                 */
                keys[elem.fieldName()] = std::make_tuple(ordCounter++, elem.type(), static_cast<uint64_t>(elem.timestamp().getSecs()));
                ordCounter++;
                break;

            default:
                if (FTDCBSONUtil::isFTDCType(elem.type())) {
                    MONGO_UNREACHABLE;
                }
                break;
        }
    }
    return keys;
}

std::map<std::string, std::tuple<size_t, BSONType, uint64_t>> flattenedBSONDoc(const BSONObj& doc) {
   size_t ctr = 0;
   return _flattenedBSONDoc(doc, ctr);
}

Status FTDCFileReader::extractTimeseries(FTDCMetricsSubset& mr) {
std::cout << mr.timespan().first << " - " << mr.timespan().last << "\n";
    //mr is initialized only with keys, tspan, sampleResolution;

    Date_t metadataDocDtId = Date_t::min();

    while (true) {
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

        // The Date_t "_id" field is in every FTDC file top-level doc.
        // We'll use the one in the metadata doc as a primary identifier
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
            metadataDocDtId = dtId;
            ; //Nothing else

        } else if (ftdcType == FTDCBSONUtil::FTDCType::kMetricChunk) {

            FTDCPMTimespan roughTspan = { dtId, dtId + Seconds(600) };
            if (!mr.timespan().overlaps(roughTspan)) {
                continue;
            }
            /**
             * If no metadataDoc found yet OR dtId < metadataDoc then assume this metrics chunk
             * if from salvaged interim data of the previous server process; skip it.
             *
             * FTDCFileManager::recoverInterimFile() and ...::openArchiveFile() will salvage
             * metrics chunk docs (theoretically also metadata docs) and insert them before the
             * new process' metrics chunks. If we detect this just skip it. (The ideal solution
             * is that FTDCProcessMetrics objects can have an additional 'tail' file in their file
             * list they can use to reach their lost metric chunk tail; not attempting this now.)
             */
            if (metadataDocDtId == Date_t::min() || dtId < metadataDocDtId) {
                std::cout << "Skipping kMetricsChunk with date Id = " << dtId << " in file " << _file << " because " <<
                        "it is assumed to be interim data from previous server process\n";
                continue;

            }

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

            std::map<int, int> eR; //exporting rows. Key is pos in refdoc; Value is row in mr.metrics.
            std::map<int, uint64_t> uint64RefDocVals;
            auto fbdMap = flattenedBSONDoc(refDoc);
//std::cout << "fbdMap.size() = " << fbdMap.size() << "\n";
//for (auto fbdItr = fbdMap.begin(); fbdItr != fbdMap.end(); fbdItr++) {
//std::cout << fbdItr->first << ": " << typeName(std::get<1>(fbdItr->second)) << " at ord " << std::get<0>(fbdItr->second) << "\n";
//}

            for (auto const& k : mr.keys()) {
                 auto mrkElem = fbdMap.find(k);
                 if (mrkElem != fbdMap.end()) {
                     auto refDocOrd = std::get<0>(mrkElem->second);
                     eR[refDocOrd] = mr.keyRow(k);
                     //std::cout << k << " is at ord " << std::get<0>(mrkElem->second) << "\n";
                     auto bt = std::get<1>(mrkElem->second);
//if (k == "serverStatus.wiredTiger.cache.bytes read into cache") {
//std::cout << k << ": type=" << typeName(bt) << ", value = " << std::get<2>(mrkElem->second) << "\n";
//}
                     if (mr.bsonType(k) == BSONType::Undefined) {
//if (k == "serverStatus.wiredTiger.cache.bytes read into cache") {
//std::cout << "Setting mr types for " << k << " to " << typeName(bt) << "; value = " << std::get<2>(mrkElem->second) << "\n";
//}
                         mr.setBsonType(k, bt);
                     } else if (mr.bsonType(k) != bt) {
                         ; //TODO handle the bson type change case
std::cout << "field " << k << " changed from " << typeName(bt) << " to " << typeName(mr.bsonType(k)) << "\n";
//invariant(mr.bsonType(k) == bt);
                     }
                     uint64RefDocVals[refDocOrd] = std::get<2>(mrkElem->second);
                 }
            }

            std::map<std::string, size_t> targetRows; //{{"start", 0}, {"opcounters.insert", 1}, ...

            // Read count of metrics
            auto swMetricsCount = pzCdc.readAndAdvance<LittleEndian<std::uint32_t>>();
            if (!swMetricsCount.isOK()) {
                return {swMetricsCount.getStatus()};
            }
            std::uint32_t metricsCount = swMetricsCount.getValue();
//std::cout << " metricsCount = " << metricsCount << "\n";

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
            auto startTs = startTSElem.Date();

            // Knowing the exact sample count gives us more accurate lastTs estimation
            // and a chance to skip further metrics processing if unnecessary
            FTDCPMTimespan estTspan = { startTs, startTs + Seconds(sampleCount) };
            if (!mr.timespan().overlaps(estTspan)) {
                continue;
            }
//std::cout << refDoc.jsonString() << "\n";

//Dumping fbdMap
//std::map<size_t, std::tuple<std::string, BSONType, uint64_t>> frmap;
//for (auto fbdItr = fbdMap.begin(); fbdItr != fbdMap.end(); fbdItr++) {
//frmap[std::get<0>(fbdItr->second)] = { fbdItr->first, std::get<1>(fbdItr->second), std::get<2>(fbdItr->second) };
//}
//for (auto frmItr = frmap.begin(); frmItr != frmap.end(); frmItr++) {
//std::cout << frmItr->first << ": " << std::get<0>(frmItr->second) << "(" << std::get<1>(frmItr->second) << ") = " << std::get<2>(frmItr->second) << "\n";
//}
            /**
             * Watch out for pids not matching the provided one (it is provided, right?)
             * just in case the skip-salvaged-interim mc logic doesn't get all.
             */
            /*BSONElement pidElem = dps::extractElementAtPath(refDoc, "serverStatus.pid");
            if (pidElem.eoo()) {
                return {ErrorCodes::KeyNotFound, "missing 'serverStatus.pid' in a metric sample"};
            }
            auto tmpPid = static_cast<unsigned long>(pidElem.Long());*/

            std::uint64_t zeroesCount = 0;

            size_t rowOrd = 0;

            //read "start" row, expected to be first
            std::vector<uint64_t> tsVals(sampleCount + 1, 0);
            tsVals[0] = startTs.toMillisSinceEpoch();
std::cout << "refDoc.start = " << startTs << " (or as (u)int tsVals[0] = " << tsVals[0] << ")\n";
std::cout << "sampleCount = " << sampleCount << "\n";
            for (std::uint32_t j = 1; j < sampleCount + 1; j++) {
                auto swDelta = pzCdc.readAndAdvance<FTDCVarInt>();

                if (!swDelta.isOK()) {
                    return swDelta.getStatus();
                }

                auto delta = swDelta.getValue();
                invariant(delta != 0); //"start" is a clock value, should never be repeating, delta should never be 0
                tsVals[j] = tsVals[j - 1] + delta;

            }

            std::vector<int> tsOrds(tsVals.size(), -1);
//std::cout << "tsOrds = { ";
            for (std::uint32_t j = 0; j < tsVals.size(); j++) {
                if (tsVals[j] >= static_cast<uint64_t>(mr.timespan().first.toMillisSinceEpoch()) &&
                                tsVals[j] < static_cast<uint64_t>(mr.timespan().last.toMillisSinceEpoch())) {
                    tsOrds[j] = (tsVals[j] - mr.timespan().first.toMillisSinceEpoch()) / mr.resolution();
		    mr.metrics[tsOrds[j]] = tsVals[j]; //It's OK to overwrite "start" ts values placed in same ord in previous loop. We want max "start" ts in the cell.
//std::cout << tsOrds[j] << ", ";
                }
//else { std::cout << "-1, "; }
                //else leave as initialized value -1, to mean no/unset/invalid
            }
//std::cout << "}\n";

            zeroesCount = 0;
//std::cout << "Deltas: ";
            //metric 0 is "start" whose compressed VarInt values were consumed above already
            for (std::uint32_t i = 1; i < metricsCount; i++) {
                auto mRow = eR.find(i);

                if (mRow != eR.end()) {
//std::cout << "\nmRow " << mr.rowKeyName(mRow->second) << ": ";
                    std::set<int> uto;
                    for (auto x :  tsOrds) {
                        uto.insert(x);
                    }
                    uto.erase(-1); // -1 is for 'unset' 
                    for (auto tOrd : uto) {
                        mr.metrics[mr.cellOffset(mRow->second, tOrd)] = uint64RefDocVals[i];
                    }
                }

                for (std::uint32_t j = 0; j < sampleCount; j++) {
                    if (zeroesCount) {
                        //TODO: Test the following method:
                        // auto sampleIncr = min(sampleCount - j; zeroesCount); zeroesCount -= sampleIncr; j += sampleIncr - 1; (because of ++ on j in loop definition
//std::cout << "0 ";
                        zeroesCount--;
                        continue;
                    }

                    auto swDelta = pzCdc.readAndAdvance<FTDCVarInt>();

                    if (!swDelta.isOK()) {
                        return swDelta.getStatus();
                    }

                    if (swDelta.getValue() == 0) {
                        auto swZero = pzCdc.readAndAdvance<FTDCVarInt>();

                        if (!swZero.isOK()) {
                            return swDelta.getStatus();
                        }
                        zeroesCount = swZero.getValue();

                    } else if (mRow != eR.end()) {
                        auto mrmOrd = tsOrds[j + 1];
                        if (mrmOrd >= 0) {
//std::cout << (swDelta.getValue() > (UINT64_MAX - 1000000) ? UINT64_MAX - swDelta.getValue() - 1 : static_cast<uint64_t>(swDelta.getValue())) << " ";
                            // Sanity warning: the FTDCVarInt is uint64_t. Unsigned int overflow is used to achieve negative deltas
			    // Eg. next_val = (UINT64_MAX - 3) + prev_val. next_val == prev_val - 3
                            mr.metrics[mr.cellOffset(mRow->second, tsOrds[j + 1])] += swDelta.getValue();
                        }
                    }
                }
            }


        } else {
            MONGO_UNREACHABLE;
        }
    }
//std::cout << "\n";

    return Status::OK();
}

}  // namespace mongo
