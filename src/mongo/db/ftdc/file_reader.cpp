/**
 *    Copyright (C) 2018-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/ftdc/file_reader.h"

#include <boost/filesystem.hpp>
#include <fstream>

#include "mongo/base/data_range_cursor.h"
#include "mongo/base/data_type_validated.h"
#include "mongo/bson/bsonmisc.h"
#include "mongo/db/ftdc/config.h"
#include "mongo/db/ftdc/util.h"
#include "mongo/db/jsobj.h"
#include "mongo/rpc/object_check.h"
#include "mongo/util/str.h"
#include "mongo/db/bson/dotted_path_support.h"

namespace dps = ::mongo::dotted_path_support;

namespace mongo {

bool operator==(const FTDCProcessId& l, const FTDCProcessId& r) {
    return l.hostport == r.hostport && l.pid == r.pid;
}
bool operator!=(const FTDCProcessId& l, const FTDCProcessId& r) {
    return !(l==r);
}

bool operator<(const FTDCProcessId& l, const FTDCProcessId& r) {
    return l.hostport < r.hostport || (l.hostport == r.hostport && l.pid < r.pid);
}

std::string FTDCProcessMetrics::rsName() {
    BSONElement rsnmElem = dps::extractElementAtPath(metadataDoc, "getCmdLineOpts.parsed.replication.replSetName");
    if (rsnmElem.eoo()) {
         return "";
    } else {
         return rsnmElem.String();
    }
}

//Unit test: FTDCProcessMetrics a.merge(b) should create the same doc as b.merge(a);
//Unit test: file (in sourceFilepaths) that is younger or identical version of should not change any member value
//Unit test: file that is later, bigger-sample-length should raise sampleCounts sum
//Unit test: file that is the later, bigger-sample-length should raise estimated_end_ts
//Unit test: latersample.merge(onefile_oldersmallersample) should change no property
//Unit test: mismatching procId should return error
//Unit test: no sampleCounts value is zero
Status FTDCProcessMetrics::merge(const FTDCProcessMetrics& other) {
    if (procId != other.procId) {
        //It is a programming error if there is an attempt to merge metrics
        //of different processes
        return Status(ErrorCodes::BadValue,
                      "FTDCProcessMetrics::merge() on non-matching FTDCProcessId.");
    }

    /**
     * If other contains the same file (identified by the Date _id, not by
     * filepath) and that file is a younger, smaller-sample-length version then
     * we don't want to use any values from it: filepath, sampleCount, start_ts
     * and estimated_end_ts and refDoc. 
     * However: we can expect start_ts and refDoc to be identical, and
     * estimated_end_ts and start_ts are safe to use within heuristics of
     * greater-than and less-than rules.
     */
    for (auto const& [date_id, oth_fpath] : other.sourceFilepaths) {
        auto oth_sampleCount = other.sampleCounts.find(date_id)->second;
        auto oth_timespan = other.timespans.find(date_id)->second;
        auto oldfpItr = sourceFilepaths.find(date_id);
        if (oldfpItr == sourceFilepaths.end()) {
            sourceFilepaths[date_id] = oth_fpath;
            sampleCounts[date_id] = oth_sampleCount;
            timespans[date_id] = oth_timespan;
        } else {
            auto existing_fpath = oldfpItr->second;
            std::cerr << "Info: Duplicate FTDC files for " << procId.hostport << ", pid=" << procId.pid <<
                    ", starting datetime id=" << date_id << " found." << std::endl;
            if (oth_sampleCount < sampleCounts[date_id]) {
                std::cerr << "File " << oth_fpath << " will be ignored because file " << existing_fpath << " has larger sample count" << std::endl;
            } else {
                std::cerr << "File " << existing_fpath << " will be ignored because file " << oth_fpath << "is larger" << std::endl;
                sourceFilepaths[date_id] = oth_fpath;
                sampleCounts[date_id] = oth_sampleCount;
                timespans[date_id] = oth_timespan;
            }
        }
    }

    if (other.start_ts < start_ts) {
        start_ts = other.start_ts;
        metadataDoc = other.metadataDoc; //expected to be identical, but for consistency let's use earliest.
        firstRefDoc = other.firstRefDoc;
    }
    if (other.estimate_end_ts > estimate_end_ts) {
        estimate_end_ts = other.estimate_end_ts;
    }

    return Status::OK();
}

std::ostream& operator<<(std::ostream& os, FTDCProcessMetrics& pm) {
    std::string fpList;
    for (auto const& [k, v] : pm.sourceFilepaths) {
        fpList = fpList + v.string() + " ";
    }
    os << pm.procId.hostport << " [" << pm.procId.pid << "] " << pm.rsName() << " " << fpList;
    return os;
}

FTDCFileReader::~FTDCFileReader() {
    _stream.close();
}

StatusWith<bool> FTDCFileReader::hasNext() {
    while (true) {
        if (_state == State::kNeedsDoc) {
            auto swRP = readAndParseTopLevelDoc();

            auto swDocs = FTDCBSONUtil::getMetricsFromMetricDoc(_parent, &_decompressor);
            if (!swDocs.isOK()) {
                return swDocs.getStatus();
            }

            _docs = swDocs.getValue();

            // There is always at least the reference document
            _pos = 0;

            return swRP;
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

StatusWith<bool> FTDCFileReader::readAndParseTopLevelDoc() {

    if (_stream.eof()) {
        return {false};
    }

    auto swDoc = readTopLevelBSONDoc();
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

        /**
         * FYI a raw FTDC metadata doc looks like this if printed as JSON.
         * { "_id" : { "$date" : "2019-11-25T15:08:42.031+0900" },
         *   "type" : 0,
         *   "doc" : {
         *     "start" : { "$date" : "2019-11-25T15:08:42.031+0900" },
         *     "buildInfo" : { "start": { .. }, "version" : "3.6.12", ... },
         *     "getCmdLineOpts" : { "start" : { ... }, "argv" : [ ... ], "parsed" : { "config" : ... },
         *     "hostInfo" : { "start" : { ... }, "system" : { ... },
         *     "end" : { "$date" : "2019-11-25T15:08:42.031+0900" }
         *   }
         * }
         */

        auto swMetadata = FTDCBSONUtil::getBSONDocumentFromMetadataDoc(_parent);
        if (!swMetadata.isOK()) {
            return swMetadata.getStatus();
        }

        _metadata = swMetadata.getValue();

    } else if (type == FTDCBSONUtil::FTDCType::kMetricChunk) {
        _state = State::kMetricChunk;

        /**
         * FYI a raw FTDC metadata doc looks like this if printed as JSON.
         * The ref bson doc, sample and metrics counts, and the varint deltas
         * are not exposed. Decompression into yet another bson doc is required first.
         * { "_id" : { "$date" : "2019-11-25T15:08:43.000+0900" },
         *   "type" : 1,
         *   "data" : { "$binary" : "<uncompressed-size-uint32_t><zlib-compressed-binary-val>", "$type" : "00" }
         * }
         */

        auto swMP = FTDCBSONUtil::getMetricsPreviewFromMetricDoc(_parent, &_decompressor);
        if (!swMP.isOK()) {
            return swMP.getStatus();
        }

        auto valsTuple = swMP.getValue();
        _refDoc = std::get<0>(valsTuple);
        _sampleCount = std::get<2>(valsTuple);

    }

    return {true};
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

StatusWith<BSONObj> FTDCFileReader::readTopLevelBSONDoc() {
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
                str::stream() << "Failed to read 4 bytes from file \"" << _file.generic_string()
                              << "\""};
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
                str::stream() << "Invalid BSON length found in file \"" << _file.generic_string()
                              << "\""};
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
                str::stream() << "Failed to read " << readSize << " bytes from file \""
                              << _file.generic_string() << "\""};
    }

    ConstDataRange cdr(_buffer.data(), _buffer.data() + bsonLength);

    // TODO: Validated only validates objects based on a flag which is the default at the moment
    auto swl = cdr.readNoThrow<Validated<BSONObj>>();
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

    boost::system::error_code ec;
    _fileSize = boost::filesystem::file_size(file, ec);
    if (ec) {
        return {ErrorCodes::NonExistentPath,
                str::stream() << "\"" << file.generic_string()
                              << "\" file size could not be retrieved during open: "
                              << ec.message()};
    }

    _file = file;

    return Status::OK();
}

StatusWith<FTDCProcessMetrics> FTDCFileReader::previewMetadataAndTimeseries() {
    FTDCProcessMetrics pm;
    auto sw = readAndParseTopLevelDoc();
    assert(_state == State::kMetricChunk || _state == State::kMetadataDoc);

    unsigned int ctr = 0;
    Date_t estimateEndTS;
    /**
     * Intuition break warning: FTDCFileReader::hasNext() advances the position, not next()
     */
    while (sw.isOK() && sw.getValue() && ctr++ < 100000) {
        pm.sourceFilepaths[_dateId] = _file;

        if (_state == State::kMetadataDoc) {
            // _dateId, _metadata have been loaded by readAndParseTopLevelDoc()

            pm.metadataDoc = _metadata;
            BSONElement hpElem = dps::extractElementAtPath(pm.metadataDoc, "hostInfo.system.hostname");
            if (hpElem.eoo()) {
                return {ErrorCodes::KeyNotFound, "missing 'hostInfo.system.hostname' in metadata doc"};
            }
            pm.procId.hostport = hpElem.String();

        } else { // _state == State::kMetricChunk
            // _dateId, _refDoc, _sampleCount have been loaded by readAndParseTopLevelDoc()

            //TODO: pm.keys += zip(_refDoc.getFieldNames(), bsontype)
            BSONElement startTSElem = dps::extractElementAtPath(_refDoc, "start");
            if (startTSElem.eoo()) {
                return {ErrorCodes::KeyNotFound, "missing 'start' timestamp in a metric sample"};
            }
            pm.sampleCounts[_dateId] += _sampleCount;
            estimateEndTS = startTSElem.Date() + Milliseconds(static_cast<int64_t>(_sampleCount) * 1000);
            pm.timespans[_dateId] = {startTSElem.Date(), estimateEndTS};
            if (pm.firstRefDoc.isEmpty()) {
                pm.firstRefDoc = _refDoc.getOwned();
                pm.start_ts = startTSElem.Date();
                BSONElement pidElem = dps::extractElementAtPath(_refDoc, "serverStatus.pid");
                if (pidElem.eoo()) {
                    return {ErrorCodes::KeyNotFound, "missing 'serverStatus.pid' in a metric sample"};
                }
                pm.procId.pid = pidElem.Long();
            }

        }
        sw = readAndParseTopLevelDoc();
        assert(_state == State::kMetricChunk || _state == State::kMetadataDoc);
    }
    pm.estimate_end_ts = estimateEndTS;

    return {pm};
}

}  // namespace mongo
