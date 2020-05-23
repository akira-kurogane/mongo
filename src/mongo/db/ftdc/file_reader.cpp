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

Status FTDCProcessMetrics::merge(const FTDCProcessMetrics& other) {
    //throw error if hostport and pid don't match exactly

    for (auto const& [k, v] : other.sourceFilepaths) {
        //TODO: if k (a datetime) is already present, work out which file is more recent / has a longer metric sample and use it instead.
        sourceFilepaths[k] = v;
    }

    if (other.start_ts < start_ts) {
        start_ts = other.start_ts;
    }
    if (other.end_ts > end_ts) {
        end_ts = other.end_ts;
    }

    //metadataDoc should be identical
    //refDoc is expected to have the same number of keys

    for (auto const& [k, v] : other.sampleCounts) {
        sampleCounts[k] = v;
    }

    //Throw error if metricsCount doesn't match

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
    auto sw = hasNext();
    
    unsigned int ctr = 0;
    /**
     * Intuition break warning: FTDCFileReader::hasNext() advances the position, not next()
     */
    while (sw.isOK() && sw.getValue() && ctr++ < 100000) {
        auto ftdcType = std::get<0>(next());
        //auto d = std::get<1>(next()).getOwned();

        if (ftdcType == FTDCBSONUtil::FTDCType::kMetadata) {
            pm.metadataDoc = std::get<1>(next());
            auto ts = std::get<2>(next());
            pm.sourceFilepaths[ts] = _file;
            BSONElement hpElem = dps::extractElementAtPath(pm.metadataDoc, "hostInfo.system.hostname");
            if (hpElem.eoo()) {
                return {ErrorCodes::KeyNotFound, "missing 'hostInfo.system.hostname' in metadata doc"};
            }
            pm.procId.hostport = hpElem.String();
        } else if (ftdcType == FTDCBSONUtil::FTDCType::kMetricChunk) {
            pm.refDoc = std::get<1>(next()).getOwned();
            BSONElement startTSElem = dps::extractElementAtPath(pm.refDoc, "start");
            if (startTSElem.eoo()) {
                return {ErrorCodes::KeyNotFound, "missing 'start' timestamp in a metric sample"};
            }
            pm.start_ts = startTSElem.Date();
            BSONElement pidElem = dps::extractElementAtPath(pm.refDoc, "serverStatus.pid");
            if (pidElem.eoo()) {
                return {ErrorCodes::KeyNotFound, "missing 'serverStatus.pid' in a metric sample"};
            }
            pm.procId.pid = pidElem.Long();
	    return {pm}; //Exit early because this is a preview-only function
        } else {
            MONGO_UNREACHABLE;
        }
        sw = hasNext();
    }

    return {pm};
}

}  // namespace mongo
