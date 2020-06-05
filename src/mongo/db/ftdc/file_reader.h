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

#pragma once

#include <boost/filesystem/path.hpp>
#include <boost/optional.hpp>
#include <fstream>
#include <stddef.h>
#include <vector>

#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/db/ftdc/decompressor.h"
#include "mongo/db/ftdc/util.h"
#include "mongo/db/jsobj.h"

namespace mongo {

/**
 * To identify a mongod (or mongos) process uniquely we use the hostport string
 * and the pid.
 */
struct FTDCProcessId {
    std::string hostport;
    unsigned long pid;
};

bool operator==(const FTDCProcessId& l, const FTDCProcessId& r);
bool operator!=(const FTDCProcessId& l, const FTDCProcessId& r);
bool operator<(const FTDCProcessId& l, const FTDCProcessId& r);

/**
 * Timestamps range. For conveying the metrics sample coverage time.
 */
struct FTDCPMTimespan {
    Date_t first;
    Date_t last;
};

/**
 * A struct representing the metrics found in one or more FTDC files for
 * one process instance of mongod or mongos i.e. will not run over to
 * include metrics generated after the process is restarted.
 *
 * Can be generated from one file, but intended to be merged with the same
 * from files before and after in time that have identical pid and
 * hostname(+port).
 *
 * start_ts is the first "start" value from the kMetricChunks in all files.
 * estimate_end_ts is "start" + (metricCount * 1 sec) of the last
 * kMetricChunk in the last file.
 *
 * The files are packed in a tuple with the "_id" date value in the
 * kMetadataDoc as the first value to make it easy to identify when we've
 * received the same file. It may be a newer, larger version of the same
 * file and if it so should replace. If not we can ignore using that file
 * because it must be a duplicate or an earlier, shorter version.
 *
 * lastRefDoc is the last refDoc member from the last kMetricChunk from all
 * files loaded so far. It will probably have many more dozens of metrics in it
 * than the very first refDoc created when the process started.
 *
 * A sampleCounts value will be the sum of samples in all kMetricChunks for a
 * given file. It's map key is the same "_id" date value used as key for
 * sourceFilepaths;
 */

struct FTDCProcessMetrics {
    FTDCProcessId procId;
    std::map<Date_t, boost::filesystem::path> sourceFilepaths;
    std::map<Date_t, std::uint32_t> sampleCounts;
    std::map<Date_t, FTDCPMTimespan> timespans;
    BSONObj metadataDoc;
    BSONObj lastRefDoc;
    std::map<std::string, BSONType> keys;

    std::string rsName() const;
    Date_t firstSampleTs() const;
    Date_t estimateLastSampleTs() const;

    Status merge(const FTDCProcessMetrics& pm);
    void mergeRefDocKeys(const BSONObj& _refDoc);

    /**
     * Executes metadataAndTimeseries for each file in this process session,
     * The metadata doc is ignored. The metrics are concatenated together as
     * one larger array of timeseries.
     */
    std::map<std::string, std::vector<uint64_t>> timeseries(float resolution = 0.0);

    //temporary debugging use
    friend std::ostream& operator<<(std::ostream& os, FTDCProcessMetrics& pm);
};

/**
 * Reads a file, either an archive stream or interim file
 *
 * Does not recover interim files into archive files.
 */
class FTDCFileReader {
    FTDCFileReader(const FTDCFileReader&) = delete;
    FTDCFileReader& operator=(const FTDCFileReader&) = delete;

public:
    FTDCFileReader() : _state(State::kNeedsDoc) {}
    ~FTDCFileReader();

    /**
     * Open the specified file
     */
    Status open(const boost::filesystem::path& file);

    /**
     * Returns true if their are more records in the file.
     * Returns false if the end of the file has been reached.
     * Return other error codes if the file is corrupt.
     * Convention break warning: hasNext() advances the position, not next()
     */
    StatusWith<bool> hasNext();

    /**
     * Returns the next document.
     * Metadata documents are unowned.
     * Metric documents are owned.
     * Convention break warning: hasNext() advances the position, not next()
     */
    std::tuple<FTDCBSONUtil::FTDCType, const BSONObj&, Date_t> next();

    /**
     * Reads the entire file and returns the FTDC metrics
     * including the metadata doc for each mongod/mongos process lifetime
     * covered. The metrics will contain keys vs. timeseries. Consecutive
     * kMetricChunk sections will be concatenated together.
     *
     * Resolution 0 means to keep time samples as found. Setting to a value
     * larger than the sampling frequency will downsize the sample resolution.
     *
     * Assumption is that one file will only contain one kMetadataDoc and the
     * remainder will be kMetricChunks. If this is incorrect change to return
     * a vector of FTDCProcessMetrics.
     */
    FTDCProcessMetrics metadataAndTimeseries(float resolution = 0.0);

    /**
     * As above, but only scans the beginning of each file to fill toplogy
     * identity fields and fetch the reference doc / first sample.
     */
    StatusWith<FTDCProcessMetrics> previewMetadataAndTimeseries();

private:
    /**
     * Read next consecutive BSON document from _stream of raw file. This will
     * be either a kMetadataDoc or kMetricChunk. If the file is corrupt returns
     * an appropriate status.
     */
    StatusWith<BSONObj> readTopLevelBSONDoc();

    // Read next top-level doc, set _state, _parent, _dateId, _metadataDoc,
    // _docs and _pos (= current metric chunk array pos) members.
    StatusWith<bool> readAndParseTopLevelDoc();

private:
    FTDCDecompressor _decompressor;

    /**
     * Internal state of file reading state machine.
     *
     *      +--------------+      +--------------+
     *   +> |  kNeedsDoc   | <--> | kMetadataDoc |
     *   |  +--------------+      +--------------+
     *   |
     *   |    +----------+
     *   |    v          |
     *   |  +--------------+
     *   +> | kMetricChunk |
     *      +--------------+
     */
    enum class State {

        /**
         * Indicates that we need to read another document from disk
         */
        kNeedsDoc,

        /**
         * Indicates that are processing a metric chunk, and have one or more documents to return.
         */
        kMetricChunk,

        /**
         * Indicates that we need to read another document from disk
         */
        kMetadataDoc,
    };

    State _state{State::kNeedsDoc};

    // Current position in the _docs array.
    std::size_t _pos{0};

    // Current set of metrics documents
    std::vector<BSONObj> _docs;

    // _id of current metadata or metric chunk
    Date_t _dateId;

    // Current metadata document - unowned
    BSONObj _metadata;

    // ref doc of current metrics chunk
    BSONObj _refDoc;

    // metrics and sample count in current metrics chunk. N.b. this sample
    // count is of the packed deltas array, doesn't include the reference
    // doc's values. Typically it is 299. 1 sec in the ref doc, the remaining
    // seconds in the 5 min chunk in the packed deltas array.
    /**
     * 1061 metrics x 159 samples
     * 1219 metrics x 0 samples <-- local.oplog stats appear
     * 1291 metrics x 9 samples <-- repl is intialized
     * 1294 metrics x 0 samples <-- primary node detected, lastWrite is recorded, etc.
     * 1296 metrics x 0 samples <-- one new lock statistic increments for the first time
     * 1306 metrics x 299 samples <-- readConcernMajorityOpTime appears
     * 1306 metrics x 299 samples
     * 1306 metrics x 299 samples
     * ...
     * 1306 metrics x 299 samples
     * 1306 metrics x 299 samples
     * 1306 metrics x 299 samples
     * 1306 metrics x 59 samples
     * 1327 metrics x 104 samples <-- shardingStatistics appears
     * 1329 metrics x 261 samples <-- a couple of new lock stats appear
     * 1330 metrics x 299 samples <-- a couple of new lock stats appear
     * 1330 metrics x 299 samples
     * ...
     * */
    std::uint32_t _metricsCount;
    std::uint32_t _sampleCount;

    // Parent document
    BSONObj _parent;

    // Buffer of data read from disk
    std::vector<char> _buffer;

    // File name
    boost::filesystem::path _file;

    // Size of file on disk
    std::size_t _fileSize{0};

    // Input file stream
    std::ifstream _stream;
};

}  // namespace mongo
