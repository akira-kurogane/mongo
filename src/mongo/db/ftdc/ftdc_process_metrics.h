#pragma once

#include "mongo/util/time_support.h"
#include "mongo/bson/bsonobj.h"

#include <boost/filesystem/path.hpp>
#include <vector>
#include <map>

#include "mongo/base/status.h"

namespace mongo {

/**
 * To identify a mongod (or mongos) process uniquely we use the hostport string
 * and the pid.
 */
struct FTDCProcessId {
    std::string hostport;
    uint64_t pid;
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

    bool isValid();
    bool overlaps(FTDCPMTimespan& other);
    FTDCPMTimespan intersection(FTDCPMTimespan& other);
};

struct FTDCMSKeyNameType {
    std::string keyName;
    BSONType bsonType;
    //TODO add a COUNTER, GAUGE, HISTOGRAM timeseries type here too
};

/**
 * A subset of the metrics, optionally downsampled to lower resolution
 * - Subset of metrics by dot-concatenated key list
 * - To a fixed timespan
 * - To a resolution in milliseconds - E.g. 60000 for 1 min samples. 1000ms as default.
 *
 * _keys will have "start" key added during ctor if absent
 */
class FTDCMetricsSubset {

public:
    FTDCMetricsSubset(std::vector<std::string> keys, FTDCPMTimespan tspan,
                      uint32_t r = 1000);
    ~FTDCMetricsSubset() {};

public:
    FTDCPMTimespan timespan() {
        return _tspan;
    }
    uint32_t resolutionMs() { return _stepMs; }

    std::vector<std::string> keys() {
        std::vector<std::string> r;
        r.reserve(_kNT.size());
        for (auto x : _kNT) {
            r.emplace_back(x.keyName);
        }
        return r;
    }

    std::string rowKeyName(size_t rowOrd) { return _kNT[rowOrd].keyName; }
    size_t keyRow(std::string kn) {
        assert(_keyRows.find(kn) != _keyRows.end());
        return _keyRows[kn];
    }

    BSONType bsonType(std::string k) { return _kNT[_keyRows[k]].bsonType; }
    void setBsonType(std::string k, BSONType t) { _kNT[_keyRows[k]].bsonType = t; }

    std::vector<std::uint64_t> metrics; //Size = _rowLength * _kNT.size()

    std::vector<FTDCMSKeyNameType> keyNamesAndType() { return _kNT; }

    /**
     * Return all metrics in a BSONDoc of this format:
     * {
     *   key: [ BSONElement, BSONElement, ... ],
     *   ...
     * }
     * First key will be "start" timestamp. All arrays same length; each i'th
     * element is the sample at i'th "start" timestamp value.
     */
    BSONObj bsonMetrics();

    /**
     * Write all metrics to a CSV file
     * "start","2020-11-11T11:11:11.000+000","2020-11-11T11:11:12.000+000",
     * "serveStatus.A.a",123,123,...
     * "serverStatus.A.b",456,488,...
     * ...
     */
    void writeCSV(boost::filesystem::path dirfp, FTDCProcessId pmId);

    /**
     * Write all metrics to a CSV file suitable for Pandas dataframe import
     * "start","serveStatus.A.a","serverStatus.A.b",....
     * "2020-11-11T11:11:11.000+000",123,456,...
     * "2020-11-11T11:11:12.000+000",123,488,...
     * ...
     * As well as the data file with the above format a *.mapping.csv file
     * will be added alongside it.
     */
    void writePandasDataframeCSV(boost::filesystem::path dirfp, FTDCProcessId pmId);


    /**
     * Write all metrics to a jsonlines file suitable for VictorMetric's
     * /api/v1/import endpoint.
     * (Each metric will be one line only; The multiline format is just for this
     * comment.)
     * {"metric":
     *   {"__name__":"mongodb_ss_A_a","job":"mongodb","instance":"hostA:27018",
     *    "cl_role":"xxx","cl_id":"...","rs_nm":"rs1","rs_state":"1"},
     *   values":[1,1,2,...,n'th value],
     *   "timestamps":[1549891472010,1549891487724,1549891503438,...,n'th-epochms-timestamp]
     * }
     * {"metric":
     *   {"__name__":"mongodb_ss_A_b","job":"mongodb","instance":"hostA:27018",
     *   .... } }
     */
    void writeVMJsonLines(boost::filesystem::path dirfp, FTDCProcessId pmId,
                    std::map<std::string, std::string> topologyLabels);

    /**
     * Return a copy of one row of the metrics in a vector of base uint64_t type
     */
    std::vector<std::uint64_t> metricsRow(size_t i) {
        std::vector<std::uint64_t> r;
        r.reserve(_rowLength);
        r = std::vector<std::uint64_t>(metrics.begin() + cellOffset(i, 0),
                                       metrics.begin() + cellOffset(i + 1, 0));
        return r;
    }
    std::vector<std::uint64_t> metricsRow(std::string keyName) {
        return metricsRow(keyRow(keyName));
    }

    std::vector<std::uint64_t> nullsRow() {
        std::vector<std::uint64_t> r(_rowLength, 7777777777);
        return r;
    }

    size_t cellOffset(size_t row, size_t col) { return row * _rowLength + col; }

private:
    FTDCPMTimespan _tspan;
    uint32_t _stepMs;

    size_t _rowLength; //end time = _start + (sampleLength * _stepMs)

    std::vector<FTDCMSKeyNameType> _kNT;

    std::map<std::string, size_t> _keyRows;
};

/**
 * The minimal properties of a FTDC file needed for effective iteration.
 */
struct FTDCFileSpan {
    boost::filesystem::path path;
    FTDCPMTimespan timespan;
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
 * salvageChunkDtId: Use != Date_t::min() to see if unset or not.
 * salvageChunkFileSpan: The last kMetricsChunk for a process can be in another
 * file which doesn't have kMetadataDoc that would identify it as belonging to
 * this process. There are two ways this occurs: 1. the interim file. 2. After
 * a crash the new process will write the kMetadataDoc, then copy in the interim
 * file's chunk, then start writing its own.
 */

struct FTDCProcessMetrics {
    FTDCProcessId procId;
    std::map<Date_t, FTDCFileSpan> filespans;
    BSONObj metadataDoc;
    BSONObj lastRefDoc;
    //TODO: Date_t salvageChunkDtId = Date_t::min();
    //TODO: FTDCFileSpan salvageChunkFilespan;

    std::string rsName() const;
    std::string clusterRole() const;
    Date_t firstSampleTs() const;
    Date_t estimateLastSampleTs() const;
    std::map<std::string, BSONType> lastRefDocKeys();

    Status merge(const FTDCProcessMetrics& pm);
    void mergeRefDocKeys(const BSONObj& _refDoc);

    /**
     * Executes metadataAndTimeseries for each file in this process session,
     * The metadata doc is ignored. The metrics are concatenated together as
     * one larger array of timeseries.
     * tspan argument will be reduced to what overlaps with firstSampleTs()
     * - estimateLastSampleTs()
     * TODO: add timeshift-hack arg
     */
    StatusWith<FTDCMetricsSubset> timeseries(std::vector<std::string>& keys, 
                FTDCPMTimespan tspan, uint32_t sampleResolution);

    /**
     * This produces a map that has the same keys, one exception aside, that
     * topologyInfo.baseLabels() in exporter/topology_info.go does.
     *
     * The cluster id can not be determined from FTDC metrics files.
     *
     * Exception to mongodb_exporter rules: "rs_state" (eg. "1" (primary),
     * "2" (secondary), etc.) is left unset because it is not constant for
     * the span of the mongod process.
     */
    std::map<std::string, std::string> topologyLabels() const {
       std::map<std::string, std::string> m;
       m["cl_role"] = clusterRole();
       m["cl_id"] = "";
       m["rs_nm"] = rsName();
       //m["rs_state"] = "";
       return m;
    }

    //temporary debugging use
    friend std::ostream& operator<<(std::ostream& os, FTDCProcessMetrics& pm);
};

}  // namespace mongo
