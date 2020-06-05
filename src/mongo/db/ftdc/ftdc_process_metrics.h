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

}  // namespace mongo
