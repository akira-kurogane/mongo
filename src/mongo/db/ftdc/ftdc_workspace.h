#pragma once

#include <boost/filesystem/path.hpp>
//#include <tuple>
#include <vector>
#include <set>
#include <map>

#include "mongo/base/status.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/ftdc/ftdc_process_metrics.h"
#include "mongo/db/ftdc/file_reader.h"

namespace mongo {

/**
 * A container of FTDC files, exposing by three dimensions:
 * Filename, MongoDB topology (replica set, host), and
 * timeseries range.
 */
class FTDCWorkspace {

public:
    FTDCWorkspace();
    ~FTDCWorkspace();

    Status addFTDCFiles(std::vector<boost::filesystem::path> paths, bool recursive = false);

    /**
     * Clear the list of FTDC files
     */
    void clear();

    std::set<boost::filesystem::path> filePaths();

    /**
     * Topology is replicaset name -> hostport
     *
     * Cluster is missing because there is no clusterId in the FTDC metrics.
     */
    std::map<std::string, std::map<std::string, std::set<FTDCProcessId>>>
    topology() const;

    /**
     * Set of all unique <hostname>:<port> strings
     */
    std::set<std::string> hostPortList() const {
        std::set<std::string> v;
        auto tp = topology();
        for (auto const& [rsnm, hpvals] : tp) {
            for (auto const& [hp, pmIds] : hpvals) {
                v.insert(hp);
            }
        }
        return v;
    }


    /**
     * Simple getter for the FTDCProcessMetrics objects
     */
    const FTDCProcessMetrics& processMetrics(FTDCProcessId pmId);

    /**
     * A union of all key names from all FTDCProcessMetrics objects
     */
    std::set<std::string> keys();

    /**
     * The min first sample and max last (estimated) sample timestamps
     */
    FTDCPMTimespan boundaryTimespan();

    /**
     * Return the timeseries from all process metrics, filtered by
     *   the argument.
     * keys is a vector so a presentation ordered is implied; it is passed as
     * non-const ref value so compulsory fields such as "start" can be added
     * if they were absent.
     */
    std::map<FTDCProcessId, FTDCMetricsSubset> timeseries(std::vector<std::string>& keys,
                    FTDCPMTimespan timespan, uint32_t sampleResolution = 1000);

private:
    // Map of all FTDCProcessMetrics
    std::map<FTDCProcessId, FTDCProcessMetrics> _pmMap;

    // Map of map via replset name -> hostpost to {hostport, pid}, which is the
    // key to the FTDCProcessMetrics in _pmMap
    std::map<std::string, std::map<std::string, std::set<FTDCProcessId>>> _rs;

    // Add ProcessMetric object to _pmMap, and also into _rs by topology
    Status _addFTDCProcessMetrics(FTDCProcessMetrics& pm);
};

}  // namespace mongo
