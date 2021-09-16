#pragma once

#include <boost/filesystem/path.hpp>
//#include <tuple>
#include <vector>
#include <set>
#include <map>
#include <string>

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
    std::vector<std::string> keys();

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

    /**
     * Returns a FTDCMetricsSubset that is a merge of all FTDCMetricsSubsets
     *   for all hosts and all their separate process instances.
     * To keep metrics from different hosts in different rows the metric names
     *   are annotated with the hostport string. Eg.
     *   "locks.global.W.totalTime/hosta.domain.com:27018".
     * "HNAK" -> Hostname Annotated Keys
     */
    FTDCMetricsSubset hnakMergedTimeseries(std::vector<std::string>& keys,
                    FTDCPMTimespan timespan, uint32_t sampleResolution = 1000);

private:
    // Map of all FTDCProcessMetrics
    std::map<FTDCProcessId, FTDCProcessMetrics> _pmMap;

    // list of metricnames under each unique <hostname:port:pid>
    // Useful for merging metrics from different hosts sorted as close as possible
    //   to their natural order returned in the getDiagnosticData output.
    std::map<std::string, std::list<std::string>> _metricsByProcHierarchy();

    // Reduction of the hierarchy in metricsByProcHierarchy() to have a parent
    //   level of hosts only. I.e. lists of metricnames under each unique
    //   <hostname:port>.
    std::map<std::string, std::list<std::string>> _metricsByHostHierarchy();

    // Map of map via replset name -> hostpost to {hostport, pid}, which is the
    // key to the FTDCProcessMetrics in _pmMap
    std::map<std::string, std::map<std::string, std::set<FTDCProcessId>>> _rs;

    // Add ProcessMetric object to _pmMap, and also into _rs by topology
    Status _addFTDCProcessMetrics(FTDCProcessMetrics& pm);
};

/**
 * Used this way with a FTDCMetricsRange reference:
 *    std::cout << "..." << FTDCMetricsStreamJSONFormatter(ftdcMR) << "..."
 * Outputs the array of unint64_t values in the FTDCMetricsRange's range as
 * the valid JSON representation delimited with commas.
 */
class FTDCMetricsStreamJSONFormatter {
public:
    FTDCMetricsStreamJSONFormatter(FTDCMetricsRange& mrref) : _mrref(mrref) {}
    std::ostream& operator()(std::ostream& os) const;
private:
    FTDCMetricsRange& _mrref;
};

std::ostream& operator<<(std::ostream& os, FTDCMetricsStreamJSONFormatter fmtr);

}  // namespace mongo
