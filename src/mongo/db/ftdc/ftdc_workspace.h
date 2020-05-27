#pragma once

#include <boost/filesystem/path.hpp>
//#include <tuple>
#include <vector>
#include <set>
#include <map>

#include "mongo/base/status.h"
#include "mongo/db/jsobj.h"
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
    topology();

    const FTDCProcessMetrics& processMetrics(FTDCProcessId pmId);

private:
    // Map of all FTDCProcessMetrics
    std::map<FTDCProcessId, FTDCProcessMetrics> _pmMap;

    // Paths of all FTDC files in this workspace
    std::set<boost::filesystem::path> _paths;

    // Map of map via replset name -> hostpost to {hostport, pid}, which is the
    // key to the FTDCProcessMetrics in _pmMap
    std::map<std::string, std::map<std::string, std::set<FTDCProcessId>>> _rs;

    // Add ProcessMetric object to _pmMap, and filepath to _paths and also into
    // _rs by topology
    Status _addFTDCProcessMetrics(FTDCProcessMetrics& pm);
};

}  // namespace mongo
