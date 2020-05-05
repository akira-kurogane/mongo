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

    std::map<std::string, std::map<std::string, std::set<boost::filesystem::path>>> topology();

private:
    //Map of all ProcessMetrics
    std::map<std::tuple<std::string, unsigned long>, ProcessMetrics> _pmMap;

    // Paths of all FTDC files in this workspace
    std::set<boost::filesystem::path> _paths;

    // Replica set -> hostport -> filepaths two-layer map
    // Dev note: Ideally we would have a cluster level too, but there are no
    // clusterId values in the metrics.
    std::map<std::string, std::map<std::string, std::set<boost::filesystem::path>>> _rs;

    // Add filepath to _paths and also into _rs by topology
    // TODO: remove the path arg, fill _paths from procMetrics.sourceFilepaths instead
    Status _addFTDCFilepath(boost::filesystem::path p, ProcessMetrics& procMetrics);
};

}  // namespace mongo
