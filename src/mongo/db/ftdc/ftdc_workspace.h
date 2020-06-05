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

    const std::set<boost::filesystem::path>& filePaths();

};

}  // namespace mongo
