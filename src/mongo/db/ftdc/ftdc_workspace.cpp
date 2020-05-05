#include "mongo/platform/basic.h"

#include <boost/filesystem.hpp>

#include "mongo/db/ftdc/ftdc_workspace.h"

#include "mongo/base/status_with.h"
#include "mongo/db/bson/dotted_path_support.h"

namespace dps = ::mongo::dotted_path_support;

namespace mongo {

namespace {

/**
 * Open a file with FTDCFileReader, read into as far as the initial metadata
 * packed doc. Extract some info and pass back in the topologyId ref parameter.
 */
bool confirmFTDCFile(const boost::filesystem::path& p, ProcessMetrics& procMetrics) {
    if (!boost::filesystem::is_regular_file(p) || boost::filesystem::file_size(p) == 0) {
        return false;
    }
    FTDCFileReader reader;
    auto s = reader.open(p);
    if (s != Status::OK()) {
        return false;
    }
    StatusWith<ProcessMetrics> swProcMetrics = reader.previewMetadataAndTimeseries();
    if (swProcMetrics.isOK()) {
        procMetrics = swProcMetrics.getValue();
	return true;
    }
    std::cerr << swProcMetrics.getStatus().reason() << std::endl;
    return false;
}

} // namespace

FTDCWorkspace::FTDCWorkspace() {}

FTDCWorkspace::~FTDCWorkspace() {}

Status FTDCWorkspace::addFTDCFiles(std::vector<boost::filesystem::path> paths, bool recursive) {

    for (auto p = paths.begin(); p != paths.end(); ++p) {
        ProcessMetrics pm;
        if (boost::filesystem::is_regular_file(*p)) {
            if (confirmFTDCFile(*p, pm)) {
                auto s = _addFTDCFilepath(*p, pm);
            }
        } else if (boost::filesystem::is_directory(*p)) {
            //if (recursive) { 
            //boost::filesystem::recursive_directory_iterator dItr(*p);
            boost::filesystem::directory_iterator dItr(*p);
            boost::filesystem::directory_iterator endItr;
            for (; dItr != endItr; ++dItr) {
                boost::filesystem::directory_entry& dEnt = *dItr;
                auto f = dEnt.path().filename();
                if (confirmFTDCFile(dEnt.path(), pm)) {
                     auto s = _addFTDCFilepath(dEnt.path(), pm);
                }
            }
        }
    }

    return Status::OK();
}

std::set<boost::filesystem::path> FTDCWorkspace::filePaths() {
    return _paths;
}

std::map<std::string, std::map<std::string, std::set<boost::filesystem::path>>>
FTDCWorkspace::topology() {
    return _rs;
}

void FTDCWorkspace::clear() {
    _rs.clear();
    _paths.clear();
}

Status FTDCWorkspace::_addFTDCFilepath(boost::filesystem::path p, ProcessMetrics& procMetrics) {

    std::tuple<std::string, unsigned long> pmId = {procMetrics.hostport, procMetrics.pid};
    if (_pmMap.find(pmId) == _pmMap.end()) {
        _pmMap[pmId] = procMetrics;
    } else {
        ; //_pmMap[pmId].merge(procMetrics); TODO implement the merge method
    }

    _paths.insert(p);

    auto rsnm = procMetrics.rsName();
    auto hostpost = procMetrics.hostport;
    auto hfl = _rs.find(rsnm);
    if (hfl == _rs.end()) {
	std::map<std::string, std::set<boost::filesystem::path>> x;
        _rs[rsnm] = x;
    }
    auto fl = _rs[rsnm].find(hostpost);
    if (fl == _rs[rsnm].end()) {
	std::set<boost::filesystem::path> x;
	_rs[rsnm][hostpost] = x;
    }
    _rs[rsnm][hostpost].insert(p);

    return Status::OK();
}

} // namespace mongo
