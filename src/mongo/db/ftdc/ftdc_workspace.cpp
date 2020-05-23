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
bool confirmFTDCFile(const boost::filesystem::path& p, FTDCProcessMetrics& procMetrics) {
    if (!boost::filesystem::is_regular_file(p) || boost::filesystem::file_size(p) == 0) {
        return false;
    }
    FTDCFileReader reader;
    auto s = reader.open(p);
    if (s != Status::OK()) {
        return false;
    }
    StatusWith<FTDCProcessMetrics> swProcMetrics = reader.previewMetadataAndTimeseries();
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
        FTDCProcessMetrics pm;
        if (boost::filesystem::is_regular_file(*p)) {
            if (confirmFTDCFile(*p, pm)) {
                auto s = _addFTDCProcessMetrics(pm);
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
                     auto s = _addFTDCProcessMetrics(pm);
                }
            }
        }
    }

    return Status::OK();
}

std::set<boost::filesystem::path> FTDCWorkspace::filePaths() {
    return _paths;
}

std::map<std::string, std::map<std::string, std::set<FTDCProcessId>>>
FTDCWorkspace::topology() {
    return _rs;
}

void FTDCWorkspace::clear() {
    _rs.clear();
    _paths.clear();
    _pmMap.clear();
}

Status FTDCWorkspace::_addFTDCProcessMetrics(FTDCProcessMetrics& pm) {

    if (_pmMap.find(pm.procId) == _pmMap.end()) {
        _pmMap[pm.procId] = pm;
    } else {
        auto s = _pmMap[pm.procId].merge(pm);
	if (!s.isOK()) {
            return s;
	}
    }
    for (auto const& p : pm.sourceFilepaths) {
        _paths.insert(p.second);
    }

    auto rsnm = pm.rsName();
    auto hostpost = pm.procId.hostport;
    auto hfl = _rs.find(rsnm);
    if (hfl == _rs.end()) {
	std::map<std::string, std::set<FTDCProcessId>> x;
        _rs[rsnm] = x;
    }
    auto fl = _rs[rsnm].find(hostpost);
    if (fl == _rs[rsnm].end()) {
	std::set<FTDCProcessId> x;
	_rs[rsnm][hostpost] = x;
    }
    _rs[rsnm][hostpost].insert(pm.procId);

    return Status::OK();
}

} // namespace mongo
