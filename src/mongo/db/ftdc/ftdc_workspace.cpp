#include "mongo/platform/basic.h"

#include <boost/filesystem.hpp>

#include "mongo/db/ftdc/ftdc_workspace.h"

#include "mongo/base/status_with.h"

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
//    StatusWith<FTDCProcessMetrics> swProcMetrics = reader.extractProcessMetricsHeaders();
//    if (swProcMetrics.isOK()) {
//        procMetrics = swProcMetrics.getValue();
//	return true;
//    }
//    std::cerr << swProcMetrics.getStatus().reason() << std::endl;
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
    std::set<boost::filesystem::path> paths;
    for (std::map<FTDCProcessId, FTDCProcessMetrics>::const_iterator itPM = _pmMap.begin();
         itPM != _pmMap.end(); ++itPM) {
        auto pm = itPM->second;
        for (std::map<Date_t, boost::filesystem::path>::const_iterator itFp = pm.sourceFilepaths.begin();
             itFp !=pm.sourceFilepaths.end(); ++itFp) {
            auto path = itFp->second;
            paths.insert(path);
        }
    }
    return paths;
}

std::map<std::string, std::map<std::string, std::set<FTDCProcessId>>>
FTDCWorkspace::topology() const {
    return _rs;
}

const FTDCProcessMetrics& FTDCWorkspace::processMetrics(FTDCProcessId pmId) {
    return _pmMap[pmId];
}

std::set<std::string> FTDCWorkspace::keys() {
    std::set<std::string> m;
    for (std::map<FTDCProcessId, FTDCProcessMetrics>::const_iterator itPM = _pmMap.begin();
         itPM != _pmMap.end(); ++itPM) {
        auto pm = itPM->second;
        for (std::map<std::string, BSONType>::const_iterator it = pm.keys.begin();
             it != pm.keys.end(); ++it) {
            auto k = it->first;
            m.insert(k);
	}
    }
    return m;
}

FTDCPMTimespan FTDCWorkspace::boundaryTimespan() {
    Date_t first = Date_t::max();
    Date_t last = Date_t::min();
    for (std::map<FTDCProcessId, FTDCProcessMetrics>::const_iterator itPM = _pmMap.begin();
         itPM != _pmMap.end(); ++itPM) {
        auto pm = itPM->second;
        for (std::map<Date_t, FTDCPMTimespan>::const_iterator it = pm.timespans.begin();
             it !=pm.timespans.end(); ++it) {
            auto tspan = it->second;
            if (tspan.first < first) {
                first = tspan.first;
	    }
            if (tspan.last > last) {
                last = tspan.last;
	    }
        }
    }
    return {first, last};
}

void FTDCWorkspace::clear() {
    _rs.clear();
    _pmMap.clear();
}

Status FTDCWorkspace::_addFTDCProcessMetrics(FTDCProcessMetrics& pm) {

    /**
     * We don't want it if there is no metadataDoc. This happens as a matter
     * of course for FTDCProcessMetrics generated from a metrics.interim file.
     * It would be an improvement if we identify and merge with the other
     * metrics file that the FTDCFileManager would recoverInterimFile into,
     * but now we do nothing.
     */
    if (pm.metadataDoc.isEmpty()) {
        return Status::OK();
    }

    if (_pmMap.find(pm.procId) == _pmMap.end()) {
        _pmMap[pm.procId] = pm;
    } else {
        auto s = _pmMap[pm.procId].merge(pm);
	if (!s.isOK()) {
            return s;
	}
    }

    auto rsnm = ""; //TODO extract rsname for FTDCProcessMetrics object
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
