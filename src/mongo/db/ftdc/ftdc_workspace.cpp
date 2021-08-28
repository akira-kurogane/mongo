#include "mongo/platform/basic.h"

// TODO use stdc++17 std::filesystem instead of boost if we update the build scripts
#include <boost/filesystem.hpp>

#include "mongo/db/ftdc/ftdc_workspace.h"

#include "mongo/base/status_with.h"

namespace mongo {

namespace {

/**
 * FTDCFileReader::extractProcessMetricsHeaders() success implies this is a valid FTDC file.
 */
bool extractPMHeaders(const boost::filesystem::path& p, FTDCProcessMetrics& procMetrics) {
    if (!boost::filesystem::is_regular_file(p) || boost::filesystem::file_size(p) == 0) {
        return false;
    }
    FTDCFileReader reader;
    auto s = reader.open(p);
    if (s != Status::OK()) {
        return false;
    }
    StatusWith<FTDCProcessMetrics> swProcMetrics = reader.extractProcessMetricsHeaders();
    if (swProcMetrics.isOK()) {
        procMetrics = swProcMetrics.getValue();
        return true;
    }
    bool nonbson_found = swProcMetrics.getStatus() == ErrorCodes::InvalidLength;
    if (!(/*ignore_nonftdc && */ nonbson_found)) {
        std::cerr << swProcMetrics.getStatus().reason() << std::endl;
    }
    return false;
}

} // namespace

FTDCWorkspace::FTDCWorkspace() {}

FTDCWorkspace::~FTDCWorkspace() {}

Status FTDCWorkspace::addFTDCFiles(std::vector<boost::filesystem::path> paths, bool recursive) {

    for (auto p = paths.begin(); p != paths.end(); ++p) {
        FTDCProcessMetrics pm;
        if (boost::filesystem::is_regular_file(*p)) {
            if (extractPMHeaders(*p, pm)) {
                auto s = _addFTDCProcessMetrics(pm);
            }
        } else if (boost::filesystem::is_directory(*p)) {
            if (recursive) {
                boost::filesystem::recursive_directory_iterator dItr(*p);
                boost::filesystem::recursive_directory_iterator endItr;
                for (; dItr != endItr; ++dItr) {
                    boost::filesystem::directory_entry& dEnt = *dItr;
                    auto f = dEnt.path().filename();
                    if (extractPMHeaders(dEnt.path(), pm)) {
                         auto s = _addFTDCProcessMetrics(pm);
                    }
                }
            } else {
                boost::filesystem::directory_iterator dItr(*p);
                boost::filesystem::directory_iterator endItr;
                for (; dItr != endItr; ++dItr) {
                    boost::filesystem::directory_entry& dEnt = *dItr;
                    auto f = dEnt.path().filename();
                    if (extractPMHeaders(dEnt.path(), pm)) {
                         auto s = _addFTDCProcessMetrics(pm);
                    }
                }
            }
        }
    }

    //TODO: when salvageChunk* values are returned by extractProcessMetricsHeaders()
    // post-process to match them to found FTDCProcessMetrics. There won't necessarily
    // be a match for all, though.

    return Status::OK();
}

std::set<boost::filesystem::path> FTDCWorkspace::filePaths() {
    std::set<boost::filesystem::path> paths;
    for (std::map<FTDCProcessId, FTDCProcessMetrics>::const_iterator itPM = _pmMap.begin();
         itPM != _pmMap.end(); ++itPM) {
        auto pm = itPM->second;
        for (std::map<Date_t, FTDCFileSpan>::const_iterator itFp = pm.filespans.begin();
             itFp !=pm.filespans.end(); ++itFp) {
            auto fspn = itFp->second;
            paths.insert(fspn.path);
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

//TODO: return the map of keyname -> BSONType like the PM method does?
std::set<std::string> FTDCWorkspace::keys() {
    std::set<std::string> m;
    for (std::map<FTDCProcessId, FTDCProcessMetrics>::const_iterator itPM = _pmMap.begin();
         itPM != _pmMap.end(); ++itPM) {
        auto pm = itPM->second;
        auto pmKeys = pm.lastRefDocKeys();
        for (std::map<std::string, BSONType>::const_iterator it = pmKeys.begin();
             it != pmKeys.end(); ++it) {
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
        for (std::map<Date_t, FTDCFileSpan>::const_iterator it = pm.filespans.begin();
             it !=pm.filespans.end(); ++it) {
            auto tspan = it->second.timespan;
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
     * TODO: return this as another 'salvage chunk' item instead if we implement
     * the salvageChunk* properties in FTDCProcessMetrics
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

std::map<FTDCProcessId, FTDCMetricsSubset> FTDCWorkspace::timeseries(
                std::vector<std::string>& keys,
                FTDCPMTimespan tspan, uint32_t sampleResolution) {
    std::map<FTDCProcessId, FTDCMetricsSubset> resultMap;

    for (auto& [pmId, pm] : _pmMap) {
        FTDCPMTimespan pmTspan = {pm.firstSampleTs(), pm.estimateLastSampleTs()};
        if (tspan.isValid() && tspan.overlaps(pmTspan)) { 
            auto swTF = pm.timeseries(keys, tspan, sampleResolution);
            if (!swTF.isOK()) {
                std::cerr << "Error attempting to read timeseries data from " << pmId.hostport <<
                        " (pid=" << pmId.pid << "): " << swTF.getStatus().reason() << "\n";
                continue;
            }
            resultMap.insert(std::make_pair(pmId, std::move(swTF.getValue())));
        }
    }
    return resultMap;
}

} // namespace mongo
