#include "mongo/platform/basic.h"
#include <list>

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

//Utility function used by approximate_seq_merge
bool all_lists_empty(std::map<std::string, std::list<std::string>>& hml) {
  bool r = true;
  for (auto& [h, l] : hml) {
    if (!l.empty()) {
      r = false;
    }
  }
  return r;
}

//Utility function used by approximate_seq_merge
bool weight_fraction_sort(std::tuple<std::string, size_t, float> a, std::tuple<std::string, size_t, float> b) {
  auto aw = std::get<2>(a); //weight fraction
  auto bw = std::get<2>(b); //weight fraction
  return aw < bw ? true : (
    aw > bw ? false : (
      std::get<1>(a) < std::get<1>(b) //count
    )
  );
}

/**
 * Merge the lists of strings from multiple parents (parent group by string of host,
 *   or host + pid) into one unique list. The point of this unusual merge it is preserves
 *   the original order of the lists as best as possible.
 * Used in FTDCWorkspace to merge the metric keys from multiple mongod processes. The
 *   keys are rehydrated from the FTDC metric files in the order of the getDiagnosticData
 *   command output. It will nearly entirely match but there are differences between
 *   those from version to version, when replica set members ids differ, etc.
 */
std::list<std::tuple<std::string, std::string>> approximate_seq_merge(std::map<std::string, std::list<std::string>>& hml) {
  std::list<std::tuple<std::string, std::string>> sl; //sorted list with metric name, hostname tuples

  std::map<std::string, size_t> mn_counts;
  for (auto& [h, l] : hml) {
    for (const auto& mn : l) {
      mn_counts[mn]++;
    }
  }

  while (!all_lists_empty(hml)) {
    std::map<std::string, size_t> mn_freq; //frequency by metric name
    std::vector<std::tuple<std::string, size_t, float>> w_mn_freq; //weighted frequency by metric name
    for (auto& [h, l] : hml) {
      if (!l.empty()) {
        auto& mn = l.front(); //metric name
        mn_freq[mn]++;
      }
    }
    for (auto [mn, c] : mn_freq) {
      w_mn_freq.push_back({mn, c, float(c) / mn_counts[mn]});
    }
    std::sort(w_mn_freq.begin(), w_mn_freq.end(), weight_fraction_sort);
    auto sel_mn = std::get<0>(*w_mn_freq.rbegin());

    std::vector<std::string> uhq; //unpopped hosts
    for (auto& [h, l] : hml) {
      if (l.front() == sel_mn) {
        sl.push_back({h, sel_mn});
        l.pop_front();
        mn_counts[sel_mn]--;
      } else {
        uhq.push_back(h);
      }
    }
    if (mn_counts[sel_mn]) {
      for (auto h : uhq) {
        auto& l = hml[h];
        auto p = find(l.begin(), l.end(), sel_mn);
        if (p != l.end()) {
          sl.push_back({h, sel_mn});
          l.erase(p);
          mn_counts[sel_mn]--;
        }
      }
    }
    assert(mn_counts[sel_mn] == 0);
  }
  return sl;
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
                    //don't recurse hidden dirs or attempt to FTDC-parse hidden files
                    if (f.string()[0] == '.') {
                       dItr.no_push();
                    } else {
                       if (extractPMHeaders(dEnt.path(), pm)) {
                          auto s = _addFTDCProcessMetrics(pm);
                       }
                    }
                }
            } else {
                boost::filesystem::directory_iterator dItr(*p);
                boost::filesystem::directory_iterator endItr;
                for (; dItr != endItr; ++dItr) {
                    boost::filesystem::directory_entry& dEnt = *dItr;
                    auto f = dEnt.path().filename();
                    //don't attempt to FTDC-parse hidden files
                    if (f.string()[0] == '.') {
                       ;
                    } else {
                       if (extractPMHeaders(dEnt.path(), pm)) {
                          auto s = _addFTDCProcessMetrics(pm);
                       }
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

std::map<std::string, std::list<std::string>> FTDCWorkspace::metricsByProcHierarchy() {
    std::map<std::string, std::list<std::string>> hml;
    for (auto& [pmId, pm] : _pmMap) {
        std::list<std::string> l;
        for (auto k : pm.lastRefDocKeys()) {
            l.push_back(k.keyName);
        }
        hml[pmId.hostport + ":" + std::to_string(pmId.pid)] = l;
    }
    return hml;
}

std::map<std::string, std::list<std::string>> FTDCWorkspace::metricsByHostHierarchy() {
    std::map<std::string, std::list<std::string>> r;
    std::map<std::string, std::list<std::string>> pmVsL = metricsByProcHierarchy();
    std::map<std::string, std::map<std::string, std::list<std::string>>> hm;
    for (auto& [pmid_str, pkl] : pmVsL) {
        std::string hp = pmid_str.substr(0, pmid_str.find_last_of(':')); //extract out the hostport substr
        hm[hp][pmid_str] = pkl;
    }
    for (auto& [h, pml] : hm) {
        auto sl = approximate_seq_merge(pml); //std::list<std::tuple<std::string PARENTLABEL, std::string METRIC>>
        std::list<std::string> new_l;
        for (auto tpl : sl) {
            new_l.push_back(std::get<1>(tpl));
        }
        r[h] = new_l;
    }
    return r;
}

std::vector<std::string> FTDCWorkspace::keys() {
    std::vector<std::string> v;
    auto ml = metricsByProcHierarchy();
    auto sl = approximate_seq_merge(ml); //std::list<std::tuple<std::string PARENTLABEL, std::string METRIC>>

    //"start" is a compulsory first metric that will appear once for each process' metrics. Insert once
    //  and skip the duplicates.
    assert(std::get<1>(sl.front) == "start");
    v.push_back("start");

    std::string last_k;
    for (auto tpl : sl) {
        auto k = std::get<1>(tpl);
        if (k != "start") {
            if (k != last_k) {
                v.push_back(k);
                last_k = k;
            }
        }
    }
    return v;
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

FTDCMetricsSubset
FTDCWorkspace::hnakMergedTimeseries(std::vector<std::string>& keys, FTDCPMTimespan timespan, uint32_t sampleResolution) {
    auto pi_ts = timeseries(keys, timespan, sampleResolution);
    auto hml = metricsByHostHierarchy();

    if (!hml.size()) {
        //Have to return something - so return just the timestamp metric row.
        //  (This is always filled in by the FTDCMetricsSubset ctor for consistency.)
        //TODO consider if returning a completely empty (i.e. zero rows) FTDCMetricsSubset is possible.
        FTDCMetricsSubset blank_ms({"start"}, timespan, sampleResolution);
        return blank_ms;
    }

    auto sl = approximate_seq_merge(hml); //std::list<std::tuple<std::string HOST, std::string METRIC>>

    std::vector<std::string> merged_keys;
    //"start" is a compulsory first metric that will appear once for each process' metrics. Insert once
    //  and skip the duplicates.
    assert(std::get<1>(sl.front) == "start");
    merged_keys.push_back("start");  

    for (auto tpl : sl) {
        auto k = std::get<1>(tpl);
        auto hp = std::get<0>(tpl);
        if (k != "start") {
            merged_keys.push_back(k + "/" + hp);
        }
    }

    FTDCMetricsSubset mms(merged_keys, timespan, sampleResolution);
    //MAIN TODO iterate pi_ts again, copy metric values from each FTDCMetricsSubset to this new
    //  FTDCMetricsSubset that is the superset of them, both in time range and key set.
    return mms;
}

} // namespace mongo
