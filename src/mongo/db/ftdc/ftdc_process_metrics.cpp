#include "mongo/db/ftdc/ftdc_process_metrics.h"
#include "mongo/db/ftdc/file_reader.h"

#include "mongo/db/bson/dotted_path_support.h"

namespace dps = ::mongo::dotted_path_support;

namespace mongo {

bool operator==(const FTDCProcessId& l, const FTDCProcessId& r) {
    return l.hostport == r.hostport && l.pid == r.pid;
}
bool operator!=(const FTDCProcessId& l, const FTDCProcessId& r) {
    return !(l==r);
}

bool operator<(const FTDCProcessId& l, const FTDCProcessId& r) {
    return l.hostport < r.hostport || (l.hostport == r.hostport && l.pid < r.pid);
}

bool FTDCPMTimespan::isValid() {
    return last > first;
}

bool FTDCPMTimespan::overlaps(FTDCPMTimespan& other) {
    return isValid() && other.isValid() &&
	((first >= other.first && first < other.last) ||
        (last > other.first && last <= other.last));
}
//        if (tspan.isValid() && ((s >= tspan.first && s < tspan.first) ||
//                        (e > tspan.first && e <= tspan.last))) {

FTDCMetricsSubset::FTDCMetricsSubset(std::vector<std::string> keys, FTDCPMTimespan tspan,
                            uint32_t sampleResolution) {
    _keys = keys;
    _tspan = tspan;
    _stepMs = sampleResolution;
    _sampleLength = (tspan.last.toMillisSinceEpoch() - tspan.first.toMillisSinceEpoch()) / _stepMs;
    for (size_t i = 0; i < _keys.size(); ++i) {
        _keyRow[_keys[i]] = i;
    }

    _metrics.resize(_sampleLength * _keys.size(), UINT64_MAX); //Max value being used to indicate unset
}

std::string FTDCProcessMetrics::rsName() const {
    BSONElement rsnmElem = dps::extractElementAtPath(metadataDoc, "getCmdLineOpts.parsed.replication.replSetName");
    if (rsnmElem.eoo()) {
         return "";
    } else {
         return rsnmElem.String();
    }
}

Date_t FTDCProcessMetrics::firstSampleTs() const {
    return filespans.begin()->second.timespan.first;
}

Date_t FTDCProcessMetrics::estimateLastSampleTs() const {
    return filespans.rbegin()->second.timespan.last;
}

//Unit test: FTDCProcessMetrics a.merge(b) should create the same doc as b.merge(a);
//Unit test: file (in sourceFilepaths) that is younger or identical version of should not change any member value
//Unit test: file that is the later, bigger-sample-length should raise estimated_end_ts
//Unit test: latersample.merge(onefile_oldersmallersample) should change no property
//Unit test: mismatching procId should return error
Status FTDCProcessMetrics::merge(const FTDCProcessMetrics& other) {
    if (procId != other.procId) {
        //It is a programming error if there is an attempt to merge metrics
        //of different processes
        return Status(ErrorCodes::BadValue,
                      "FTDCProcessMetrics::merge() on non-matching FTDCProcessId.");
    }

    /**
     * If other contains the same file (identified by the Date _id, not by
     * filepath) and that file is a younger, smaller-sample-length version then
     * we don't want to use any values from it: filepath, timespan and refDoc. 
     */
    for (std::map<Date_t, FTDCFileSpan>::const_iterator it = other.filespans.begin();
         it != other.filespans.end(); ++it) {
        auto date_id = it->first;
        auto oth_fpath = it->second.path;
        auto oth_timespan = it->second.timespan;
        auto oldfpItr = filespans.find(date_id);
        if (oldfpItr == filespans.end()) {
            filespans[date_id] = { oth_fpath, oth_timespan };
        } else {
            auto existing_fpath = oldfpItr->second.path;
            std::cerr << "Info: Duplicate FTDC files for " << procId.hostport <<
                    ", pid=" << procId.pid << ", starting datetime id=" <<
                    date_id << " found." << std::endl;
            if (oth_timespan.last < filespans[date_id].timespan.last) {
                std::cerr << "File " << oth_fpath << " will be ignored because file " <<
                    existing_fpath << " has more recent samples" << std::endl;
            } else {
                std::cerr << "File " << existing_fpath << "'s metrics will be discarded because file " <<
                    oth_fpath << " has more recent samples" << std::endl;
                filespans[date_id] = { oth_fpath, oth_timespan };
            }
        }
    }

    auto fSTs = firstSampleTs();
    auto oFSTs = other.firstSampleTs();
    if (oFSTs < fSTs) {
        metadataDoc = other.metadataDoc; //expected to be identical in all files, but for consistency let's use latest
        lastRefDoc = other.lastRefDoc;
    }

    return Status::OK();
}

StatusWith<FTDCMetricsSubset> FTDCProcessMetrics::timeseries(std::vector<std::string>& keys, 
		FTDCPMTimespan tspan, uint32_t sampleResolution) {
    FTDCMetricsSubset m(keys, tspan, sampleResolution);
    // TODO:
    // Init the BSON types of the keys first, log which metrics in output.keys are missing

    for (std::map<Date_t, FTDCFileSpan>::iterator it = filespans.begin();
         it != filespans.end(); ++it) {
	if (it->second.timespan.overlaps(tspan)) {
            FTDCFileReader reader;
            auto s = reader.open(it->second.path);
            if (s != Status::OK()) {
                return s;
            }
	    auto sET = reader.extractTimeseries(m);
            if (!sET.isOK()) {
                return sET;
            }
        }
    }
    return {m};
}

std::ostream& operator<<(std::ostream& os, FTDCProcessMetrics& pm) {
    std::string fpList;
    for (std::map<Date_t, FTDCFileSpan>::iterator it = pm.filespans.begin();
         it != pm.filespans.end(); ++it) {
        fpList = fpList + it->second.path.string() + " ";
    }
    os << pm.procId.hostport << " [" << pm.procId.pid << "] " << pm.rsName() << " " << fpList;
    return os;
}

}  // namespace mongo
