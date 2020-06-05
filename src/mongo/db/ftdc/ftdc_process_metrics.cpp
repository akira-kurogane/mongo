#include "mongo/db/ftdc/ftdc_process_metrics.h"

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

Date_t FTDCProcessMetrics::firstSampleTs() const {
    return timespans.begin()->second.first;
}

Date_t FTDCProcessMetrics::estimateLastSampleTs() const {
    return timespans.rbegin()->second.last;
}

//Unit test: FTDCProcessMetrics a.merge(b) should create the same doc as b.merge(a);
//Unit test: file (in sourceFilepaths) that is younger or identical version of should not change any member value
//Unit test: file that is later, bigger-sample-length should raise sampleCounts sum
//Unit test: file that is the later, bigger-sample-length should raise estimated_end_ts
//Unit test: latersample.merge(onefile_oldersmallersample) should change no property
//Unit test: mismatching procId should return error
//Unit test: no sampleCounts value is zero
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
     * we don't want to use any values from it: filepath, sampleCount, timespan
     * and refDoc. 
     */
    for (std::map<Date_t, boost::filesystem::path>::const_iterator it = other.sourceFilepaths.begin();
         it != other.sourceFilepaths.end(); ++it) {
        auto date_id = it->first;
        auto oth_fpath = it->second;
        auto oth_sampleCount = other.sampleCounts.find(date_id)->second;
        auto oth_timespan = other.timespans.find(date_id)->second;
        auto oldfpItr = sourceFilepaths.find(date_id);
        if (oldfpItr == sourceFilepaths.end()) {
            sourceFilepaths[date_id] = oth_fpath;
            sampleCounts[date_id] = oth_sampleCount;
            timespans[date_id] = oth_timespan;
        } else {
            auto existing_fpath = oldfpItr->second;
            std::cerr << "Info: Duplicate FTDC files for " << procId.hostport << ", pid=" << procId.pid <<
                    ", starting datetime id=" << date_id << " found." << std::endl;
            if (oth_sampleCount < sampleCounts[date_id]) {
                std::cerr << "File " << oth_fpath << " will be ignored because file " << existing_fpath << " has larger sample count" << std::endl;
            } else {
                std::cerr << "File " << existing_fpath << " will be ignored because file " << oth_fpath << "is larger" << std::endl;
                sourceFilepaths[date_id] = oth_fpath;
                sampleCounts[date_id] = oth_sampleCount;
                timespans[date_id] = oth_timespan;
            }
        }
    }

    auto fSTs = firstSampleTs();
    auto oFSTs = other.firstSampleTs();
    if (oFSTs < fSTs) {
        metadataDoc = other.metadataDoc; //expected to be identical, but for consistency let's use latest
        lastRefDoc = other.lastRefDoc;
    }

    return Status::OK();
}

std::ostream& operator<<(std::ostream& os, FTDCProcessMetrics& pm) {
    std::string fpList;
    for (std::map<Date_t, boost::filesystem::path>::iterator it = pm.sourceFilepaths.begin();
         it != pm.sourceFilepaths.end(); ++it) {
        fpList = fpList + it->second.string() + " ";
    }
    os << pm.procId.hostport << " [" << pm.procId.pid << "] " << "[TODO print rsName]" << " " << fpList;
    return os;
}

}  // namespace mongo
