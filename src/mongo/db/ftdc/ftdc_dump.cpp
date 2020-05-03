#include "mongo/platform/basic.h"

#include <boost/filesystem.hpp>

#include "mongo/db/ftdc/file_reader.h"
#include "mongo/db/bson/dotted_path_support.h"

namespace fs = boost::filesystem;

using namespace mongo;

namespace dps = ::mongo::dotted_path_support;

bool isFTDCFile(const fs::path& p) {
    if (!fs::is_regular_file(p) || fs::file_size(p) == 0) {
        return false;
    }
    FTDCFileReader reader;
    auto s = reader.open(p);
    if (s != Status::OK()) {
        return false;
    }
    auto sw = reader.hasNext();
    return sw.isOK();
}

std::vector<fs::path> findAllFTDCFiles(const fs::path& p) {
    std::vector<fs::path> fpaths;

    if (fs::is_regular_file(p)) {
        if (isFTDCFile(p)) {
            fpaths.emplace_back(p);
        }
    } else if (fs::is_directory(p)) {
        //fs::recursive_directory_iterator dItr(p);
        fs::directory_iterator dItr(p);
        fs::directory_iterator endItr;
        for (fs::directory_iterator dItr(p); dItr != endItr; ++dItr) {
            fs::directory_entry& dEnt = *dItr;
            auto f = dEnt.path().filename();
            if (isFTDCFile(dEnt.path())) {
                 fpaths.emplace_back(dEnt.path());
            }
        }
    }

    std::sort(fpaths.begin(), fpaths.end());

    return fpaths;
}

bool printFTDCFileSummary(const fs::path& p) {
    FTDCFileReader reader;

    auto s = reader.open(p);
    if (s != Status::OK()) {
        std::cerr << "Failed to open file " << p.string() << std::endl;
        return false;
    }

    std::vector<BSONObj> list;
    auto sw = reader.hasNext();
    
    unsigned int ctr = 0;
    bool resetMC = true;
    Date_t mc_start_ts;
    Date_t mc_end_ts;
    /**
     * Intuition break warning: FTDCFileReader::hasNext() advances the position, not next()
     */
    while (sw.isOK() && sw.getValue() && ctr++ < 1000) {
        auto ftdcType = std::get<0>(reader.next());
        auto d = std::get<1>(reader.next()).getOwned();
        auto ts = std::get<2>(reader.next()); //This is the metricChunk's first "start" ts only. Stays constant whilst the _pos in the concatenated arrays of metrics are iterated

        if (ftdcType == FTDCBSONUtil::FTDCType::kMetricChunk) {
            if (resetMC) {
                 mc_start_ts = ts;
                 BSONElement pidElem = dps::extractElementAtPath(d, "serverStatus.pid");
                 if (pidElem.eoo()) {
                     return false;
                 } else {
                     std::cout << "serverStatus.pid = " << pidElem.Long() << std::endl;
                 }
            }
            resetMC = false;
            BSONElement startTSElem = dps::extractElementAtPath(d, "start");
            if (startTSElem.eoo()) {
                 return false;
            } else {
                mc_end_ts = startTSElem.Date();
            }
        } else if (ftdcType == FTDCBSONUtil::FTDCType::kMetadata) {
            if (!resetMC) {
                 std::cout << mc_start_ts << " - " << mc_end_ts.toString() << std::endl;
            }
            resetMC = true;
            BSONElement shElem = dps::extractElementAtPath(d, "hostInfo.system.hostname");
            BSONElement bivElem = dps::extractElementAtPath(d, "buildInfo.version");
            if (shElem.eoo() || bivElem.eoo()) {
                 return false;
            } else {
                //std::cout << d.jsonString(Strict) << std::endl;
                std::cout << shElem.String() << " - " << bivElem.String() << std::endl;
            }
        } else {
            MONGO_UNREACHABLE;
        }
        sw = reader.hasNext();
    }

    std::cout << mc_start_ts << " - " << mc_end_ts.toString() << std::endl;

    return true;
}

int main(int argc, char* argv[], char** envp) {
    if (argc < 2) {
        std::cerr << "usage: " << argv[0] << " <ftdc-file-or-dir-path>" << std::endl;
        exit(1);
    }
    fs::path p(argv[1]);
    std::vector<fs::path> fp = findAllFTDCFiles(argv[1]);
    if (fp.size() == 0) {
        std::cerr << "There are no FTDC metrics files at " << argv[1] << std::endl;
        exit(1);
    }
    bool rds = true;
    for (auto fpItr = fp.begin(); fpItr != fp.end(); ++fpItr) {
        printFTDCFileSummary(*fpItr);
    }
    exit(rds ? 0 : 1);
}
