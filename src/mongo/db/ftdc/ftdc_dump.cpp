#include "mongo/platform/basic.h"

#include <boost/filesystem.hpp>

#include "mongo/db/ftdc/file_reader.h"
#include "mongo/db/ftdc/ftdc_workspace.h"
#include "mongo/db/bson/dotted_path_support.h"

namespace fs = boost::filesystem;

using namespace mongo;

namespace dps = ::mongo::dotted_path_support;

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
        std::cerr << "usage: " << argv[0] << " <ftdc-file-or-dir-path> [<ftdc-file-or-dir-path>*]" << std::endl;
        exit(1);
    }

    size_t i;
    std::vector<fs::path> av;
    for (i = 1; i < static_cast<size_t>(argc); ++i) {
        av.push_back(fs::path(argv[i]));
    }

    FTDCWorkspace ws;
    Status s = ws.addFTDCFiles(av);
    auto fp = ws.filePaths();

    if (fp.size() == 0) {
        std::cerr << "There are no FTDC metrics files at " << argv[1];
	size_t i;
	for (i = 2; i < static_cast<size_t>(argc); ++i) {
	    std::cerr << ", " << argv[i];
	}
        std::cerr << std::endl;
        exit(1);
    }

    for (auto fpItr = fp.begin(); fpItr != fp.end(); ++fpItr) {
        printFTDCFileSummary(*fpItr);
    }

    exit(0);
}
