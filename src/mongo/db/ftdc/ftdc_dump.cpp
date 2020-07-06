#include "mongo/platform/basic.h"

#include <boost/filesystem.hpp>

#include "mongo/db/ftdc/ftdc_workspace.h"

namespace fs = boost::filesystem;

using namespace mongo;

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
        std::cerr << "There are no FTDC metrics files at " << av[0];
        size_t i;
        for (i = 1; i < av.size(); ++i) {
            std::cerr << ", " << av[i];
        }
        std::cerr << std::endl;
        exit(1);
    }

    auto tspan = ws.boundaryTimespan();
    std::cout << "Samples between " << tspan.first << " - " << tspan.last << std::endl;
        
    auto tp = ws.topology();
    for (auto const& [rsnm, hpvals] : tp) {
        std::cout << rsnm << std::endl;
        for (auto const& [hp, pmIds] : hpvals) {
            std::cout << "  " << hp << std::endl;
            for (auto const& pmId : pmIds) {
                std::cout << "    " << pmId.hostport << ":" << pmId.pid << std::endl;
                auto pm = ws.processMetrics(pmId);
                std::cout << "    " << pm.firstSampleTs() << " - " << pm.estimateLastSampleTs() << std::endl;
            }
        }
    }

    Date_t testRangeS =  tspan.first + ((tspan.last - tspan.first) * 4) / 10;
    Date_t testRangeE = testRangeS + Seconds(250);
    std::vector<std::string> keys = {
        "serverStatus.wiredTiger.cache.bytes read into cache",
        "serverStatus.wiredTiger.cache.bytes written from cache",
        "serverStatus.wiredTiger.cache.tracked dirty bytes in the cache", //gauge
        "serverStatus.wiredTiger.cache.eviction state", //gauge
        "serverStatus.wiredTiger.transaction.transaction checkpoint generation", //boolean gauge
        "serverStatus.opLatencies.*.latency",
        "serverStatus.opLatencies.*.ops" };
    std::map<FTDCProcessId, FTDCMetricsSubset> fPmTs = ws.timeseries(keys, {testRangeS, testRangeE});
std::cout << fPmTs.size() << std::endl;
    for (auto& [pmId, m] : fPmTs) {
        std::cout << pmId.hostport << ": " << m.timespan().first << std::endl;
    }

    // Better test would include some metrics absent, some unknown keys, and a metric only appearing halfway through the 250s say some finegrained lock stat

    //For graphing lib purposes an output like this, in limited size, will be accepted I think
    //[ {"ts": x, "ops": y, "dirty_bytes": z, ...},
    //  {"ts": x, "ops": y, "dirty_bytes": z, ...},
    //  {"ts": x, "ops": y, "dirty_bytes": z, ...},
    //]

    exit(0);
}
