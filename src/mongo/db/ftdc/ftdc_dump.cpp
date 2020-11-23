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

    std::cout << "Ten random keys in this workspace:\n";
    auto ks =  ws.keys();
    for (size_t i = 0; i < 10; i++) {
        auto mitr = ks.begin();
        std::advance(mitr, rand() % ks.size());
        std::cout << "  " << *mitr << "\n";
    }

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

    //Date_t testRangeS =  tspan.first + ((tspan.last - tspan.first) * 4) / 10;
    //b
    //Date_t testRangeE = testRangeS + Seconds(250);
    //uint32_t testStepMs = 10000;
    //Date_t testRangeS =  tspan.first + ((tspan.last - tspan.first) * 1) / 10;
    //Date_t testRangeE = tspan.last - ((tspan.last - tspan.first) * 2) / 10;
    //uint32_t testStepMs = (tspan.last.toMillisSinceEpoch() - tspan.first.toMillisSinceEpoch()) / 30;
    Date_t testRangeS =  tspan.first;
    Date_t testRangeE = tspan.first + Seconds(360);
    uint32_t testStepMs = 10000;

    std::vector<std::string> keys = {
        /*test: leaving "start" blank, should be forcefully added*/
        "serverStatus.connections.current",
        "serverStatus.connections.available",
        "serverStatus.tcmalloc.tcmalloc.total_free_bytes",
        "serverStatus.wiredTiger.cache.bytes read into cache",
        "serverStatus.wiredTiger.cache.bytes written from cache",
        "serverStatus.wiredTiger.cache.tracked dirty bytes in the cache", //gauge
        "serverStatus.wiredTiger.cache.eviction state", //gauge
        "serverStatus.wiredTiger.transaction.transaction checkpoint currently running", //boolean gauge
        "serverStatus.wiredTiger.transaction.transaction checkpoint generation",
        "serverStatus.repl.lastWrite.opTime.ts", 
        "serverStatus.opLatencies.reads.latency",
        "serverStatus.opLatencies.reads.ops",
        "serverStatus.shardingStatistics.totalDonorChunkCloneTimeMillis",
    };
    //ks =  ws.keys();
    //std::vector<std::string> keys(ks.begin(), ks.end());

    std::map<FTDCProcessId, FTDCMetricsSubset> fPmTs = ws.timeseries(keys, {testRangeS, testRangeE}, testStepMs);

    if (!fPmTs.size()) {
        std::cout << "FTDCWorkspace::timeseries() returned an empty map (i.e. no results)\n";
    }
    for (auto& [pmId, ms] : fPmTs) {
        std::cout << "\n" << pmId.hostport << "(" << pmId.pid << "): " << ms.timespan().first << " - " << ms.timespan().last << std::endl;
        //auto b = ms.bsonMetrics();
        //std::cout << b.jsonString(JsonStringFormat::Strict, 1);
	ms.writePandasDataframeCSV("/tmp/junk.csv");

    }

    // Better test would include some metrics absent, some unknown keys, and a metric only appearing halfway through the 250s say some finegrained lock stat

    //For graphing lib purposes an output like this, in limited size, will be accepted I think
    //[ {"ts": x, "ops": y, "dirty_bytes": z, ...},
    //  {"ts": x, "ops": y, "dirty_bytes": z, ...},
    //  {"ts": x, "ops": y, "dirty_bytes": z, ...},
    //]

    exit(0);
}
