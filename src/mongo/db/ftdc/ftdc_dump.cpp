#include "mongo/platform/basic.h"

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>

#include "mongo/db/ftdc/ftdc_workspace.h"

namespace fs = boost::filesystem;
namespace po = boost::program_options;

using namespace mongo;

//TODO: push this options processing out to a separate file. Might be best to start again
//  with https://gist.github.com/ksimek/4a2814ba7d74f778bbee
po::variables_map init_cmdline_opts(int argc, char* argv[], std::vector<fs::path>& input_fpaths) {
    po::options_description general("General options");
    general.add_options()
        ("help", "produce a help message")
        ("input-file", po::value<std::vector<std::string>>(), "path to FTDC metrics file, or parent directory. Can be multiple locations. \"--input-file\" option flag is unnecessary.")
        ;
    
    po::options_description filter_opts("Filter options");
    filter_opts.add_options()
        ("hostport", po::value<std::string>(), "hostname:port filter")
        ("ts-start", po::value<std::string>(), "exclude results before this timestamp")
        ("ts-end",   po::value<std::string>(), "exclude results after this timestamp")
        ("metrics-filter", po::value<std::string>(), "file of metric names to include. (See output of --list-metrics for format.)")
        ;
    
    po::options_description output_opts("Output options");
    output_opts.add_options()
        ("print-topology", po::value<bool>()->default_value(true), "Print replica sets, hosts, timespans. Will be default if no other output option chosen.")
        ("list-metrics", "Print list of all metrics found to stdout")
        ("output-dir", po::value<std::string>()->default_value("/tmp"), "Directory to put timeseries metrics output files in")
        ("output-bson", "Output timeseries metrics in BSON format to directory")
        ("output-csv", "Output timeseries metrics in CSV format to directory")
        ("output-pandas-csv", "Output timeseries metrics in Pandas dataframe CSV format to directory. A *.mapping.csv alongside each data csv file is also output.")
        ("resolutionMs", po::value<unsigned int>()->default_value(1000), "Resolution in milliseconds for timeseries output methods.")
        ;

    po::positional_options_description posarg_optsd;
    posarg_optsd.add("input-file", -1);
    
    // Declare an options description instance which will include
    // all the options
    po::options_description all_opts("Allowed options");
    all_opts.add(general).add(filter_opts).add(output_opts);
    
    po::variables_map vm;
    po::parsed_options parsed_opts = po::command_line_parser(argc, argv).options(all_opts).positional(posarg_optsd).run();
    po::store(parsed_opts, vm);
    //po::notify(vm); There are no notifier functions used yet

    //if (no output opts) { vm.emplace("print-topology", po::variable_value{}); }

    if (vm.count("help")) {
        std::cout << all_opts;
        exit(0);
    }

    //auto pa = po::collect_unrecognized(parsed_opts.options, po::include_positional);
    //if (pa.size()) {
    //    pa.erase(pa.begin()); //delete the exe name at position [0] in this vector
    //}

    if (vm.count("input-file")) {
        auto ifv = vm["input-file"].as<std::vector<std::string>>();
        auto paItr = ifv.begin();
        while (paItr != ifv.end()) {
            if (!fs::exists(*paItr)) {
                std::cerr << "Error: there is no directory or file at " << (*paItr) << "\n";
                exit(1);
            }
            input_fpaths.push_back(*paItr);
            paItr++;
        }
    }
    if (input_fpaths.size() == 0) {
        std::cerr << "Error: no input directories or files specified\n";
        std::cerr << "Usage: ftdc_dump <options> ftdc-dir-or-file-path [ftdc-dir-or-file-path]*\n";
        exit(1);
    }

    auto omc = vm.count("output-bson") + vm.count("output-csv") + vm.count("output-pandas-csv");
    if (!vm["output-dir"].defaulted()) {
        auto dp = fs::path(vm["output-dir"].as<std::string>());
        if (!omc && dp != "/tmp"/*default*/) {
            std::cerr << "Note: Ignoring \"--output-dir\" option because no timeseries format option, eg. --output-bson, is selected.\n";
        }
        if (!fs::is_directory(dp)) {
            std::cerr << "--output-dir option value "  << dp << " is not a directory\n";
            exit(1);
        }
        if (-1 == access(dp.c_str(), W_OK)) {
            std::cerr << "This process doesn't have write permisions for --output-dir "  << dp << "\n";
            exit(1);
        }
    }

    return vm;

}

int main(int argc, char* argv[], char** envp) {

    //Parse cmdline options. Will exit on --help or option error
    std::vector<fs::path> input_fpaths;
    auto vm = init_cmdline_opts(argc, argv, input_fpaths);
    
    FTDCWorkspace ws;
    Status s = ws.addFTDCFiles(input_fpaths); //TODO add --ts-start/end and --hostport for filtering

    auto fp = ws.filePaths();
    if (fp.size() == 0) {
        std::cerr << "There are no FTDC metrics files at " << input_fpaths[0];
        size_t i;
        for (i = 1; i < input_fpaths.size(); ++i) {
            std::cerr << ", " << input_fpaths[i];
        }
        std::cerr << std::endl;
        exit(1);
    }

    auto tspan = ws.boundaryTimespan();

    if (vm["print-topology"].as<bool>()) {
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
    }

    if (vm.count("list-metrics")) {
        auto ks =  ws.keys();
        auto mitr = ks.begin();
        while (mitr != ks.end()) {
            std::cout << "  " << *mitr << "\n";
            mitr++;
        }
    }

    Date_t testRangeS =  tspan.first + ((tspan.last - tspan.first) * 4) / 10;
    Date_t testRangeE = testRangeS + Seconds(86400);
    //vm.emplace("resolutionMs", po::variable_value{1000, false});
    //Date_t testRangeS =  tspan.first + ((tspan.last - tspan.first) * 1) / 10;
    //Date_t testRangeE = tspan.last - ((tspan.last - tspan.first) * 2) / 10;
    //vm.emplace("resolutionMs", po::variable_value{ (tspan.last.toMillisSinceEpoch() - tspan.first.toMillisSinceEpoch()) / 30, false);
    //Date_t testRangeS =  tspan.first;
    //Date_t testRangeE = tspan.last;
    //vm.emplace("resolutionMs", po::variable_value{60000, false});

    //TODO if (opts.metricsFilter() ..)
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
        "serverStatus.opLatencies.writes.latency",
        "serverStatus.opLatencies.writes.ops",
        "serverStatus.opcounters.command",
        "serverStatus.opcounters.delete",
        "serverStatus.opcounters.getmore",
        "serverStatus.opcounters.insert",
        "serverStatus.opcounters.query",
        "serverStatus.opcounters.update",
        "serverStatus.shardingStatistics.totalDonorChunkCloneTimeMillis",
    };
    //ks =  ws.keys();
    //std::vector<std::string> keys(ks.begin(), ks.end());

    auto omc = vm.count("output-bson") + vm.count("output-csv") + vm.count("output-pandas-csv");
    if (omc) {
        std::map<FTDCProcessId, FTDCMetricsSubset> fPmTs = ws.timeseries(keys, {testRangeS, testRangeE}, vm["resolutionMs"].as<unsigned int>());
    
        if (!fPmTs.size()) {
            std::cout << "FTDCWorkspace::timeseries() returned an empty map (i.e. no results)\n";
        }

        auto odirpath = vm["output-dir"].as<std::string>();
        for (auto& [pmId, ms] : fPmTs) {
            std::cout << "\n" << pmId.hostport << "(" << pmId.pid << "): " << ms.timespan().first << " - " << ms.timespan().last << std::endl;
            if (vm.count("output-bson")) {
                auto b = ms.bsonMetrics();
                fs::path bfpath(odirpath + "/ftdc_timeseries." + pmId.hostport + ".pid" + std::to_string(pmId.pid) + ".bson");
                std::ofstream bf(bfpath, std::ios::out);
                bf << b; //TODO: this is dumping as a string format :( need to change to binary
                std::cout << "Created " << bfpath << ". Tip: you can view content with bsondump.\n";
                //std::cout << b.jsonString(JsonStringFormat::Strict, 1);
            }
            if (vm.count("output-csv")) {
                ms.writeCSV(odirpath, pmId);
            }
            if (vm.count("output-pandas-csv")) {
                ms.writePandasDataframeCSV(odirpath, pmId);
            }
    
        }
    }

    exit(0);
}
