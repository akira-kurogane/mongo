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
        ("input-file", po::value<std::vector<std::string>>(), "path to FTDC directory (typically \"diagnostic.data/\") and/or individual metrics.YYYYMMDD... FTDC metric files. Can be multiple locations. (It's optional to write \"--input-file\", all positional args are used to be input directories or files.)")
        ;
    
    po::options_description filter_opts("Filter options");
    filter_opts.add_options()
        ("ts-start", po::value<std::string>(), "exclude results before this timestamp")
        ("ts-end",   po::value<std::string>(), "exclude results after this timestamp")
        ("metrics-filter", po::value<std::string>(), "file of metric names to include. (See output of --list-metrics for samples of format.)")
        ;
    
    po::options_description smyprint_optsd("Print summaries to stdout");
    smyprint_optsd.add_options()
        ("print-topology", po::value<bool>()->default_value(true), "Print replica sets, hosts, timespans. On by default.")
        ("print-metadata-doc", "Print last kMetadataDoc BSON object for each db instance.")
        ("list-metrics", "Print list of all metrics in these FTDC files")
        ;
        //plus a function to print reconstructed diagnosticData doc at any given second?

    po::options_description export_optsd("Export options");
    export_optsd.add_options()
        ("export-dir", po::value<std::string>()->default_value("."), "Directory to put exported timeseries metrics files in. Current working dir is default.")
        ("bson-timeseries", "Export timeseries as BSON file(s)")
        ("csv-timeseries", "Export timeseries as CSV file(s)")
        ("pandas-csv-timeseries", "Export timeseries as Pandas dataframe CSV format. A *.mapping.csv file is also saved alongside each data csv file.")
        ("vm-jsonlines-timeseries", "Export jsonlines file(s) that can be imported into VictoriaMetrics using the /api/v1/import endpoint.")
        ("resolution", po::value<float>()->default_value(1), "Resolution in seconds for timeseries output methods.")
        ;

    po::positional_options_description posarg_optsd;
    posarg_optsd.add("input-file", -1);
    
    // Declare an options description instance which will include
    // all the options
    po::options_description all_opts("Allowed options");
    all_opts.add(general).add(filter_opts).add(smyprint_optsd).add(export_optsd);
    
    po::variables_map vm;
    po::parsed_options parsed_opts = po::command_line_parser(argc, argv).options(all_opts).positional(posarg_optsd).run();
    po::store(parsed_opts, vm);
    //po::notify(vm); There are no notifier functions used yet

    //if (no output opts) { vm.emplace("print-topology", po::variable_value{}); }

    if (vm.count("help")) {
        std::cout << all_opts;
	_exit(0);
    }

    if (vm.count("input-file")) {
        auto ifv = vm["input-file"].as<std::vector<std::string>>();
        auto paItr = ifv.begin();
        while (paItr != ifv.end()) {
            if (!fs::exists(*paItr)) {
                std::cerr << "Error: there is no directory or file at " << (*paItr) << "\n";
                _exit(1);
            }
            input_fpaths.push_back(*paItr);
            paItr++;
        }
    }
    if (input_fpaths.size() == 0) {
        std::cerr << "Error: no input directories or files specified\n";
        std::cerr << "Usage: ftdc_dump <options> ftdc-dir-or-file-path [ftdc-dir-or-file-path]*\n";
        _exit(1);
    }

    std::vector<std::string> iso8601_optnms({ "ts-start", "ts-end" });
    for (auto const& optnm : iso8601_optnms) {
        if (vm.count(optnm)) {
            auto optval_str = vm[optnm].as<std::string>();
            StatusWith<Date_t> sWdt = dateFromISOString(optval_str);
            if (!sWdt.isOK()) {
                std::cerr << "Error: " << optnm << " option value \"" << optval_str << "\" could not be parsed as a date.\nAn ISO 8601 format is required. YYYY-MM-DDTHH:MM[:SS[.m[m[m]]]]Z.\nTimezone indicators -HHMM or +HHMM also accepted instead of the UTC indicator \"Z\".\n";
                _exit(1);
            }
        }
    }
    if (vm.count("ts-start") && vm.count("ts-end")) {
        auto ts_start_str = vm["ts-start"].as<std::string>();
        auto ts_end_str   = vm["ts-end"].as<std::string>();
        StatusWith<Date_t> sWdts = dateFromISOString(ts_start_str);
        StatusWith<Date_t> sWdte = dateFromISOString(ts_end_str);
        if (sWdts.getValue() >= sWdte.getValue()) {
            std::cerr << "Error: --ts-end option value " << ts_end_str << " is earlier or equal to the --ts-start option value " << ts_start_str << ". Exiting.\n";
            _exit(1);
        }
    }

    auto omc = vm.count("bson-timeseries") + vm.count("csv-timeseries") + vm.count("pandas-csv-timeseries") + vm.count("vm-jsonlines-timeseries");
    auto dp = fs::path(vm["export-dir"].as<std::string>());
    if (!vm["export-dir"].defaulted()) {
        if (!omc) {
            std::cerr << "Note: Ignoring \"--export-dir\" option because no timeseries format option, eg. --csv-timeseries, is selected.\n";
        }
        if (!fs::is_directory(dp)) {
            std::cerr << "--export-dir option value "  << dp << " is not a directory\n";
            _exit(1);
        }
    }
    if (omc && -1 == access(dp.c_str(), W_OK)) {
        std::cerr << "This process doesn't have write permisions for --export-dir " << dp << "\n";
        _exit(1);
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
        _exit(1);
    }

    auto tspan = ws.boundaryTimespan();

    if (vm["print-topology"].as<bool>() || vm.count("print-metadata-doc")) {
        std::cout << "Samples between " << dateToISOStringUTC(tspan.first) <<
                " - " << dateToISOStringUTC(tspan.last) << std::endl;
            
        auto tp = ws.topology();
        for (auto const& [rsnm, hpvals] : tp) {
            std::cout << (rsnm == "" ? "<no replsetname>" : rsnm) << std::endl;
            for (auto const& [hp, pmIds] : hpvals) {
                std::cout << "  " << hp << std::endl;
                for (auto const& pmId : pmIds) {
                    auto pm = ws.processMetrics(pmId);
                    std::cout << "    instance pid=" << pmId.pid << "\t";
                    std::cout << "    " << dateToISOStringUTC(pm.firstSampleTs()) <<
                            " - " << dateToISOStringUTC(pm.estimateLastSampleTs()) << std::endl;
                    if (vm.count("print-metadata-doc")) {
                        std::cout << pm.metadataDoc.jsonString(JsonStringFormat::Strict, 1) << "\n\n";
                    }
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

    Date_t ts_limit_start =  tspan.first;
    if (vm.count("ts-start")) {
        StatusWith<Date_t> sWdt = dateFromISOString(vm["ts-start"].as<std::string>()); //pre-validated in init_cmdline_opts()
        ts_limit_start = sWdt.getValue();
    }
    Date_t ts_limit_end = tspan.last;
    if (vm.count("ts-end")) {
        StatusWith<Date_t> sWdt = dateFromISOString(vm["ts-end"].as<std::string>()); //pre-validated in init_cmdline_opts()
        ts_limit_end = sWdt.getValue();
    }

    std::vector<std::string> ekl; //Extraction metric key list
    if (vm.count("metrics-filter")) {
        std::ifstream mif(vm["metrics-filter"].as<std::string>());
        if (!mif) {
            std::cerr << "Error: couldn't open --metrics-filter file " << vm["metrics-filter"].as<std::string>() << "\n";
            _exit(1);
        }
        std::string s;
        while (std::getline(mif, s)) {
            //if (s.trim() != "") {
                ekl.push_back(s);
            //}
        }
        if (ekl.size() == 0) {
            std::cerr << "The --metrics-filter file " << vm["metrics-filter"].as<std::string>() << " didn't have any lines in it. Exiting.\n";
            _exit(1);
        }
    } else {
        auto ks =  ws.keys();
        ekl.assign(ks.begin(), ks.end());
    }

    auto omc = vm.count("bson-timeseries") + vm.count("csv-timeseries") +
               vm.count("pandas-csv-timeseries") + vm.count("vm-jsonlines-timeseries");
    if (omc) {
        std::map<FTDCProcessId, FTDCMetricsSubset> fPmTs = ws.timeseries(ekl,
                        {ts_limit_start, ts_limit_end},
                        vm["resolution"].as<float>() * 1000/*ms*/); //TODO: add timeshift-hack arg
    
        if (!fPmTs.size()) {
            std::cout << "FTDCWorkspace::timeseries() returned an empty map (i.e. no results)\n";
        }

        auto odirpath = vm["export-dir"].as<std::string>();
        for (auto& [pmId, ms] : fPmTs) {
            //std::cout << "\nExporting timeseries for " << pmId.hostport << "(" << pmId.pid << "): " << ms.timespan().first << " - " << ms.timespan().last << std::endl;
            if (vm.count("bson-timeseries")) {
                auto b = ms.bsonMetrics();
                fs::path bfpath(odirpath + "/ftdc_timeseries." + pmId.hostport + ".pid" + std::to_string(pmId.pid) + ".bson");
                std::ofstream bf(bfpath.c_str());
                bf << b; //TODO: this is dumping as a string format :( supposed to be binary
                //iov.iov_len = b->len; iov.iov_base = (void *)bson_get_data(b); mongoc_stream_writev (stream, &iov, 1, 0);
                std::cout << "Created " << bfpath << ". Tip: you can view content with bsondump.\n";
                //std::cout << b.jsonString(JsonStringFormat::Strict, 1);
            }
            if (vm.count("csv-timeseries")) {
                ms.writeCSV(odirpath, pmId);
            }
            if (vm.count("pandas-csv-timeseries")) {
                ms.writePandasDataframeCSV(odirpath, pmId);
            }
            if (vm.count("vm-jsonlines-timeseries")) {
                auto tpl_lbls = ws.processMetrics(pmId).topologyLabels();
                //TODO: optionally fill tpl_lbls["cl_id"] if user provides one?
                ms.writeVMJsonLines(odirpath, pmId, tpl_lbls);
            }
    
        }
    }

    _exit(0);
}
