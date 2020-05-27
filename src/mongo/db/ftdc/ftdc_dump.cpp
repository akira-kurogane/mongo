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

    exit(0);
}
