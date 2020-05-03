#include "mongo/platform/basic.h"

#include <boost/filesystem.hpp>

#include "mongo/db/ftdc/file_reader.h"

namespace mongo {

bool dumpRandom10(const boost::filesystem::path& p) {
    FTDCFileReader reader;

    auto s = reader.open(p);
    if (s != Status::OK()) {
        std::cerr << "Failed to open file " << p.string() << std::endl;
        return false;
    }

    std::vector<BSONObj> list;
    auto sw = reader.hasNext();
    
    unsigned int ctr = 0;
    while (sw.isOK() && sw.getValue() && ctr++ < 10) {
        auto d = std::get<1>(reader.next()).getOwned();
	std::cout << d << std::endl;
        sw = reader.hasNext();
    }

    return true;
}

}  // namespace mongo

int main(int argc, char* argv[], char** envp) {
    if (argc < 2) {
        std::cerr << "usage: " << argv[0] << " <ftdc-file-path>" << std::endl;
        exit(1);
    }
    boost::filesystem::path p(argv[1]);
    auto exitStatus = mongo::dumpRandom10(p);
    exit(exitStatus ? 0 : 1);
}
