#include "mongo/platform/basic.h"

#include <boost/filesystem.hpp>

#include "mongo/db/ftdc/ftdc_workspace.h"

#include "mongo/db/ftdc/file_reader.h"
#include "mongo/db/jsobj.h"

namespace mongo {

namespace {

bool isFTDCFile(const boost::filesystem::path& p) {
    if (!boost::filesystem::is_regular_file(p) || boost::filesystem::file_size(p) == 0) {
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

std::tuple<std::string, std::string> extractNodeTopologyId(const boost::filesystem::path& p) {
    // TODO. Change to StatusWith; open file, read rs name (use "" if none) and
    // host:port from Metadata doc. Or add this into isFTDCFile as ref parms
    return std::tuple<std::string, std::string>("", "");
}

} // namespace

FTDCWorkspace::FTDCWorkspace() {}

FTDCWorkspace::~FTDCWorkspace() {}

Status FTDCWorkspace::addFTDCFiles(std::vector<boost::filesystem::path> paths, bool recursive) {

    for (auto p = paths.begin(); p != paths.end(); ++p) {
        if (boost::filesystem::is_regular_file(*p)) {
            if (isFTDCFile(*p)) {
                auto s = _addFTDCFilepath(*p);
            }
        } else if (boost::filesystem::is_directory(*p)) {
            //if (recursive) { 
            //boost::filesystem::recursive_directory_iterator dItr(*p);
            boost::filesystem::directory_iterator dItr(*p);
            boost::filesystem::directory_iterator endItr;
            for (; dItr != endItr; ++dItr) {
                boost::filesystem::directory_entry& dEnt = *dItr;
                auto f = dEnt.path().filename();
                if (isFTDCFile(dEnt.path())) {
                     auto s = _addFTDCFilepath(dEnt.path());
                }
            }
        }
    }

    return Status::OK();
}

std::set<boost::filesystem::path> FTDCWorkspace::filePaths() {
    return _paths;
}

std::map<std::string, std::map<std::string, std::set<boost::filesystem::path>>>
FTDCWorkspace::topology() {
    return _rs;
}

void FTDCWorkspace::clear() {
    _rs.clear();
    _paths.clear();
}

Status FTDCWorkspace::_addFTDCFilepath(boost::filesystem::path p) {

    _paths.insert(p);

    auto ntid = extractNodeTopologyId(p);
    auto rsnm = std::get<0>(ntid);
    auto hostnm = std::get<1>(ntid);

    auto hfl = _rs.find(rsnm);
    if (hfl == _rs.end()) {
	std::map<std::string, std::set<boost::filesystem::path>> x;
        _rs[rsnm] = x;
    }
    auto fl = _rs[rsnm].find(hostnm);
    if (fl == _rs[rsnm].end()) {
	std::set<boost::filesystem::path> x;
	_rs[rsnm][hostnm] = x;
    }
    _rs[rsnm][hostnm].insert(p);

    return Status::OK();
}

} // namespace mongo
