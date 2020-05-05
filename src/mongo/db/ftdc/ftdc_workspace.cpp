#include "mongo/platform/basic.h"

#include <boost/filesystem.hpp>

#include "mongo/db/ftdc/ftdc_workspace.h"

#include "mongo/base/status_with.h"
#include "mongo/db/ftdc/file_reader.h"
#include "mongo/db/bson/dotted_path_support.h"

namespace dps = ::mongo::dotted_path_support;

namespace mongo {

namespace {

/**
 * Open a file with FTDCFileReader, read into as far as the initial metadata
 * packed doc. Extract some info and pass back in the topologyId ref parameter.
 */
bool confirmFTDCFile(const boost::filesystem::path& p, ::mongo::FTDCWorkspace::TopologyId& topologyId) {
    if (!boost::filesystem::is_regular_file(p) || boost::filesystem::file_size(p) == 0) {
        return false;
    }
    FTDCFileReader reader;
    auto s = reader.open(p);
    if (s != Status::OK()) {
        return false;
    }
    /**
     * Intuition break warning: FTDCFileReader::hasNext() advances the position, not next()
     */
    auto sw = reader.hasNext();
    if (!sw.isOK()) {
        return false;
    }
    auto ftdcType = std::get<0>(reader.next());
    if (ftdcType != FTDCBSONUtil::FTDCType::kMetadata) {
        std::cerr << "First packed doc in " << p.string() << " isn't a kMetadata doc" << std::endl;
        return false;
    }
    auto d = std::get<1>(reader.next()).getOwned();
    //auto ts = std::get<2>(reader.next()); //This is the metricChunk's first "start" ts only. Stays constant whilst the _pos in the concatenated arrays of metrics are iterated
    BSONElement hpElem = dps::extractElementAtPath(d, "hostInfo.system.hostname");
    if (hpElem.eoo()) {
        std::cerr << "No hostInfo.system.hostname element found in kMetadata doc" << std::endl;
        return false;
    } 
    topologyId.hostPort = hpElem.String();
    BSONElement rsnmElem = dps::extractElementAtPath(d, "getCmdLineOpts.parsed.replication.replSetName");
    if (rsnmElem.eoo()) {
         topologyId.rsName = "";
    } else {
         topologyId.rsName = rsnmElem.String();
    }
    return true;
}

} // namespace

FTDCWorkspace::FTDCWorkspace() {}

FTDCWorkspace::~FTDCWorkspace() {}

Status FTDCWorkspace::addFTDCFiles(std::vector<boost::filesystem::path> paths, bool recursive) {

    for (auto p = paths.begin(); p != paths.end(); ++p) {
        TopologyId tempTId = {"", ""};
        if (boost::filesystem::is_regular_file(*p)) {
            if (confirmFTDCFile(*p, tempTId)) {
                auto s = _addFTDCFilepath(*p, tempTId);
            }
        } else if (boost::filesystem::is_directory(*p)) {
            //if (recursive) { 
            //boost::filesystem::recursive_directory_iterator dItr(*p);
            boost::filesystem::directory_iterator dItr(*p);
            boost::filesystem::directory_iterator endItr;
            for (; dItr != endItr; ++dItr) {
                boost::filesystem::directory_entry& dEnt = *dItr;
                auto f = dEnt.path().filename();
                if (confirmFTDCFile(dEnt.path(), tempTId)) {
                     auto s = _addFTDCFilepath(dEnt.path(), tempTId);
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

Status FTDCWorkspace::_addFTDCFilepath(boost::filesystem::path p, TopologyId& topologyId) {

    _paths.insert(p);

    auto rsnm = topologyId.rsName;
    auto hostpost = topologyId.hostPort;
    auto hfl = _rs.find(rsnm);
    if (hfl == _rs.end()) {
	std::map<std::string, std::set<boost::filesystem::path>> x;
        _rs[rsnm] = x;
    }
    auto fl = _rs[rsnm].find(hostpost);
    if (fl == _rs[rsnm].end()) {
	std::set<boost::filesystem::path> x;
	_rs[rsnm][hostpost] = x;
    }
    _rs[rsnm][hostpost].insert(p);

    return Status::OK();
}

} // namespace mongo
