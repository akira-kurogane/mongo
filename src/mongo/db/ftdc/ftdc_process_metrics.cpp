#include <pcrecpp.h>
#include "mongo/db/ftdc/ftdc_process_metrics.h"
#include "mongo/db/ftdc/file_reader.h"

#include "mongo/db/bson/dotted_path_support.h"

namespace dps = ::mongo::dotted_path_support;
namespace fs = boost::filesystem;

namespace mongo {

bool operator==(const FTDCProcessId& l, const FTDCProcessId& r) {
    return l.hostport == r.hostport && l.pid == r.pid;
}
bool operator!=(const FTDCProcessId& l, const FTDCProcessId& r) {
    return !(l==r);
}

bool operator<(const FTDCProcessId& l, const FTDCProcessId& r) {
    return l.hostport < r.hostport || (l.hostport == r.hostport && l.pid < r.pid);
}

bool FTDCPMTimespan::isValid() {
    return last > first;
}

bool FTDCPMTimespan::overlaps(FTDCPMTimespan& other) {
    return isValid() && other.isValid() &&
        ((first >= other.first && first < other.last) ||
        (last > other.first && last <= other.last) ||
        (first < other.first && last > other.last) || 
        (other.first < first && other.last > last));
}

/**
 * This will return an invalid timespan if the two timespans don't overlap.
 * Test with isValid() if not confirming overlaps() is true before using.
 */
FTDCPMTimespan FTDCPMTimespan::intersection(FTDCPMTimespan& other) {
    return { first >= other.first ? first : other.first,
             last < other.last ? last : other.last };
}

FTDCMetricsSubset::FTDCMetricsSubset(std::vector<std::string> keys, FTDCPMTimespan tspan,
                            uint32_t sampleResolution) {
    /**
     * Add "start" metric (i.e. timestamp of samples) if absent.
     */
    if (std::find(keys.begin(), keys.end(), "start") == keys.end()) {
        keys.emplace_back("start");
    }
    /**
     * Force "start" to be the first key to make it simple to find.
     */
    auto startKeyItr = std::find(keys.begin(), keys.end(), "start");
    while (startKeyItr != keys.begin()) {
        *startKeyItr-- = *(startKeyItr - 1);
    }
    keys[0] = "start";

    _kNT.reserve(keys.size());
    size_t keyRowCtr = 0;

    for (auto k : keys) {
        FTDCMSKeyNameType y{k, BSONType::Undefined};
        _kNT.emplace_back(y);
        _keyRows[k] = keyRowCtr++;
    }

    _tspan = tspan;
    _stepMs = sampleResolution;

    _rowLength = (tspan.last.toMillisSinceEpoch() - tspan.first.toMillisSinceEpoch()) / _stepMs;
    /**
     * If a partial span exists on the end add that too. E.g. if _stepMs is
     * 10000 and the sample is at x mins, 34 secs then the last column will be
     * for x min 31s to 40s but the last "start" ts value in it will be 34s. It
     * will follow the second-to-last whole step for x min 21s to 30s which
     * (probably) does have a last sample at x min 30s.
     */
    if ((tspan.last.toMillisSinceEpoch() - tspan.first.toMillisSinceEpoch()) % _stepMs) {
        _rowLength += 1;
    }

    metrics.resize(_rowLength * _kNT.size(), 7777777777); //Constant value being used to indicate unset
}

BSONObj FTDCMetricsSubset::bsonMetrics() {
   invariant(_rowLength * _kNT.size() == metrics.size());
   auto mPtr = metrics.begin();
   BSONObjBuilder builder;
   for (auto x : _kNT) {
        //current metric row's array builder
        BSONObjBuilder ab(builder.subarrayStart(x.keyName));
        switch (x.bsonType) {
            case NumberDouble:
            case NumberInt:
            case NumberLong:
                for (size_t i = 0; i < _rowLength; ++i) {
                   ab.append(ab.numStr(i), static_cast<long long int>(*mPtr++));
                }
                break;

            case Bool:
                for (size_t i = 0; i < _rowLength; ++i) {
                   ab.append(ab.numStr(i), static_cast<bool>(*mPtr++));
                }
                break;

            case Date:
                for (size_t i = 0; i < _rowLength; ++i) {
                   ab.append(ab.numStr(i), Date_t::fromMillisSinceEpoch(static_cast<std::uint64_t>(*mPtr++)));
                }
                break;

            case bsonTimestamp: {
                //TODO: if we want the original increment part as well then 
                // _flattenedBSONDoc() should add it somehow. Eg. as keyName + ".i" of this field.
                for (size_t i = 0; i < _rowLength; ++i) {
                    ab.append(ab.numStr(i), Timestamp(static_cast<long long int>(*mPtr++), 0/*dummy increment field*/));
                }
                break;
            }

            case Undefined:
                for (size_t i = 0; i < _rowLength; ++i) {
                   ab.appendUndefined(ab.numStr(i));
                }
                break;

            default:
                MONGO_UNREACHABLE;
                break;
        }
        ab.done();
    }
    return builder.obj();
}

void FTDCMetricsSubset::writeCSV(boost::filesystem::path dirfp, FTDCProcessId pmId) {
    fs::path csvfpath(dirfp.string() + "/ftdc_metrics." + pmId.hostport + ".pid" + std::to_string(pmId.pid) + ".csv");
    std::ofstream cf(csvfpath.c_str());

    auto mPtr = metrics.begin();
    for (auto x : _kNT) {
        cf << "\"" << x.keyName;
        switch (x.bsonType) {
            case NumberDouble:
            case NumberInt:
            case NumberLong:
                for (size_t i = 0; i < _rowLength; ++i) {
                   cf << "," << std::to_string(static_cast<long long int>(*mPtr++));
                }
                break;

            case Bool:
                for (size_t i = 0; i < _rowLength; ++i) {
                   bool x = *mPtr++;
                   cf << "," << (x ? "true" : "false");
                }
                break;

            case Date:
                for (size_t i = 0; i < _rowLength; ++i) {
                   cf << ",\"" << dateToISOStringUTC(Date_t::fromMillisSinceEpoch(static_cast<std::uint64_t>(*mPtr++)));
                }
                break;

            case bsonTimestamp: {
                for (size_t i = 0; i < _rowLength; ++i) {
                    //TODO: maybe Date_t::fromSecondsSinceEpoch(*mPtr++) is better. I.e. make it the same as for Date as this is CSV and BSON format won't be reconstructured from it
                    cf << "," << std::to_string(static_cast<long long int>(*mPtr++));
                }
                break;
            }

            case Undefined:
                for (size_t i = 0; i < _rowLength; ++i) {
                   cf << ",undefined";
                }
                break;

            default:
                MONGO_UNREACHABLE;
                break;
        }
        cf << "\n";
    }
    std::cout << csvfpath << " created. It contains a matrix of " << _kNT.size() << " metrics by " << _rowLength << " time samples.\n";
}

void FTDCMetricsSubset::writePandasDataframeCSV(boost::filesystem::path dirfp, FTDCProcessId pmId) {
    fs::path data_fpath(dirfp.string() + "/pandas_dataframe." + pmId.hostport + ".pid" + std::to_string(pmId.pid) + ".csv");
    fs::path mpf_fpath = data_fpath;
    mpf_fpath.replace_extension(".mapping.csv");

    auto b = bsonMetrics();

    invariant(_rowLength * _kNT.size() == metrics.size());
    std::ofstream mpf(mpf_fpath.c_str());
    //TO DELETE std::vector<FTDCMSKeyNameType> ktv = keyNamesAndType(); //this is _kNT ... maybe I should have just referenced it directly
    for (size_t c = 0; c < _kNT.size(); ++c) {
        mpf << "\"" << _kNT[c].keyName << "\",";
    }
    mpf << "\n";
    for (size_t c = 0; c < _kNT.size(); ++c) {
        mpf << typeName(_kNT[c].bsonType) << ",";
    }
    mpf << "\n";

    std::ofstream cf(data_fpath.c_str());
    for (size_t c = 0; c < _kNT.size(); ++c) {
        cf << "\"" << _kNT[c].keyName << "\",";
    }
    cf << "\n";
    for (size_t rowPos = 0; rowPos < _rowLength; ++rowPos) {
        for (size_t c = 0; c < _kNT.size(); ++c) {
            auto bt = _kNT[c].bsonType;
            auto val = metrics[c * _rowLength + rowPos];
            switch (bt) {
                case NumberDouble:
                case NumberInt:
                case NumberLong:
                    cf << std::to_string(val) << ",";
                    break;

                case Bool:
                    cf << (val ? "true" : "false") << ",";
                    break;

                case Date:
                    cf << "\"" << dateToISOStringUTC(Date_t::fromMillisSinceEpoch(val)) << "\",";
                    break;

                case bsonTimestamp: {
                    //TODO: maybe Date_t::fromSecondsSinceEpoch(val) is better. I.e. make it the same as for Date as this is CSV and BSON format won't be reconstructured from it
                    cf << std::to_string(val) << ",";
                    break;
                }

                case Undefined:
                    cf << "undefined,";
                    break;

                default:
                    MONGO_UNREACHABLE;
                    break;
            }
            cf << "\n";
        }
    }

    std::cout << "Created " << data_fpath << " and matching *.mapping.csv file. Contains " << _rowLength << " samples of " << _kNT.size() << " metrics at " << (_stepMs/1000) << "s period.\n";
}

/**
 * This function replicates the metric renaming done in the
 * prometheusize() function in exporter/metrics.go in the
 * "v0.20.0" branch of https://github.com/percona/mongodb_exporter/
 *
 * Eg. "serverStatus.connections.current" ->
 *       "mongodb_ss_connections_current"
 *     "replSetGetStatus.members.1.opTime" ->
 *       "mongodb_rs_members_1_opTime"
 *
 * Note that it only rewrites the metric name, and some metrics have 
 * further processing that remove a part and replace it with a label.
 * Eg. mongodb_rs_members_1_opTime ->
 *       mongodb_rs_members_opTime{rs_mbr="hostA:portY"}
 *     mongodb_ss_opLatencies_ops ->
 *       xx
 *
 * The golang code being referenced as of Dec 2019 is below:
 * -----
 * prefixes = [][]string{
 *              {"serverStatus.wiredTiger.transaction", "ss_wt_txn"},
 *              {"serverStatus.wiredTiger", "ss_wt"},
 *              {"serverStatus", "ss"},
 *              {"replSetGetStatus", "rs"},
 *              {"systemMetrics", "sys"},
 *              {"local.oplog.rs.stats.wiredTiger", "oplog_stats_wt"},
 *              {"local.oplog.rs.stats", "oplog_stats"},
 *              {"collstats_storage.wiredTiger", "collstats_storage_wt"},
 *              {"collstats_storage.indexDetails", "collstats_storage_idx"},
 *              {"collStats.storageStats", "collstats_storage"},
 *              {"collStats.latencyStats", "collstats_latency"},
 *      }
 *      ...
 *      ...
 *      // Regular expressions used to make the metric name Prometheus-compatible
 *      // This variables are global to compile the regexps only once.
 *      specialCharsRe        = regexp.MustCompile(`[^a-zA-Z0-9_]+`)
 *      repeatedUnderscoresRe = regexp.MustCompile(`__+`)
 *      dollarRe              = regexp.MustCompile(`\_$`)
 */
std::string prometheusize(std::string s) {

    pcrecpp::RE("^serverStatus.wiredTiger.transaction").Replace("ss_wt_txn", &s);
    pcrecpp::RE("^serverStatus.wiredTiger").Replace("ss_wt", &s);
    pcrecpp::RE("^serverStatus").Replace("ss", &s);
    pcrecpp::RE("^replSetGetStatus").Replace("rs", &s);
    pcrecpp::RE("^systemMetrics").Replace("sys", &s);
    pcrecpp::RE("^local.oplog.rs.stats.wiredTiger").Replace("oplog_stats_wt", &s);
    pcrecpp::RE("^local.oplog.rs.stats").Replace("oplog_stats", &s);
    pcrecpp::RE("^collstats_storage.wiredTiger").Replace("collstats_storage_wt", &s);
    pcrecpp::RE("^collstats_storage.indexDetails").Replace("collstats_storage_idx", &s);
    pcrecpp::RE("^collStats.storageStats").Replace("collstats_storage", &s);
    pcrecpp::RE("^collStats.latencyStats").Replace("collstats_latency", &s);

    pcrecpp::RE("[^a-zA-Z0-9_]+").GlobalReplace("_", &s);
    pcrecpp::RE("__+").GlobalReplace("_", &s);
    pcrecpp::RE("\\_$").GlobalReplace("", &s);

    return "mongodb_" + s;
}

/**
 * Change some metrics by removing substrings of the name and assinging in extra labels
 * This meets the requirement of https://jira.percona.com/browse/PMM-6506,
 * which is achieved with the function specialConversions() in exporter/metrics.go
 *
 *         nodeToPDMetrics = map[string]string{
 *              "collStats.storageStats.indexDetails.":            "index_name",
 *              "globalLock.activeQueue.":                         "count_type",
 *              "globalLock.locks.":                               "lock_type",
 *              "serverStatus.asserts.":                           "assert_type",
 *              "serverStatus.connections.":                       "conn_type",
 *              "serverStatus.globalLock.currentQueue.":           "count_type",
 *              "serverStatus.metrics.commands.":                  "cmd_name",
 *              "serverStatus.metrics.cursor.open.":               "csr_type",
 *              "serverStatus.metrics.document.":                  "doc_op_type",
 *              "serverStatus.opLatencies.":                       "op_type",
 *              "serverStatus.opReadConcernCounters.":             "concern_type",
 *              "serverStatus.opcounters.":                        "legacy_op_type",
 *              "serverStatus.opcountersRepl.":                    "legacy_op_type",
 *              "serverStatus.transactions.commitTypes.":          "commit_type",
 *              "serverStatus.wiredTiger.concurrentTransactions.": "txn_rw_type",
 *              "serverStatus.wiredTiger.perf.":                   "perf_bucket",
 *              "systemMetrics.disks.":                            "device_name",
 *      }
 * Extra-fiddly ones that labelize a node not at the lowest level.
 * mongodb_ss_opLatencies_(commands|writes|reads)_(ops|latency){...}
 * mongodb_ss_opLatencies_\2{...,"op_type":"\1"}
 *
 * mongodb_ss_wt_concurrentTransactions_(available|out|totalTickets){...}
 * mongodb_ss_wt_concurrentTransactions{...,"txn_rw":"\1"}
 */

pcrecpp::RE oplatencies_re("^(mongodb_ss_opLatencies)_(commands|writes|reads)_(ops|latency)");
pcrecpp::RE wt_conctx_re("^(mongodb_ss_wt_concurrentTransactions)_(available|out|totalTickets)");

std::vector<std::tuple<pcrecpp::RE, std::string>> last_node_relabels = {
    {pcrecpp::RE("^(mongodb_collstats_storage_idx)_(.+)"),         "index_name"}, 
    {pcrecpp::RE("^(mongodb_globalLock_activeQueue)_(.+)"),        "count_type"},
    {pcrecpp::RE("^(mongodb_globalLock_locks)_(.+)"),              "lock_type"},
    {pcrecpp::RE("^(mongodb_ss_asserts)_(.+)"),                    "assert_type"},
    {pcrecpp::RE("^(mongodb_ss_connections)_(.+)"),                "conn_type"},
    {pcrecpp::RE("^(mongodb_ss_globalLock_currentQueue)_(.+)"),    "count_type"},
    {pcrecpp::RE("^(mongodb_ss_metrics_commands)_(.+)"),           "cmd_name"},
    {pcrecpp::RE("^(mongodb_ss_metrics_cursor_open)_(.+)"),        "csr_type"},
    {pcrecpp::RE("^(mongodb_ss_metrics_document)_(.+)"),           "doc_op_type"},
    {pcrecpp::RE("^(mongodb_ss_opReadConcernCounters)_(.+)"),      "concern_type"},
    {pcrecpp::RE("^(mongodb_ss_opcounters)_(.+)"),                 "legacy_op_type"},
    {pcrecpp::RE("^(mongodb_ss_opcountersRepl)_(.+)"),             "legacy_op_type"},
    {pcrecpp::RE("^(mongodb_ss_transactions_commitTypes)_(.+)"),   "commit_type"},
    {pcrecpp::RE("^(mongodb_ss_wt_perf)_(.+)"),                    "perf_bucket"},
    {pcrecpp::RE("^(mongodb_sys_disks)_(.+)"),                     "device_name"}
};

std::tuple<std::string, std::map<std::string, std::string>> specialLabels(std::string m_name) {
    std::map<std::string, std::string> l;
    std::string n = m_name;
    std::string ms1, ms2, ms3;

    if (oplatencies_re.FullMatch(n, &ms1, &ms2, &ms3)) {
        n = ms1 + "_" + ms3;
        l["op_type"] = ms2;
    }
    if (wt_conctx_re.FullMatch(n, &ms1, &ms2)) {
        n = ms1;
        l["txn_rw"] = ms2;
    }

    for (auto regnl : last_node_relabels) {
        auto [ re, nl ] = regnl;
        if (re.FullMatch(n, &ms1, &ms2)) {
            n = ms1;
            l[nl] = ms2;
            continue;
        }
    }

    return {n, l};
}

void FTDCMetricsSubset::writeVMJsonLines(boost::filesystem::path dirfp,
                FTDCProcessId pmId, std::map<std::string, std::string> topologyLabels) {
    fs::path jfpath(dirfp.string() + "/ftdc_metrics." + pmId.hostport + ".pid" + std::to_string(pmId.pid) + ".victoriametrics.jsonlines");
    std::ofstream jf(jfpath.c_str());

    invariant(_rowLength * _kNT.size() == metrics.size());

    auto start_ts_v = metricsRow("start");
    invariant(start_ts_v.size());
    auto rs_state_v = metricsRow("replSetGetStatus.myState");
    invariant(rs_state_v.size());

    auto rs_ptr = metrics.begin(); //Row start pointer
    std::string pm_constant_lbls = ",\"job\":\"mongodb\",\"instance\":\"" + pmId.hostport + "\"";
    for (auto [k, v] : topologyLabels) {
        pm_constant_lbls += ",\"" + k + "\":\"" + v + "\"";
    }

    //TODO: iterate by ranges of constant rs_state
    //std::vector<std::tuple<size_t, size_t>>
    uint64_t rs_state = 1;
    for (auto x : _kNT) {
        if (x.keyName != "start") { //we don't output the "start" timestamp as its own timeseries

            auto prom_m_name = prometheusize(x.keyName);
            auto [ m_name, xl ] = specialLabels(prom_m_name);
            jf << "{\"metric\":{\"__name__\":\"" << m_name << "\"" <<
                    pm_constant_lbls << ",\"rs_state\":\"" << rs_state << "\"";
            for (auto [ k, v ] : xl) { //Add extra labels if this metric has them
                jf << ",\"" << k << "\":\"" << v << "\"";
            }
            jf << "}";

            std::stringstream vals_ss;
            vals_ss << "[";
            std::stringstream ts_ss;
            ts_ss << "[";

            switch (x.bsonType) {
                case NumberDouble:
                case NumberInt:
                case NumberLong:
                    for (size_t i = 0; i < _rowLength; ++i) {
                       if (start_ts_v[i] != 7777777777 && rs_ptr[i] != 7777777777) {
                           ts_ss << start_ts_v[i] << ",";
                           vals_ss << std::to_string(static_cast<long long int>(rs_ptr[i])) << ",";
                       }
                    }
                    break;

                case Bool:
                    for (size_t i = 0; i < _rowLength; ++i) {
                       if (start_ts_v[i] != 7777777777 && rs_ptr[i] != 7777777777) {
                           ts_ss << start_ts_v[i] << ",";
                           bool x = rs_ptr[i];
                           vals_ss << (x ? "true" : "false") << ","; //TODO confirm if this is the right datatype representation for VM
                       }
                    }
                    break;

                case Date:
                    for (size_t i = 0; i < _rowLength; ++i) {
                       if (start_ts_v[i] != 7777777777 && rs_ptr[i] != 7777777777) {
                           ts_ss << start_ts_v[i] << ",";
                           vals_ss << "\"" << dateToISOStringUTC(Date_t::fromMillisSinceEpoch(static_cast<std::uint64_t>(rs_ptr[i]))) << ","; //TODO change to VM datatype
                       }
                    }
                    break;

                case bsonTimestamp: {
                    for (size_t i = 0; i < _rowLength; ++i) {
                       if (start_ts_v[i] != 7777777777 && rs_ptr[i] != 7777777777) {
                           ts_ss << start_ts_v[i] << ",";
                           //TODO: maybe Date_t::fromSecondsSinceEpoch(*rs_ptr++) is better. I.e. make it the same as for Date as this is CSV and BSON format won't be reconstructured from it
                           vals_ss << std::to_string(static_cast<long long int>(rs_ptr[i])) << ","; //TODO change to VM datatype
                       }
                    }
                    break;
                }

                case Undefined:
                    //do no output
                    break;

                default:
                    MONGO_UNREACHABLE;
                    break;
            }

            if (ts_ss.str().size() > 0) {
                vals_ss.seekp(-1, vals_ss.cur); //overwrite last "," with array end "]";
                vals_ss << "]";
                ts_ss.seekp(-1, ts_ss.cur); //overwrite last "," with array end "]";
                ts_ss << "]";
                jf << ",\"values\":"     << vals_ss.str();
                jf << ",\"timestamps\":" << ts_ss.str();
            }
else { std::cerr << "DEBUG: " << x.keyName << " had only null values in its timeseries metrics in this sample period.\n"; }

            jf << "}\n";

        } //End: if (x.keyName != "start")

        rs_ptr += _rowLength;
    }

    std::cout << "Created " << jfpath << " Contains " << _rowLength << " samples of " << _kNT.size() << " metrics at " << (_stepMs/1000) << "s period.\n";
}

std::string FTDCProcessMetrics::rsName() const {
    BSONElement rsnmElem = dps::extractElementAtPath(metadataDoc, "getCmdLineOpts.parsed.replication.replSetName");
    if (rsnmElem.eoo()) {
         return "";
    } else {
         return rsnmElem.String();
    }
}

std::string FTDCProcessMetrics::clusterRole() const {
    //If the sharding.configDB config option is present that indicates this is a mongos node
    BSONElement scfgdbElem = dps::extractElementAtPath(metadataDoc, "getCmdLineOpts.parsed.sharding.configDB");
    if (!scfgdbElem.eoo()) {
        return "mongos"; //This is a made-up role name.
    }
    BSONElement clrElem = dps::extractElementAtPath(metadataDoc, "getCmdLineOpts.parsed.sharding.clusterRole");
    if (clrElem.eoo()) {
         return ""; //mongodb_exporter outputs "mongod" here. I think blank is better.
    } else {
         return clrElem.String(); //If present this will be "configsvr" or "shardsvr"
    }
}

Date_t FTDCProcessMetrics::firstSampleTs() const {
    return filespans.begin()->second.timespan.first;
}

Date_t FTDCProcessMetrics::estimateLastSampleTs() const {
    return filespans.rbegin()->second.timespan.last;
}

std::map<std::string, BSONType> FTDCProcessMetrics::lastRefDocKeys() {
   std::map<std::string, std::tuple<size_t, BSONType, uint64_t>> x =
           FTDCFileReader::flattenedBSONDoc(lastRefDoc);
   std::map<std::string, BSONType> m;
   for (auto itr = x.begin(); itr != x.end(); itr++) {
       m.insert({itr->first, std::get<1>(itr->second)});
   }
   return m;
}

//Unit test: FTDCProcessMetrics a.merge(b) should create the same doc as b.merge(a);
//Unit test: file (in sourceFilepaths) that is younger or identical version of should not change any member value
//Unit test: file that is the later, bigger-sample-length should raise estimated_end_ts
//Unit test: latersample.merge(onefile_oldersmallersample) should change no property
//Unit test: mismatching procId should return error
Status FTDCProcessMetrics::merge(const FTDCProcessMetrics& other) {
    if (procId != other.procId) {
        //It is a programming error if there is an attempt to merge metrics
        //of different processes
        return Status(ErrorCodes::BadValue,
                      "FTDCProcessMetrics::merge() on non-matching FTDCProcessId.");
    }

    /**
     * If other contains the same file (identified by the Date _id, not by
     * filepath) and that file is a younger, smaller-sample-length version then
     * we don't want to use any values from it: filepath, timespan and refDoc. 
     */
    for (std::map<Date_t, FTDCFileSpan>::const_iterator it = other.filespans.begin();
         it != other.filespans.end(); ++it) {
        auto date_id = it->first;
        auto oth_fpath = it->second.path;
        auto oth_timespan = it->second.timespan;
        auto oldfpItr = filespans.find(date_id);
        if (oldfpItr == filespans.end()) {
            filespans[date_id] = { oth_fpath, oth_timespan };
        } else {
            auto existing_fpath = oldfpItr->second.path;
            std::cerr << "Info: Duplicate FTDC files for " << procId.hostport <<
                    ", pid=" << procId.pid << ", starting datetime id=" <<
                    date_id << " found." << std::endl;
            if (oth_timespan.last < filespans[date_id].timespan.last) {
                std::cerr << "File " << oth_fpath << " will be ignored because file " <<
                    existing_fpath << " has more recent samples" << std::endl;
            } else {
                std::cerr << "File " << existing_fpath << "'s metrics will be discarded because file " <<
                    oth_fpath << " has more recent samples" << std::endl;
                filespans[date_id] = { oth_fpath, oth_timespan };
            }
        }
    }

    auto fSTs = firstSampleTs();
    auto oFSTs = other.firstSampleTs();
    if (oFSTs < fSTs) {
        metadataDoc = other.metadataDoc; //expected to be identical in all files, but for consistency let's use latest
        lastRefDoc = other.lastRefDoc;
    }

    return Status::OK();
}

StatusWith<FTDCMetricsSubset> FTDCProcessMetrics::timeseries(std::vector<std::string>& keys, 
                FTDCPMTimespan tspan, uint32_t sampleResolution) {
    invariant(tspan.isValid());
    FTDCPMTimespan ownTspan = { firstSampleTs(), estimateLastSampleTs() };
    invariant(ownTspan.overlaps(tspan)); //strictly forcing callers to avoid using when the result timespan would be empty
    FTDCPMTimespan trimmedTspan = tspan.intersection(ownTspan);
//std::cout << "Trimmed timespan for " << procId.hostport << "(" << procId.pid << ") = " << trimmedTspan.first << " - " << trimmedTspan.last << "\n";
    FTDCMetricsSubset m(keys, trimmedTspan, sampleResolution);

    for (std::map<Date_t, FTDCFileSpan>::iterator it = filespans.begin();
         it != filespans.end(); ++it) {
        if (it->second.timespan.overlaps(trimmedTspan)) {
            FTDCFileReader reader;
            auto s = reader.open(it->second.path);
            if (s != Status::OK()) {
                return s;
            }
            auto sET = reader.extractTimeseries(m);
            if (!sET.isOK()) {
                return sET;
            }
        }
    }
    return {m};
}

std::ostream& operator<<(std::ostream& os, FTDCProcessMetrics& pm) {
    std::string fpList;
    for (std::map<Date_t, FTDCFileSpan>::iterator it = pm.filespans.begin();
         it != pm.filespans.end(); ++it) {
        fpList = fpList + it->second.path.string() + " ";
    }
    os << pm.procId.hostport << " [" << pm.procId.pid << "] " << pm.rsName() << " " << fpList;
    return os;
}

}  // namespace mongo
