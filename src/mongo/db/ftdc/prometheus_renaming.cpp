#include "mongo/db/ftdc/prometheus_renaming.h"
#include <vector>
#include <pcrecpp.h>

namespace mongo {

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
 * Note that it only rewrites the metric name in a string => string way. Other processing in promNameAndLabels()
 * will further rearrange some regex-matched metric names and put parts of the name in labels.
 *
 * ---------------------------------------------------------
 *
 * The golang code being referenced as of Dec 2019 is below:
 *
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
 *              "systemMetrics.disks.":                            "device_name",
 *      }
 * Extra-fiddly ones that labelize a node not at the lowest level.
 * mongodb_ss_opLatencies_(commands|writes|reads)_(ops|latency){...}
 * mongodb_ss_opLatencies_\2{...,"op_type":"\1"}
 *
 * mongodb_ss_wt_concurrentTransactions_(available|out|totalTickets){...}
 * mongodb_ss_wt_concurrentTransactions{...,"txn_rw":"\1"}
 */

/**
 * The following metrics will all be remade according the same regex match-and-replace pattern
 *   '^(prefix_of_metric)_(lowestnode)$' => '\1{"new_lbl_name"="\2"}'
 */
std::vector<std::tuple<pcrecpp::RE, std::string>> last_node_relabels = {
    {pcrecpp::RE("^(mongodb_collstats_storage_idx)_(.+)"),         "index_name"}, 
    {pcrecpp::RE("^(mongodb_globalLock_activeQueue)_(.+)"),        "count_type"},
    {pcrecpp::RE("^(mongodb_globalLock_locks)_(.+)"),              "lock_type"},
    {pcrecpp::RE("^(mongodb_ss_asserts)_(.+)"),                    "assert_type"},
    {pcrecpp::RE("^(mongodb_ss_connections)_(.+)"),                "conn_type"},
    {pcrecpp::RE("^(mongodb_ss_globalLock_currentQueue)_(.+)"),    "count_type"},
    {pcrecpp::RE("^(mongodb_ss_metrics_cursor_open)_(.+)"),        "csr_type"},
    {pcrecpp::RE("^(mongodb_ss_metrics_document)_(.+)"),           "doc_op_type"},
    {pcrecpp::RE("^(mongodb_ss_opReadConcernCounters)_(.+)"),      "concern_type"},
    {pcrecpp::RE("^(mongodb_ss_opcounters)_(.+)"),                 "legacy_op_type"},
    {pcrecpp::RE("^(mongodb_ss_opcountersRepl)_(.+)"),             "legacy_op_type"},
    {pcrecpp::RE("^(mongodb_ss_transactions_commitTypes)_(.+)"),   "commit_type"},
};

//Eg. "mongodb_rs_members_1_pingMs" =>
//     mongodb_rs_members_pingMs{"rs_mbr_id"="1"}
pcrecpp::RE rs_mbrs_re("^(mongodb_rs_members)_(\\d+)_([a-zA-Z0-9_]+)");

//Eg. "mongodb_ss_opLatencies_writes_ops" =>
//     mongodb_ss_opLatencies_ops{"op_type":"writes"}
pcrecpp::RE oplatencies_re("^(mongodb_ss_opLatencies)_(commands|writes|reads)_(ops|latency)");

//Eg. "mongodb_ss_wt_concurrentTransactions_read_available" =>
//     mongodb_ss_wt_concurrentTransactions{"txn_rw":"read"}
pcrecpp::RE wt_conctx_re("^(mongodb_ss_wt_concurrentTransactions)_(read|write)_(available|out|totalTickets)");

//Eg. "mongodb_sys_disks_sda_read_time_ms" =>
//     mongodb_sys_disks_sda_read_time_ms{"device_name":"sda"}
pcrecpp::RE sys_disks_re("^(mongodb_sys_disks)_([a-zA-Z0-9]+)_(.+)");

//Eg. "mongodb_ss_metrics_commands_migrateClone_total" =>
//     mongodb_ss_metrics_commands_total{"cmd_name":"migrateClone"}
pcrecpp::RE mcmds_total_failed_re("^(mongodb_ss_metrics_commands)_([a-zA-Z0-9]+)_(total|failed)$");

//Eg. "mongodb_ss_wt_perf_file_system_read_latency_histogram_bucket_1_10_49ms" =>
//     mongodb_ss_wt_perf_file_system_latency{"rw_type"="read","histogram_bucket"="1_10_49ms"}
pcrecpp::RE wt_perf_buckets_re("^(mongodb_ss_wt_perf_.+)_(read|write)_latency_histogram_bucket_([a-zA-Z0-9_]+)");

PrometheusNameAndLabels promNameAndLabels(std::string mongodb_m_name) {
    std::map<std::string, std::string> l;
    std::string n = prometheusize(mongodb_m_name);
    std::string ms1, ms2, ms3;

    if (rs_mbrs_re.FullMatch(n, &ms1, &ms2, &ms3)) {
        n = ms1 + "_" + ms3;
        l["rs_mbr_id"] = ms2;
    } else if (oplatencies_re.FullMatch(n, &ms1, &ms2, &ms3)) {
        n = ms1 + "_" + ms3;
        l["op_type"] = ms2;
    } else if (wt_conctx_re.FullMatch(n, &ms1, &ms2, &ms3)) {
        n = ms1+ "_" + ms3;
        l["txn_rw"] = ms2;
    } else if (sys_disks_re.FullMatch(n, &ms1, &ms2, &ms3)) {
        n = ms1 + "_" + ms3;
        l["device_name"] = ms2;
    } else if (mcmds_total_failed_re.FullMatch(n, &ms1, &ms2, &ms3)) {
        n = ms1 + "_" + ms3;
        l["cmd_name"] = ms2;
    } else if (wt_perf_buckets_re.FullMatch(n, &ms1, &ms2, &ms3)) {
        n = ms1 + "_latency";
        l["rw_type"] = ms2;
        l["histogram_bucket"] = ms3;
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

std::map<std::string, PrometheusNameAndLabels> prometheusRenamesMap(std::set<std::string> orig_names) {
    std::map<std::string, PrometheusNameAndLabels> m;
    for (auto onm : orig_names) {
        m[onm] = promNameAndLabels(onm);
    }
    return m;
}

}  // namespace mongo
