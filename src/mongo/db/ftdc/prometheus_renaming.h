#pragma once

#include <string>
#include <map>
#include <set>

namespace mongo {

struct PrometheusNameAndLabels {
    std::string name;
    std::map<std::string, std::string> extra_lbls;
};

/**
 * Create map of original MongoDB native, dot-concatenated getDiagnostic() metric name vs.
 * Prometheus-suitable equivalent name, plus extra labels for certain metrics.
 * E.g. the following metric is
 *
 *   serverStatus.metrics.commands.listCollections
 *
 * becomes the following prometheus metric.
 *
 *   mongodb_ss_metrics_commands{"cmd_name"="listCollections"}
 *
 * Labels are only created for certain metrics matched by regex rules. See promNameAndLabels() function.
 */
std::map<std::string, PrometheusNameAndLabels> prometheusRenamesMap(std::set<std::string> orig_names);

}  // namespace mongo
