#pragma once

#include "../ResultHandling.h"

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <limits>
#include <string>
#include <vector>


struct ComparisonStatistics {
    size_t total_runs = 0;

    size_t replication_wins = 0;
    size_t sp_wins = 0;
    size_t equal = 0;

    double total_relative_improvement = 0.0;

    double min_relative_improvement =
        std::numeric_limits<double>::infinity();

    double max_relative_improvement =
        -std::numeric_limits<double>::infinity();

    double total_sp_runtime_ms = 0.0;
    double total_replication_runtime_ms = 0.0;
};


struct ReplicationComparisonStatistics {
    ComparisonStatistics all;
    ComparisonStatistics without_replication;
    ComparisonStatistics with_replication;
};


inline TestResult const* find_result(
    TestRun const& run,
    std::string const& label)
{
    for (TestResult const& result : run) {
        if (result.label == label) return &result;
    }

    return nullptr;
}


inline void update_statistics(
    ComparisonStatistics& stats,
    TestResult const& sp,
    TestResult const& replication)
{
    ++stats.total_runs;

    double sp_cost = sp.objective;
    double replication_cost = replication.objective;

    if (replication_cost < sp_cost) {
        ++stats.replication_wins;
    }
    else if (replication_cost > sp_cost) {
        ++stats.sp_wins;
    }
    else {
        ++stats.equal;
    }

    double relative_improvement = 0.0;

    if (sp_cost != 0) {
        relative_improvement =
            (sp_cost - replication_cost) / sp_cost;
    }

    stats.total_relative_improvement += relative_improvement;

    stats.min_relative_improvement =
        std::min(stats.min_relative_improvement, relative_improvement);

    stats.max_relative_improvement =
        std::max(stats.max_relative_improvement, relative_improvement);

    stats.total_sp_runtime_ms += sp.runtime_ms.count();
    stats.total_replication_runtime_ms += replication.runtime_ms.count();
}


inline ReplicationComparisonStatistics compute_replication_statistics(
    std::vector<TestRun> const& results)
{
    ReplicationComparisonStatistics stats;

    for (TestRun const& run : results) {
        TestResult const* sp =
            find_result(run, "SeriesParallelMapping");

        TestResult const* replication =
            find_result(run, "SeriesParallelReplicationMapping");

        if (sp == nullptr || replication == nullptr) continue;
        if (sp->timeout || replication->timeout) continue;

        update_statistics(stats.all, *sp, *replication);

        if (replication->replication_count == 0) {
            update_statistics(stats.without_replication, *sp, *replication);
        }
        else {
            update_statistics(stats.with_replication, *sp, *replication);
        }
    }

    return stats;
}


inline void print_statistics_group(
    std::string const& title,
    ComparisonStatistics const& stats,
    std::ostream& out)
{
    out << std::endl;
    out << title << std::endl;
    out << "----------------------------------------" << std::endl;
    out << "Runs: " << stats.total_runs << std::endl;

    if (stats.total_runs == 0) return;

    out << "Replication wins: "
        << stats.replication_wins
        << std::endl;

    out << "SP wins: "
        << stats.sp_wins
        << std::endl;

    out << "Equal: "
        << stats.equal
        << std::endl;

    double avg_improvement =
        stats.total_relative_improvement / stats.total_runs;

    double avg_sp_runtime =
        stats.total_sp_runtime_ms / stats.total_runs;

    double avg_replication_runtime =
        stats.total_replication_runtime_ms / stats.total_runs;

    out << std::fixed << std::setprecision(2);

    out << "Average improvement: "
        << avg_improvement * 100.0
        << "%"
        << std::endl;

    out << "Best improvement: "
        << stats.max_relative_improvement * 100.0
        << "%"
        << std::endl;

    out << "Worst improvement: "
        << stats.min_relative_improvement * 100.0
        << "%"
        << std::endl;

    out << "Average SP mapping time: "
        << avg_sp_runtime
        << " ms"
        << std::endl;

    out << "Average Replication mapping time: "
        << avg_replication_runtime
        << " ms"
        << std::endl;
}


inline void print_replication_statistics(
    std::vector<TestRun> const& results,
    std::ostream& out = std::cout)
{
    ReplicationComparisonStatistics stats =
        compute_replication_statistics(results);

    out << std::endl;
    out << "========================================" << std::endl;
    out << "SP vs SP + Replication" << std::endl;
    out << "========================================" << std::endl;

    print_statistics_group("ALL RUNS", stats.all, out);
    print_statistics_group(
        "NO REPLICATION SELECTED",
        stats.without_replication,
        out
    );
    print_statistics_group(
        "REPLICATION SELECTED",
        stats.with_replication,
        out
    );

    out << "========================================" << std::endl;
}