#pragma once

#include "../System.h"
#include "../EvaluationLog.h"

#include "MultiMapping.h"
#include "ReplicationTopologicalSorting.h"

#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <limits>
#include <iostream>
#include <cassert>


class ReplicationEvaluator {
private:
    System const& sys;
    mutable EvaluationLog log;
    bool log_results;

    /*
     * chosen_sources[destination_task][destination_processor][predecessor_task]
     * = source_processor
     */
    std::unordered_map<
        Task*,
        std::unordered_map<
        Processor const*,
        std::unordered_map<Task*, Processor const*>
        >
    > chosen_sources;


    bool has_chosen_source(
        Task* destination_task,
        Processor const* destination_processor,
        Task* predecessor_task) const
    {
        auto task_it = chosen_sources.find(destination_task);
        if (task_it == chosen_sources.end()) return false;

        auto processor_it = task_it->second.find(destination_processor);
        if (processor_it == task_it->second.end()) return false;

        return processor_it->second.contains(predecessor_task);
    }


    /*
     * Returns the replica used by the processor-specific projection
     * for streaming detection.
     */
    MultiMapping::DeviceTriplet const* get_projected_replica(
        MultiMapping const& mapping,
        Task* task,
        Processor const* streaming_processor) const
    {
        auto const* replica = mapping.get_replica(task, streaming_processor);
        if (replica != nullptr) return replica;

        auto const& replicas = mapping.get_replicas(task);
        if (replicas.empty()) return nullptr;

        return &(*replicas.begin());
    }


    /*
     * Evaluate one ordinary task replica.
     */
    void evaluate_task_replica(
        Task* task,
        MultiMapping::DeviceTriplet const& replica,
        std::unordered_map<Device const*, Time>& time) const
    {
        Processor const* processor = replica.processor;
        Memory const* mem_in = replica.memory_in;
        Memory const* mem_out = replica.memory_out;

        Time const t_start = std::max({ time[processor], time[mem_in], time[mem_out] });

        Time const t_end =
            t_start
            + sys.computation_time_ms(task, processor)
            + sys.transaction_time_ms(task->get_input_size(), mem_in, processor)
            + sys.transaction_time_ms(task->get_output_size(), processor, mem_out);

        time[processor] = t_end;
        time[mem_in] = t_end;
        time[mem_out] = t_end;

        if (log_results) log.log(task, t_start, t_end);
    }


    /*
     * For every destination replica, choose the source replica
     * whose data arrives first.
     */
    void evaluate_edge(
        Edge* edge,
        MultiMapping const& mapping,
        std::unordered_map<Device const*, Time>& time)
    {
        Task* source = edge->get_src();
        Task* destination = edge->get_snk();

        for (auto const& destination_replica : mapping.get_replicas(destination)) {
            Processor const* destination_processor = destination_replica.processor;

            if (has_chosen_source(destination, destination_processor, source)) continue;

            Time best_end = std::numeric_limits<Time>::infinity();
            Time best_start = 0;

            Processor const* best_source_processor = nullptr;
            Memory const* best_source_memory = nullptr;

            for (auto const& source_replica : mapping.get_replicas(source)) {
                Memory const* mem_out = source_replica.memory_out;
                Memory const* mem_in = destination_replica.memory_in;

                Time const t_start = std::max(time[mem_out], time[mem_in]);
                Time const t_end =
                    t_start
                    + sys.transaction_time_ms(source->get_output_size(), mem_out, mem_in);

                if (best_source_processor == nullptr || t_end < best_end) {
                    best_start = t_start;
                    best_end = t_end;
                    best_source_processor = source_replica.processor;
                    best_source_memory = mem_out;
                }
            }

            assert(best_source_processor != nullptr);
            assert(best_source_memory != nullptr);

            chosen_sources[destination][destination_processor][source] = best_source_processor;

            time[best_source_memory] = best_end;
            time[destination_replica.memory_in] = best_end;

            if (log_results) log.log(edge, best_start, best_end);
        }
    }


    /*
     * Evaluate one compressed streaming pipeline.
     */
    void evaluate_streaming_subgraph(
        ReplicationStreamingSubGraph const& subgraph,
        MultiMapping const& mapping,
        std::unordered_map<Device const*, Time>& time)
    {
        Processor const* streaming_processor = subgraph.processor;
        assert(streaming_processor != nullptr);

        Time t_start = 0;
        for (Device const* device : subgraph.devices) {
            t_start = std::max(t_start, time[device]);
        }

        Time execution_time = 0;

        for (Task* task : subgraph.tasks) {
            auto const* replica = mapping.get_replica(task, streaming_processor);
            assert(replica != nullptr);

            execution_time = std::max(
                execution_time,
                sys.computation_time_ms(task, replica->processor)
            );

            execution_time = std::max(
                execution_time,
                sys.transaction_time_ms(task->get_input_size(), replica->memory_in, replica->processor)
            );

            execution_time = std::max(
                execution_time,
                sys.transaction_time_ms(task->get_output_size(), replica->processor, replica->memory_out)
            );
        }

        for (Edge* edge : subgraph.edges) {
            Task* source = edge->get_src();
            Task* destination = edge->get_snk();

            auto const* source_replica = get_projected_replica(mapping, source, streaming_processor);
            auto const* destination_replica = get_projected_replica(mapping, destination, streaming_processor);

            assert(source_replica != nullptr);
            assert(destination_replica != nullptr);

            execution_time = std::max(
                execution_time,
                sys.transaction_time_ms(
                    source->get_output_size(),
                    source_replica->memory_out,
                    destination_replica->memory_in
                )
            );

            chosen_sources[destination][destination_replica->processor][source] =
                source_replica->processor;
        }

        Time const t_end = t_start + execution_time;

        for (Device const* device : subgraph.devices) {
            time[device] = t_end;
        }

        if (log_results) {
            for (Task* task : subgraph.tasks) log.log(task, t_start, t_end);
            for (Edge* edge : subgraph.edges) log.log(edge, t_start, t_end);
        }
    }


public:
    ReplicationEvaluator(System const& sys, bool log_results = false)
        : sys(sys), log_results(log_results)
    {}


    EvaluationLog const& get_log() const {
        return log;
    }


    bool is_complete(MultiMapping const& mapping, Task** out_task = nullptr) const
    {
        for (Task* task : sys.get_task_graph().get_tasks()) {
            auto const* replicas = mapping.try_get_replicas(task);

            if (replicas == nullptr || replicas->empty()) {
                if (out_task) *out_task = task;
                return false;
            }

            for (auto const& replica : *replicas) {
                if (replica.processor == nullptr ||
                    replica.memory_in == nullptr ||
                    replica.memory_out == nullptr)
                {
                    if (out_task) *out_task = task;
                    return false;
                }
            }
        }

        return true;
    }


    bool is_compatible(MultiMapping const& mapping, Task** out_task = nullptr) const
    {
        for (Task* task : sys.get_task_graph().get_tasks()) {
            auto const* replicas = mapping.try_get_replicas(task);

            if (replicas == nullptr) {
                if (out_task) *out_task = task;
                return false;
            }

            for (auto const& replica : *replicas) {
                if (!sys.is_compatible(task, replica.processor)) {
                    if (out_task) *out_task = task;
                    return false;
                }
            }
        }

        return true;
    }


    /*
     * Validates the physical capacity of the resulting MultiMapping.
     * This is separate from the stateful remaining_area rule used
     * by the search policy.
     */
    bool satisfies_capacity_constraint(
        MultiMapping const& mapping,
        Processor const** out_proc = nullptr) const
    {
        for (Processor const* processor : sys.get_platform().get_processors()) {
            Area capacity = processor->get_maximum_capacity();

            if (capacity < std::numeric_limits<Area>::infinity()) {
                for (Task* task : sys.get_task_graph().get_tasks()) {
                    if (mapping.has_replica(task, processor)) {
                        capacity -= task->get_area_requirement();
                    }
                }

                if (capacity < 0) {
                    if (out_proc) *out_proc = processor;
                    return false;
                }
            }
        }

        return true;
    }


    Processor const* get_chosen_source(
        Task* destination_task,
        Processor const* destination_processor,
        Task* predecessor_task) const
    {
        auto task_it = chosen_sources.find(destination_task);
        if (task_it == chosen_sources.end()) return nullptr;

        auto processor_it = task_it->second.find(destination_processor);
        if (processor_it == task_it->second.end()) return nullptr;

        auto predecessor_it = processor_it->second.find(predecessor_task);
        if (predecessor_it == processor_it->second.end()) return nullptr;

        return predecessor_it->second;
    }


    Time compute_cost(MultiMapping const& mapping)
    {
        chosen_sources.clear();

        ReplicationTopologicalSorting sorting(sys, mapping);
        return compute_cost_with_sorting(mapping, sorting);
    }


    Time compute_cost_with_sorting(
        MultiMapping const& mapping,
        ReplicationTopologicalSorting const& sorting)
    {
        std::unordered_map<Device const*, Time> time;

        for (Processor* processor : sys.get_platform().get_processors()) {
            time[processor] = 0;
        }

        for (Memory* memory : sys.get_platform().get_memories()) {
            time[memory] = 0;
        }

        std::unordered_set<ReplicationStreamingSubGraph const*> evaluated_streaming_subgraphs;

        for (GraphElement const& element : sorting.get_sorted_elements()) {
            if (Task* task = element.get_task()) {
                for (auto const& replica : mapping.get_replicas(task)) {
                    ReplicationStreamingSubGraph const* streaming_subgraph =
                        sorting.get_streaming_subgraph(task, replica.processor);

                    if (streaming_subgraph == nullptr) {
                        evaluate_task_replica(task, replica, time);
                        continue;
                    }

                    if (evaluated_streaming_subgraphs.contains(streaming_subgraph)) continue;

                    if (!sorting.is_first_task_in_streaming_subgraph(task, replica.processor)) continue;

                    evaluate_streaming_subgraph(*streaming_subgraph, mapping, time);
                    evaluated_streaming_subgraphs.insert(streaming_subgraph);
                }

                continue;
            }

            if (Edge* edge = element.get_edge()) {
                evaluate_edge(edge, mapping, time);
            }
        }

        for (ReplicationStreamingSubGraph const& subgraph : sorting.get_streaming_subgraphs()) {
            assert(evaluated_streaming_subgraphs.contains(&subgraph));
        }

        Time result = 0;

        for (auto const& [device, finish_time] : time) {
            result = std::max(result, finish_time);
        }

        return result;
    }


    Time evaluate_mapping_with_check(MultiMapping const& mapping)
    {
        Task* invalid_task = nullptr;

        if (!is_complete(mapping, &invalid_task)) {
            std::cerr
                << "MultiMapping incomplete. Missing or invalid replica for task "
                << invalid_task->get_label()
                << std::endl;
            return -1;
        }

        if (!is_compatible(mapping, &invalid_task)) {
            std::cerr
                << "MultiMapping invalid. Incompatible processor for task "
                << invalid_task->get_label()
                << std::endl;
            return -1;
        }

        Processor const* invalid_processor = nullptr;

        if (!satisfies_capacity_constraint(mapping, &invalid_processor)) {
            std::cerr
                << "MultiMapping invalid. Not enough capacity for "
                << invalid_processor->get_label()
                << std::endl;
            return -1;
        }

        return compute_cost(mapping);
    }
};