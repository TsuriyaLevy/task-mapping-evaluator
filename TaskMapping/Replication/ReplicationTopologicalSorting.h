#pragma once

#include "../TopologicalSorting.h"
#include "../Mapping.h"

#include "MultiMapping.h"

#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <cassert>


struct ReplicationStreamingSubGraph {
    Processor const* processor = nullptr;

    std::vector<Task*> tasks;
    std::vector<Edge*> edges;
    std::vector<Edge*> edges_out;

    std::unordered_set<Device const*> devices;
};


class ReplicationTopologicalSorting {
private:
    System const& sys;
    MultiMapping const& multi_mapping;

    TaskFirstBFSSorting base_sorting;

    std::vector<ReplicationStreamingSubGraph> streaming_subgraphs;

    /*
     * streaming_subgraph_index[task][processor]
     * gives the index of the streaming subgraph containing that replica.
     */
    std::unordered_map<
        Task*,
        std::unordered_map<Processor const*, size_t>
    > streaming_subgraph_index;


    /*
     * Build a one-to-one Mapping as seen from one streaming processor.
     *
     * A replica on streaming_proc is preferred. Otherwise, any
     * existing replica is used as fallback.
     */
    Mapping create_projection(Processor const* streaming_proc) const
    {
        Mapping projection;

        for (Task* task : sys.get_task_graph().get_tasks()) {
            auto const* replica = multi_mapping.get_replica(task, streaming_proc);

            if (replica != nullptr) {
                projection.map(task, replica->processor, replica->memory_in, replica->memory_out);
                continue;
            }

            auto const& replicas = multi_mapping.get_replicas(task);
            assert(!replicas.empty());

            auto const& fallback = *replicas.begin();
            projection.map(task, fallback.processor, fallback.memory_in, fallback.memory_out);
        }

        return projection;
    }


    /*
     * Copy an original compressed SubGraph into the
     * replication-aware representation.
     */
    ReplicationStreamingSubGraph copy_streaming_subgraph(
        SubGraph const* original_subgraph,
        Processor const* streaming_proc) const
    {
        ReplicationStreamingSubGraph result;

        result.processor = streaming_proc;
        result.tasks = original_subgraph->get_tasks();
        result.edges = original_subgraph->get_edges();
        result.edges_out = original_subgraph->get_edges_out();

        for (Task* task : result.tasks) {
            auto const* replica = multi_mapping.get_replica(task, streaming_proc);
            assert(replica != nullptr);

            result.devices.insert(replica->processor);
            result.devices.insert(replica->memory_in);
            result.devices.insert(replica->memory_out);
        }

        return result;
    }


    /*
     * Detect streaming pipelines for one processor using the
     * original compression algorithm.
     */
    void find_streaming_subgraphs(Processor const* streaming_proc)
    {
        Mapping projection = create_projection(streaming_proc);

        /*
         * Each processor gets its own sorting because compression for
         * one processor must not remove tasks that may also have
         * replicas on another processor.
         */
        TaskFirstBFSSorting temporary_sorting(sys.get_task_graph());
        temporary_sorting.compress_streamable_subtrees(projection, streaming_proc);

        /*
         * temporary_sorting owns the original SubGraph pointers, so
         * copy their contents before it is destroyed.
         */
        for (SubGraph const* original_subgraph : temporary_sorting.get_subgraphs()) {
            ReplicationStreamingSubGraph subgraph =
                copy_streaming_subgraph(original_subgraph, streaming_proc);

            size_t index = streaming_subgraphs.size();
            streaming_subgraphs.push_back(std::move(subgraph));

            for (Task* task : streaming_subgraphs[index].tasks) {
                assert(!streaming_subgraph_index[task].contains(streaming_proc));
                streaming_subgraph_index[task][streaming_proc] = index;
            }
        }
    }


    void build_streaming_subgraphs()
    {
        for (Processor* processor : sys.get_platform().get_processors()) {
            if (!processor->is_streaming_device()) continue;

            bool used = false;

            for (Task* task : sys.get_task_graph().get_tasks()) {
                if (multi_mapping.has_replica(task, processor)) {
                    used = true;
                    break;
                }
            }

            if (used) find_streaming_subgraphs(processor);
        }
    }


public:
    ReplicationTopologicalSorting(System const& sys, MultiMapping const& mapping)
        : sys(sys),
        multi_mapping(mapping),
        base_sorting(sys.get_task_graph())
    {
        build_streaming_subgraphs();
    }


    std::vector<GraphElement> const& get_sorted_elements() const
    {
        return base_sorting.get_sorted_elements();
    }


    std::vector<ReplicationStreamingSubGraph> const& get_streaming_subgraphs() const
    {
        return streaming_subgraphs;
    }


    ReplicationStreamingSubGraph const* get_streaming_subgraph(
        Task* task,
        Processor const* processor) const
    {
        auto task_it = streaming_subgraph_index.find(task);
        if (task_it == streaming_subgraph_index.end()) return nullptr;

        auto processor_it = task_it->second.find(processor);
        if (processor_it == task_it->second.end()) return nullptr;

        return &streaming_subgraphs.at(processor_it->second);
    }


    bool is_streaming_replica(Task* task, Processor const* processor) const
    {
        return get_streaming_subgraph(task, processor) != nullptr;
    }


    bool is_first_task_in_streaming_subgraph(
        Task* task,
        Processor const* processor) const
    {
        ReplicationStreamingSubGraph const* subgraph =
            get_streaming_subgraph(task, processor);

        if (subgraph == nullptr || subgraph->tasks.empty()) return false;

        return subgraph->tasks.front() == task;
    }
};