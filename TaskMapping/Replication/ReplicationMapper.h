#pragma once

#include "../System.h"
#include "../MappingUtility.h"
#include "../SeriesParallelDecomposition.h"

#include "MultiMapping.h"
#include "ReplicationPolicies.h"
#include "ReplicationUtility.h"

#include <unordered_set>
#include <queue>


class ReplicationMapper {
    ReplicationSearchStrategy strategy;
public:
    explicit ReplicationMapper(ReplicationSearchStrategy strategy)
        : strategy(strategy)
    {}

    ReplicationSolution get_task_mapping(System const& sys) const
    {
        std::vector<std::vector<Task*>> decomposition =
            create_decomposition(sys.get_task_graph());

        MultiMapping mapping =
            MultiMappingBase::create_base_mapping(sys);

        std::vector<DevicePair> device_pairs =
            device_pairs_from_platform(sys.get_platform());

        ReplicationSearchStats stats =
            EvaluateAllWithReplication::adapt_mapping(
                mapping,
                sys,
                device_pairs,
                decomposition,
                strategy
            );

        size_t final_replica_count = 0;

        for (Task* task : sys.get_task_graph().get_tasks()) {
            final_replica_count += mapping.get_replicas(task).size() - 1;
        }

        ReplicationSolution solution =
            ReplicationUtility::materialize_solution(sys, mapping);

        solution.move_count = stats.move_count;
        solution.replication_count = stats.replication_count;
        solution.final_replica_count = final_replica_count;

        return solution;
    }


private:
    std::vector<std::vector<Task*>>
        create_decomposition(TaskGraph const& task_graph) const
    {
        std::vector<std::vector<Task*>> decomposition;
        std::unordered_set<size_t> existing_subgraphs;

        SeriesParallelDecomposition spd_tree(task_graph);

        for (SeriesParallelOperation const* op : spd_tree.get_inner_nodes()) {
            auto [subgraph_id, subgraph] = subgraph_from_operation(op);

            if (subgraph.size() <= 1) continue;
            if (existing_subgraphs.contains(subgraph_id)) continue;

            existing_subgraphs.insert(subgraph_id);
            decomposition.push_back(std::move(subgraph));
        }

        /*
         * As in the original mapper, every individual task
         * is also considered as a candidate subgraph.
         */
        for (Task* task : task_graph.get_tasks()) {
            decomposition.push_back({ task });
        }

        return decomposition;
    }


    std::pair<size_t, std::vector<Task*>>
        subgraph_from_operation(SeriesParallelOperation const* op) const
    {
        std::vector<Task*> subgraph;
        size_t subgraph_id = 0;

        auto add_to_subgraph =
            [&subgraph, &subgraph_id](Task* task)
            {
                subgraph.push_back(task);
                subgraph_id ^= std::hash<Task*>{}(task);
            };

        std::queue<SeriesParallelOperation const*> queue;
        queue.push(op);

        /*
         * Parallel operations include their outer tasks,
         * matching the original Series-Parallel mapper.
         */
        if (op->get_type() == SeriesParallelOperationType::PARALLEL) {
            if (op->get_front()) add_to_subgraph(op->get_front());
            if (op->get_back()) add_to_subgraph(op->get_back());
        }

        while (!queue.empty()) {
            SeriesParallelOperation const* current = queue.front();
            queue.pop();

            switch (current->get_type()) {
            case SeriesParallelOperationType::EDGE:
                break;

            case SeriesParallelOperationType::PARALLEL:
            case SeriesParallelOperationType::SERIES:
            {
                Task* front = current->get_front();
                Task* back = current->get_back();

                for (SeriesParallelOperation const* inner : current->get_elements()) {
                    if (inner->get_front() != front) {
                        add_to_subgraph(inner->get_front());
                    }

                    if (inner->get_back() != back) {
                        add_to_subgraph(inner->get_back());
                    }

                    queue.push(inner);
                }

                break;
            }
            }
        }

        return { subgraph_id, std::move(subgraph) };
    }
};