#pragma once

#include "../System.h"
#include "../Mapping.h"
#include "../TaskGraph.h"

#include "MultiMapping.h"
#include "ReplicationEvaluation.h"

#include <unordered_map>
#include <cassert>
#include <limits>


struct ReplicationSolution {
    TaskGraph graph;
    Mapping mapping;
    MultiMapping multi_mapping;

    size_t move_count = 0;
    size_t replication_count = 0;
    size_t final_replica_count = 0;
};


class ReplicationUtility {
private:

    static Processor const* choose_source_replica_min_transfer(
        System const& sys,
        MultiMapping const& multi_mapping,
        Task* predecessor,
        Memory const* successor_memory_in)
    {
        Processor const* best_processor = nullptr;
        Time best_transfer_time = std::numeric_limits<Time>::infinity();

        for (auto const& source_replica : multi_mapping.get_replicas(predecessor)) {
            Time const transfer_time = sys.transaction_time_ms(
                predecessor->get_output_size(),
                source_replica.memory_out,
                successor_memory_in
            );

            if (best_processor == nullptr || transfer_time < best_transfer_time) {
                best_transfer_time = transfer_time;
                best_processor = source_replica.processor;
            }
        }

        return best_processor;
    }

public:

    static ReplicationSolution materialize_solution(
        System const& sys,
        MultiMapping const& multi_mapping)
    {
        TaskGraph new_graph;
        Mapping new_mapping;

        /*
         * new_tasks[original_task][processor]
         * = concrete task representing that replica.
         */
        std::unordered_map<
            Task*,
            std::unordered_map<Processor const*, Task*>
        > new_tasks;


        /*
         * Create one concrete task for every replica.
         */
        for (Task* original_task : sys.get_task_graph().get_tasks()) {
            for (auto const& replica : multi_mapping.get_replicas(original_task)) {
                Task* new_task = new_graph.add_node(
                    original_task->get_complexity(),
                    original_task->get_parallelizability(),
                    original_task->get_streamability(),
                    original_task->get_size_func()
                );

                new_task->set_area(original_task->get_area_requirement());

                new_tasks[original_task][replica.processor] = new_task;

                new_mapping.map(
                    new_task,
                    replica.processor,
                    replica.memory_in,
                    replica.memory_out
                );
            }
        }


        /*
         * Re-evaluate the final MultiMapping so chosen_sources contains
         * the predecessor replica selected for every destination replica.
         */
        ReplicationEvaluator evaluator(sys);
        evaluator.compute_cost(multi_mapping);


        /*
         * Materialize every logical dependency.
         *
         * Each destination replica receives input from exactly one
         * predecessor replica, as selected by ReplicationEvaluator.
         */
        for (Edge* original_edge : sys.get_task_graph().get_edges()) {
            Task* predecessor = original_edge->get_src();
            Task* successor = original_edge->get_snk();

            for (auto const& successor_replica : multi_mapping.get_replicas(successor)) {
                Processor const* source_processor = evaluator.get_chosen_source(
                    successor,
                    successor_replica.processor,
                    predecessor
                );

                assert(source_processor != nullptr);

                Task* new_predecessor = new_tasks.at(predecessor).at(source_processor);
                Task* new_successor = new_tasks.at(successor).at(successor_replica.processor);

                new_graph.add_edge(new_predecessor, new_successor);
            }
        }


        ReplicationSolution solution;
        solution.graph = std::move(new_graph);
        solution.mapping = std::move(new_mapping);
        solution.multi_mapping = multi_mapping;

        return solution;
    }


    static ReplicationSolution materialize_solution_min_transfer(
        System const& sys,
        MultiMapping const& multi_mapping)
    {
        TaskGraph new_graph;
        Mapping new_mapping;

        std::unordered_map<
            Task*,
            std::unordered_map<Processor const*, Task*>
        > new_tasks;

        /*
         * Create one concrete task for every replica.
         */
        for (Task* original_task : sys.get_task_graph().get_tasks()) {
            for (auto const& replica : multi_mapping.get_replicas(original_task)) {
                Task* new_task = new_graph.add_node(
                    original_task->get_complexity(),
                    original_task->get_parallelizability(),
                    original_task->get_streamability(),
                    original_task->get_size_func()
                );

                new_task->set_area(original_task->get_area_requirement());

                new_tasks[original_task][replica.processor] = new_task;

                new_mapping.map(
                    new_task,
                    replica.processor,
                    replica.memory_in,
                    replica.memory_out
                );
            }
        }

        /*
         * Materialize every logical dependency.
         *
         * For every destination replica, choose the predecessor replica
         * with the minimum communication time.
         */
        for (Edge* original_edge : sys.get_task_graph().get_edges()) {
            Task* predecessor = original_edge->get_src();
            Task* successor = original_edge->get_snk();

            for (auto const& successor_replica : multi_mapping.get_replicas(successor)) {
                Processor const* source_processor =
                    choose_source_replica_min_transfer(
                        sys,
                        multi_mapping,
                        predecessor,
                        successor_replica.memory_in
                    );

                assert(source_processor != nullptr);

                Task* new_predecessor =
                    new_tasks.at(predecessor).at(source_processor);

                Task* new_successor =
                    new_tasks.at(successor).at(successor_replica.processor);

                new_graph.add_edge(new_predecessor, new_successor);
            }
        }

        ReplicationSolution solution;
        solution.graph = std::move(new_graph);
        solution.mapping = std::move(new_mapping);
        solution.multi_mapping = multi_mapping;

        return solution;
    }
};