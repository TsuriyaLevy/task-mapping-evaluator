#pragma once

#include "../System.h"
#include "../Mapping.h"
#include "../TaskGraph.h"

#include "MultiMapping.h"
#include "ReplicationEvaluation.h"

#include <unordered_map>
#include <cassert>


struct ReplicationSolution {
    TaskGraph graph;
    Mapping mapping;
    MultiMapping multi_mapping;

    size_t move_count = 0;
    size_t replication_count = 0;
    size_t final_replica_count = 0;
};


class ReplicationUtility {
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
};