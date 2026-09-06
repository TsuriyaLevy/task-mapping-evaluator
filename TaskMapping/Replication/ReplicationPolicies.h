#pragma once

#define NOLOG

#include "../System.h"
#include "../Mapping.h"
#include "../MappingUtility.h"
#include "../GreedyMapper.h"

#include "MultiMapping.h"
#include "ReplicationEvaluation.h"

#include <unordered_map>
#include <vector>
#include <iomanip>
#include <cassert>


typedef std::vector<Task*> ReplicationSubGraphSet;
typedef std::vector<ReplicationSubGraphSet> ReplicationDecomposition;


class ReplicationEvaluationPolicyBase {
protected:

    /*
     * MOVE keeps the original Series-Parallel behavior:
     * compatible tasks are moved to the target processor and all
     * previous replicas of those tasks are removed.
     */
    static bool apply_move(
        MultiMapping& mapping,
        System const& sys,
        ReplicationSubGraphSet const& subgraph,
        DevicePair const& dev_pair)
    {
        bool change = false;

        Processor const* target_processor = dev_pair.get_proc();
        Memory const* target_memory = dev_pair.get_mem();

        for (Task* task : subgraph) {
            if (!sys.is_compatible(task, target_processor)) continue;

            auto const& replicas = mapping.get_replicas(task);
            bool already_only_on_target =
                replicas.size() == 1 && mapping.has_replica(task, target_processor);

            if (already_only_on_target) continue;

            mapping.move_task(task, target_processor, target_memory, target_memory);
            change = true;
        }

        return change;
    }


    /*
     * REPLICATE keeps all existing replicas and adds a new one
     * on the target processor when possible.
     */
    static bool apply_replication(
        MultiMapping& mapping,
        System const& sys,
        ReplicationSubGraphSet const& subgraph,
        DevicePair const& dev_pair)
    {
        bool change = false;

        Processor const* target_processor = dev_pair.get_proc();
        Memory const* target_memory = dev_pair.get_mem();

        for (Task* task : subgraph) {
            if (!sys.is_compatible(task, target_processor)) continue;
            if (mapping.has_replica(task, target_processor)) continue;

            mapping.add_replica(task, target_processor, target_memory, target_memory);
            change = true;
        }

        return change;
    }


    /*
     * Area required only for replicas that would actually be added.
     */
    static Area replication_area(
        MultiMapping const& mapping,
        System const& sys,
        ReplicationSubGraphSet const& subgraph,
        Processor const* target_processor)
    {
        Area area = 0;

        for (Task* task : subgraph) {
            if (!sys.is_compatible(task, target_processor)) continue;
            if (mapping.has_replica(task, target_processor)) continue;

            area += task->get_area_requirement();
        }

        return area;
    }
};


class EvaluateAllWithReplication : ReplicationEvaluationPolicyBase {
private:

    enum class BestOperation {
        NONE,
        MOVE,
        REPLICATE
    };

public:

    static size_t adapt_mapping(
        MultiMapping& mapping,
        System const& sys,
        std::vector<DevicePair> const& device_pairs,
        ReplicationDecomposition const& decomposition)
    {
        ReplicationEvaluator eval(sys);
        Time cost = eval.compute_cost(mapping);

        /*
         * Same subgraph-area computation as the original EvaluateAll.
         */
        std::unordered_map<ReplicationSubGraphSet const*, Area> areas;

        for (ReplicationSubGraphSet const& subgraph : decomposition) {
            Area area = 0;
            for (Task* task : subgraph) area += task->get_area_requirement();
            areas[&subgraph] = area;
        }

        /*
         * Same one-way capacity model as the original algorithm.
         * Allocated area is not freed by later moves.
         */
        std::unordered_map<Processor const*, Area> remaining_area;

        for (DevicePair const& dev_pair : device_pairs) {
            Processor const* proc = dev_pair.get_proc();
            if (proc->has_maximum_capacity()) {
                remaining_area[proc] = proc->get_maximum_capacity();
            }
        }

#ifndef NOLOG
        size_t it_count = 0;
        size_t computed_mapping_count = 0;
#endif

        size_t replication_count = 0;
        bool change;

        do {
            change = false;

            MultiMapping best_mapping = mapping;
            Time best_cost = cost;
            Processor const* best_proc = nullptr;
            Area best_area = 0;
            BestOperation best_operation = BestOperation::NONE;

            /*
             * Keep the original loop order:
             * device pair first, decomposition subgraph second.
             */
            for (DevicePair const& dev_pair : device_pairs) {
                Processor const* processor = dev_pair.get_proc();

                for (ReplicationSubGraphSet const& subgraph : decomposition) {

                    /*
                     * MOVE candidate.
                     * The strict '<' capacity check is intentionally
                     * identical to the original EvaluateAll.
                     */
                    if (!processor->has_maximum_capacity() ||
                        areas[&subgraph] < remaining_area[processor])
                    {
                        MultiMapping current_mapping = mapping;

                        if (apply_move(current_mapping, sys, subgraph, dev_pair)) {
                            Time curr_cost = eval.compute_cost(current_mapping);

#ifndef NOLOG
                            ++computed_mapping_count;
#endif

                            if (curr_cost < best_cost) {
                                best_cost = curr_cost;
                                best_mapping = std::move(current_mapping);
                                best_proc = processor;
                                best_area = areas[&subgraph];
                                best_operation = BestOperation::MOVE;
                                change = true;
                            }
                        }
                    }


                    /*
                     * REPLICATE candidate.
                     */
                    Area const replicate_area =
                        replication_area(mapping, sys, subgraph, processor);

                    if (replicate_area == 0) continue;

                    if (!processor->has_maximum_capacity() ||
                        replicate_area < remaining_area[processor])
                    {
                        MultiMapping current_mapping = mapping;

                        if (apply_replication(current_mapping, sys, subgraph, dev_pair)) {
                            Time curr_cost = eval.compute_cost(current_mapping);

#ifndef NOLOG
                            ++computed_mapping_count;
#endif

                            /*
                             * MOVE is evaluated first, so an exact tie
                             * remains a MOVE.
                             */
                            if (curr_cost < best_cost) {
                                best_cost = curr_cost;
                                best_mapping = std::move(current_mapping);
                                best_proc = processor;
                                best_area = replicate_area;
                                best_operation = BestOperation::REPLICATE;
                                change = true;
                            }
                        }
                    }
                }
            }


            if (change) {
#ifndef NOLOG
                std::cout
                    << "Iteration " << std::left << std::setw(4) << ++it_count
                    << " Solution improved! New cost: " << std::setw(5) << best_cost
                    << " Computed mappings: " << computed_mapping_count
                    << std::endl;
#endif

                mapping = std::move(best_mapping);
                cost = best_cost;

                assert(best_proc != nullptr);

                if (best_proc->has_maximum_capacity()) {
                    remaining_area[best_proc] -= best_area;
                }

                if (best_operation == BestOperation::REPLICATE) {
                    ++replication_count;
                }
            }

        } while (change);

        return replication_count;
    }
};


/*
 * Same starting mapping as the original GreedyBase:
 * one CPU/Main_RAM mapping per task, converted to MultiMapping.
 */
class MultiMappingBase {
public:

    static MultiMapping create_base_mapping(System const& sys)
    {
        GreedyMapper mapper({ "CPU", "Main_RAM" });
        Mapping base_mapping = mapper.get_task_mapping(sys);

        MultiMapping multi_mapping;

        for (Task* task : sys.get_task_graph().get_tasks()) {
            multi_mapping.add_replica(
                task,
                base_mapping.get_processor(task),
                base_mapping.get_mem_in(task),
                base_mapping.get_mem_out(task)
            );
        }

        return multi_mapping;
    }
};