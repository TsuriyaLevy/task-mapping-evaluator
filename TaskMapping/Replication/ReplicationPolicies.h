#pragma once

#define NOLOG

#include "../System.h"
#include "../Mapping.h"
#include "../MappingUtility.h"
#include "../GreedyMapper.h"
#include "../Evaluation.h"

#include "ReplicationUtility.h"
#include "ReplicationSystem.h"
#include "MultiMapping.h"
#include "ReplicationEvaluation.h"

#include <unordered_map>
#include <vector>
#include <iomanip>
#include <cassert>
#include <queue>
#include <limits>


typedef std::vector<Task*> ReplicationSubGraphSet;
typedef std::vector<ReplicationSubGraphSet> ReplicationDecomposition;

struct ReplicationSearchStats {
    size_t move_count = 0;
    size_t replication_count = 0;
};

enum class ReplicationSearchStrategy {
    INTERLEAVED,
    TWO_PHASES
};

enum class ReplicationEvaluationMethod {
    EARLIEST_ARRIVAL,
    ORIGINAL_EVALUATOR
};

constexpr ReplicationEvaluationMethod REPLICATION_EVALUATION_METHOD =
ReplicationEvaluationMethod::EARLIEST_ARRIVAL;

class ReplicationEvaluationPolicyBase {

public:

    static Time evaluate_with_earliest_arrival(
        MultiMapping const& mapping,
        System const& sys)
    {
        ReplicationEvaluator evaluator(sys);
        return evaluator.compute_cost(mapping);
    }


    static Time evaluate_with_original_evaluator(
        MultiMapping const& mapping,
        System const& sys)
    {
        ReplicationSolution solution =
            ReplicationUtility::materialize_solution_min_transfer(sys, mapping);

        ReplicationSystem replication_system(
            std::move(solution.graph),
            sys.get_platform()
        );

        MappingEvaluator evaluator(replication_system);

        return evaluator.compute_cost(solution.mapping);
    }

    static Time evaluate_mapping(
        MultiMapping const& mapping,
        System const& sys)
    {
        if (REPLICATION_EVALUATION_METHOD ==
            ReplicationEvaluationMethod::ORIGINAL_EVALUATOR)
        {
            return evaluate_with_original_evaluator(mapping, sys);
        }

        return evaluate_with_earliest_arrival(mapping, sys);
    }

protected:

    /*
     * MOVE keeps the original Series-Parallel behavior:
     * compatible tasks are moved to the target processor and all
     * previous replicas of those tasks are removed.
     */
    static bool apply_move(MultiMapping& mapping, System const& sys, ReplicationSubGraphSet const& subgraph, DevicePair const& dev_pair)
    {
        bool change = false;

        Processor const* target_processor = dev_pair.get_proc();
        Memory const* target_memory = dev_pair.get_mem();

        for (Task* task : subgraph) {
            if (!sys.is_compatible(task, target_processor)) continue;

            auto const& replicas = mapping.get_replicas(task);
            bool already_only_on_target = replicas.size() == 1 && mapping.has_replica(task, target_processor);

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
    static bool apply_replication(MultiMapping& mapping, System const& sys, ReplicationSubGraphSet const& subgraph, DevicePair const& dev_pair)
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
    static Area replication_area(MultiMapping const& mapping, System const& sys, ReplicationSubGraphSet const& subgraph, Processor const* target_processor)
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


class EvaluateAllWithReplication : public ReplicationEvaluationPolicyBase {
private:

    enum class BestOperation {
        NONE,
        MOVE,
        REPLICATE
    };


    static bool run_iteration(MultiMapping& mapping, System const& sys, std::vector<DevicePair> const& device_pairs, ReplicationDecomposition const& decomposition,
        std::unordered_map<ReplicationSubGraphSet const*, Area> const& areas, std::unordered_map<Processor const*, Area>& remaining_area,
        Time& cost, bool allow_move, bool allow_replication, ReplicationSearchStats& stats)
    {

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
                if (allow_move && (!processor->has_maximum_capacity() || areas.at(&subgraph) < remaining_area[processor])) {
                    MultiMapping current_mapping = mapping;

                    if (apply_move(current_mapping, sys, subgraph, dev_pair)) {
                        Time curr_cost = evaluate_mapping(current_mapping, sys);

                        if (curr_cost < best_cost) {
                            best_cost = curr_cost;
                            best_mapping = std::move(current_mapping);
                            best_proc = processor;
                            best_area = areas.at(&subgraph);
                            best_operation = BestOperation::MOVE;
                        }
                    }
                }


                /*
                 * REPLICATE candidate.
                 */
                if (allow_replication) {
                    Area const replicate_area = replication_area(mapping, sys, subgraph, processor);

                    if (replicate_area == 0) continue;

                    if (!processor->has_maximum_capacity() || replicate_area < remaining_area[processor]) {
                        MultiMapping current_mapping = mapping;

                        if (apply_replication(current_mapping, sys, subgraph, dev_pair)) {
                            Time curr_cost = evaluate_mapping(current_mapping, sys);

                            /*
                             * MOVE is evaluated first, so an exact tie
                             * remains a MOVE in INTERLEAVED mode.
                             */
                            if (curr_cost < best_cost) {
                                best_cost = curr_cost;
                                best_mapping = std::move(current_mapping);
                                best_proc = processor;
                                best_area = replicate_area;
                                best_operation = BestOperation::REPLICATE;
                            }
                        }
                    }
                }
            }
        }

        if (best_operation == BestOperation::NONE) return false;

        mapping = std::move(best_mapping);
        cost = best_cost;

        assert(best_proc != nullptr);

        if (best_proc->has_maximum_capacity()) {
            remaining_area[best_proc] -= best_area;
        }

        if (best_operation == BestOperation::MOVE) {
            ++stats.move_count;
        }
        else if (best_operation == BestOperation::REPLICATE) {
            ++stats.replication_count;
        }

        return true;
    }


    static void run_until_convergence(MultiMapping& mapping, System const& sys, std::vector<DevicePair> const& device_pairs, ReplicationDecomposition const& decomposition,
        std::unordered_map<ReplicationSubGraphSet const*, Area> const& areas, std::unordered_map<Processor const*, Area>& remaining_area,
        Time& cost, bool allow_move, bool allow_replication, ReplicationSearchStats& stats)
    {
        while (run_iteration(mapping, sys, device_pairs, decomposition, areas, remaining_area, cost, allow_move, allow_replication, stats)) {
        }
    }


public:

    static ReplicationSearchStats adapt_mapping(MultiMapping& mapping, System const& sys, std::vector<DevicePair> const& device_pairs, ReplicationDecomposition const& decomposition, ReplicationSearchStrategy strategy)
    {
        Time cost = evaluate_mapping(mapping, sys);

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

        ReplicationSearchStats stats;

        if (strategy == ReplicationSearchStrategy::INTERLEAVED) {

            /*
             * Original replication strategy:
             * MOVE and REPLICATE compete in every iteration.
             */
            run_until_convergence(mapping, sys, device_pairs, decomposition, areas, remaining_area, cost, true, true, stats);
        }
        else {

            /*
             * Phase 1:
             * Perform only MOVE operations until the original
             * Series-Parallel search reaches a local optimum.
             */
            run_until_convergence(mapping, sys, device_pairs, decomposition, areas, remaining_area, cost, true, false, stats);

            /*
             * Phase 2:
             * Starting from the MOVE local optimum, perform only
             * REPLICATE operations until convergence.
             */
            run_until_convergence(mapping, sys, device_pairs, decomposition, areas, remaining_area, cost, false, true, stats);
        }

        return stats;
    }
};


template <int THRESHOLD_TIMES_TEN>
class EvaluateThresholdWithReplication : public  ReplicationEvaluationPolicyBase {
private:

    enum class Operation {
        MOVE,
        REPLICATE
    };


    struct QueueElement {
        Time time_diff;
        DevicePair const* dev_pair;
        ReplicationSubGraphSet const* subgraph;
        Operation operation;

        bool operator<(QueueElement const& other) const {
            return time_diff < other.time_diff;
        }

        QueueElement(Time const& time_diff, DevicePair const* dev_pair, ReplicationSubGraphSet const* subgraph, Operation operation)
            : time_diff(time_diff), dev_pair(dev_pair), subgraph(subgraph), operation(operation) {}
    };


    static void update_stats(ReplicationSearchStats& stats, Operation operation)
    {
        if (operation == Operation::MOVE) {
            ++stats.move_count;
        }
        else {
            ++stats.replication_count;
        }
    }


    static void run_phase(MultiMapping& mapping, System const& sys, std::vector<DevicePair> const& device_pairs, ReplicationDecomposition const& decomposition,
        std::unordered_map<ReplicationSubGraphSet const*, Area> const& areas, std::unordered_map<Processor const*, Area>& remaining_area,
        Time& cost, bool allow_move, bool allow_replication, ReplicationSearchStats& stats)
    {

        std::priority_queue<QueueElement> effect_queue;

        MultiMapping best_mapping = mapping;
        Processor const* best_proc = nullptr;
        Area best_area = 0;
        Operation best_operation = Operation::MOVE;
        Time best_cost = cost;


        /*
         * Initial full evaluation.
         *
         * As in the original EvaluateThreshold, every candidate is
         * evaluated once before FirstFit starts using previous effects.
         *
         * MOVE and REPLICATE are separate candidates.
         */
        for (ReplicationSubGraphSet const& subgraph : decomposition) {
            Area const move_area = areas.at(&subgraph);

            for (DevicePair const& dev_pair : device_pairs) {
                Processor const* processor = dev_pair.get_proc();


                /*
                 * MOVE candidate.
                 */
                if (allow_move) {
                    if (!processor->has_maximum_capacity() || move_area <= remaining_area[processor]) {
                        Time cost_diff = 0;
                        MultiMapping current_mapping = mapping;

                        if (apply_move(current_mapping, sys, subgraph, dev_pair)) {
                            Time curr_cost = evaluate_mapping(current_mapping, sys);
                            cost_diff = cost - curr_cost;

                            if (curr_cost < best_cost) {
                                best_cost = curr_cost;
                                best_mapping = std::move(current_mapping);
                                best_proc = processor;
                                best_area = move_area;
                                best_operation = Operation::MOVE;
                            }
                        }

                        effect_queue.emplace(cost_diff, &dev_pair, &subgraph, Operation::MOVE);
                    }
                }


                /*
                 * REPLICATE candidate.
                 *
                 * Unlike MOVE, the required area is not fixed:
                 * it depends on which replicas already exist.
                 */
                if (allow_replication) {
                    Time cost_diff = std::numeric_limits<Time>::min();
                    Area const replicate_area = replication_area(mapping, sys, subgraph, processor);

                    if (replicate_area == 0) {
                        cost_diff = 0;
                    }
                    else if (!processor->has_maximum_capacity() || replicate_area <= remaining_area[processor]) {
                        MultiMapping current_mapping = mapping;

                        if (apply_replication(current_mapping, sys, subgraph, dev_pair)) {
                            Time curr_cost = evaluate_mapping(current_mapping, sys);
                            cost_diff = cost - curr_cost;

                            /*
                             * MOVE is evaluated first, so an exact tie
                             * remains a MOVE in INTERLEAVED mode.
                             */
                            if (curr_cost < best_cost) {
                                best_cost = curr_cost;
                                best_mapping = std::move(current_mapping);
                                best_proc = processor;
                                best_area = replicate_area;
                                best_operation = Operation::REPLICATE;
                            }
                        }
                    }

                    effect_queue.emplace(cost_diff, &dev_pair, &subgraph, Operation::REPLICATE);
                }
            }
        }


        std::vector<QueueElement> updated_elements;


        /*
         * FirstFit loop, adapted directly from the authors'
         * EvaluateThreshold policy.
         */
        while (best_cost < cost) {

            /*
             * Apply the best operation found in the previous evaluation.
             */
            mapping = std::move(best_mapping);
            cost = best_cost;

            assert(best_proc != nullptr);

            if (best_proc->has_maximum_capacity()) {
                remaining_area[best_proc] -= best_area;
            }

            update_stats(stats, best_operation);


            /*
             * Candidates evaluated during the previous iteration now
             * return to the priority queue with their updated effects.
             */
            for (QueueElement& element : updated_elements) {
                effect_queue.push(std::move(element));
            }

            updated_elements.clear();


            /*
             * Search candidates in descending order of their previously
             * observed improvement.
             */
            while (!effect_queue.empty()) {
                QueueElement const& element = effect_queue.top();


                /*
                 * Same threshold rule as the original EvaluateThreshold.
                 *
                 * THRESHOLD_TIMES_TEN = 10 corresponds to FirstFit.
                 */
                if (cost != best_cost &&
                    (element.time_diff == std::numeric_limits<Time>::min() ||
                        cost - best_cost > THRESHOLD_TIMES_TEN / 10.0 * element.time_diff)) {
                    break;
                }


                Time cost_diff = std::numeric_limits<Time>::min();
                Processor const* processor = element.dev_pair->get_proc();


                /*
                 * Re-evaluate a MOVE candidate.
                 */
                if (element.operation == Operation::MOVE) {
                    Area const move_area = areas.at(element.subgraph);

                    if (!processor->has_maximum_capacity() || move_area <= remaining_area[processor]) {
                        MultiMapping current_mapping = mapping;

                        if (apply_move(current_mapping, sys, *element.subgraph, *element.dev_pair)) {
                            Time curr_cost = evaluate_mapping(current_mapping, sys);
                            cost_diff = cost - curr_cost;

                            if (curr_cost < best_cost) {
                                best_cost = curr_cost;
                                best_mapping = std::move(current_mapping);
                                best_proc = processor;
                                best_area = move_area;
                                best_operation = Operation::MOVE;
                            }
                        }
                        else {
                            cost_diff = 0;
                        }
                    }
                }


                /*
                 * Re-evaluate a REPLICATE candidate.
                 */
                else {
                    Area const replicate_area = replication_area(mapping, sys, *element.subgraph, processor);

                    if (replicate_area == 0) {
                        cost_diff = 0;
                    }
                    else if (!processor->has_maximum_capacity() || replicate_area <= remaining_area[processor]) {
                        MultiMapping current_mapping = mapping;

                        if (apply_replication(current_mapping, sys, *element.subgraph, *element.dev_pair)) {
                            Time curr_cost = evaluate_mapping(current_mapping, sys);
                            cost_diff = cost - curr_cost;

                            if (curr_cost < best_cost) {
                                best_cost = curr_cost;
                                best_mapping = std::move(current_mapping);
                                best_proc = processor;
                                best_area = replicate_area;
                                best_operation = Operation::REPLICATE;
                            }
                        }
                    }
                }


                updated_elements.emplace_back(cost_diff, element.dev_pair, element.subgraph, element.operation);
                effect_queue.pop();
            }
        }
    }


public:

    static ReplicationSearchStats adapt_mapping(MultiMapping& mapping, System const& sys, std::vector<DevicePair> const& device_pairs,
        ReplicationDecomposition const& decomposition, ReplicationSearchStrategy strategy)
    {
        Time cost = evaluate_mapping(mapping, sys);


        /*
         * Same subgraph-area computation as the original policies.
         */
        std::unordered_map<ReplicationSubGraphSet const*, Area> areas;

        for (ReplicationSubGraphSet const& subgraph : decomposition) {
            Area area = 0;

            for (Task* task : subgraph) {
                area += task->get_area_requirement();
            }

            areas[&subgraph] = area;
        }


        /*
         * Same one-way capacity model as the original algorithm.
         * Allocated area is not freed by later moves.
         */
        std::unordered_map<Processor const*, Area> remaining_area;

        for (DevicePair const& dev_pair : device_pairs) {
            Processor const* processor = dev_pair.get_proc();

            if (processor->has_maximum_capacity()) {
                remaining_area[processor] = processor->get_maximum_capacity();
            }
        }


        ReplicationSearchStats stats;


        if (strategy == ReplicationSearchStrategy::INTERLEAVED) {

            /*
             * MOVE and REPLICATE share one FirstFit search queue
             * and compete throughout the search.
             */
            run_phase(mapping, sys, device_pairs, decomposition, areas, remaining_area, cost, true, true, stats);
        }
        else {

            /*
             * Phase 1:
             * FirstFit search using MOVE candidates only.
             */
            run_phase(mapping, sys, device_pairs, decomposition, areas, remaining_area, cost, true, false, stats);


            /*
             * Phase 2:
             * Start a new FirstFit queue from the MOVE local optimum
             * and search using REPLICATE candidates only.
             */
            run_phase(mapping, sys, device_pairs, decomposition, areas, remaining_area, cost, false, true, stats);
        }


        return stats;
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
            multi_mapping.add_replica(task, base_mapping.get_processor(task), base_mapping.get_mem_in(task), base_mapping.get_mem_out(task));
        }

        return multi_mapping;
    }
};


