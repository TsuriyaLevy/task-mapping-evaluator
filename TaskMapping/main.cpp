//#include "tests.h"
//
//int main(int argc, char* argv[]) {
//
//	int SEED = (int)time(NULL);
//	const int DEFAULT_RUNS = 100;
//	const int DEFAULT_GRAPH_SIZE = 100;
//    const int DATA_IN_MB = 100;
//
//	int RUNS = -1;
//	int GRAPH_SIZE = -1;
//
//	if (argc > 1) {
//		GRAPH_SIZE = atoi(argv[1]);
//	}
//	if (argc > 2) {
//		RUNS = atoi(argv[2]);
//	}
//	if (argc > 3) {
//		int input_seed = atoi(argv[3]);
//		if (input_seed > 0) {
//			SEED = input_seed;
//		}
//	}
//
//	if (GRAPH_SIZE < 1 || GRAPH_SIZE > 1000) {
//		GRAPH_SIZE = DEFAULT_GRAPH_SIZE;
//	}
//
//	if (RUNS < 1 || RUNS > 1000) {
//		RUNS = DEFAULT_RUNS;
//	}
//
//	srand(SEED);
//	
//	std::cout << "No tests activated, uncomment tests in main.cpp to execute them" << std::endl;
//	
//	// Note: For MappingType::ZhouLiu activate specialized code in test_size_series instead (performance issues)
//    //test_size_series(SEED, 5, 1, 30, 30, [&DATA_IN_MB](int size){return generate_random_series_parallel_graph(size, DATA_IN_MB);}, { Configuration::CGF },
//    //    {MappingType::CPU, MappingType::SingleNode, MappingType::SeriesParallel, MappingType::DeviceMILP, MappingType::TimeMILPStream}); 
//	
//    //test_size_series(SEED, 5, 5, 200, 30, [&DATA_IN_MB](int size){return generate_random_series_parallel_graph(size, DATA_IN_MB);}, { Configuration::CGF },
//    //    {MappingType::CPU, MappingType::SingleNode, MappingType::SNFirstFit, MappingType::SeriesParallel, MappingType::SPFirstFit, MappingType::HEFT, MappingType::PEFT}); 
//
//    //test_size_series(SEED, 5, 5, 100, 30, [&DATA_IN_MB](int size){return generate_random_series_parallel_graph(size, DATA_IN_MB);}, { Configuration::CGF },
//    //    {MappingType::CPU, MappingType::SNFirstFit, MappingType::SPFirstFit, MappingType::SA, MappingType::NSGAII});
//		
//	//test_nsgaii_generation_series(SEED, 50, 50, 500, 30, [&DATA_IN_MB]() {return generate_random_series_parallel_graph(200, DATA_IN_MB);}, { Configuration::CGF }, 
//	//	{ MappingType::CPU, MappingType::SPFirstFit, MappingType::SNFirstFit });
//	
//    //test_size_series(SEED, 0, 5, 200, 30, [&DATA_IN_MB](int loose_edges) {return generate_random_almost_series_parallel_graph(100, DATA_IN_MB, loose_edges);}, { Configuration::CGF },
//    //                 {MappingType::CPU, MappingType::HEFT, MappingType::PEFT, MappingType::SPFirstFit, MappingType::SNFirstFit, MappingType::NSGAII});
//
//	//test_benchmark_graphs(SEED, 10, { Configuration::CGF },
//	//	{ "makeflow/blast", "pegasus/1000genome", "pegasus/cycles", "pegasus/epigenomics", "pegasus/montage", "pegasus/soykb", "pegasus/srasearch" },
//	//	{ MappingType::CPU, MappingType::HEFT, MappingType::PEFT, MappingType::SPFirstFit, MappingType::SNFirstFit, MappingType::NSGAII });
//
//	return 0;
//}


#include "tests.h"
#include "Replication/ReplicationResultHandling.h"

#include <fstream>
#include <cmath>

struct BasePoliciesForSharedDecomposition
{
    typedef EvaluateAll EvaluationPolicy;
    typedef GreedyBase BaseMappingPolicy;
};

int main()
{
    const int SEED = 1;
    const int DATA_IN_MB = 100;

    const int FROM = 10;
    const int STEP = 10;
    const int TO = 350;
    const int RUNS = 10;

    const std::vector<double> AUGMENTATION_RATIOS = { 0.00, 0.05, 0.10, 0.20 };

    srand(SEED);

    std::cout << "======================================" << std::endl;
    std::cout << "SP / Almost-SP Replication Evaluation" << std::endl;
    std::cout << "======================================" << std::endl;

    ComputationBasedSystem system(
        TaskGraph(),
        create_platform(nbr_fpgas(Configuration::CGF))
    );

    std::vector<TestRun> all_results;

    std::ofstream csv("augmented_graph_results.csv");

    csv
        << "graph_size,"
        << "run,"
        << "augmentation_ratio,"
        << "requested_loose_edges,"

        << "original_objective,"
        << "two_phase_objective,"
        << "interleaved_objective,"

        << "two_phase_improvement_vs_original,"
        << "interleaved_improvement_vs_original,"

        << "original_runtime_ms,"
        << "two_phase_runtime_ms,"
        << "interleaved_runtime_ms,"

        << "two_phase_runtime_ratio_vs_original,"
        << "interleaved_runtime_ratio_vs_original,"

        << "original_move_operations,"
        << "two_phase_move_operations,"
        << "interleaved_move_operations,"

        << "two_phase_replication_operations,"
        << "interleaved_replication_operations,"

        << "two_phase_final_replica_count,"
        << "interleaved_final_replica_count,"

        << "original_firstfit_objective,"
        << "two_phase_firstfit_objective,"
        << "interleaved_firstfit_objective,"

        << "original_firstfit_improvement_vs_original,"
        << "two_phase_firstfit_improvement_vs_original,"
        << "interleaved_firstfit_improvement_vs_original,"
        << "two_phase_firstfit_improvement_vs_original_firstfit,"
        << "interleaved_firstfit_improvement_vs_original_firstfit,"

        << "original_firstfit_runtime_ms,"
        << "two_phase_firstfit_runtime_ms,"
        << "interleaved_firstfit_runtime_ms,"

        << "original_firstfit_runtime_ratio_vs_original,"
        << "two_phase_firstfit_runtime_ratio_vs_original,"
        << "interleaved_firstfit_runtime_ratio_vs_original,"
        << "two_phase_firstfit_runtime_ratio_vs_original_firstfit,"
        << "interleaved_firstfit_runtime_ratio_vs_original_firstfit,"

        << "original_firstfit_move_operations,"
        << "two_phase_firstfit_move_operations,"
        << "interleaved_firstfit_move_operations,"

        << "two_phase_firstfit_replication_operations,"
        << "interleaved_firstfit_replication_operations,"

        << "two_phase_firstfit_final_replica_count,"
        << "interleaved_firstfit_final_replica_count"

        << "\n";


    for (int size = FROM; size <= TO; size += STEP) {

        std::cout << std::endl;
        std::cout << "======================================" << std::endl;
        std::cout << "Graph size: " << size << std::endl;
        std::cout << "======================================" << std::endl;

        for (double augmentation_ratio : AUGMENTATION_RATIOS) {

            size_t requested_loose_edges =
                static_cast<size_t>(std::ceil(size * augmentation_ratio));

            if (requested_loose_edges == 0 && augmentation_ratio != 0.0) {
                requested_loose_edges = 1;
            }

            std::cout
                << "Graph size: " << size
                << ", requested loose edges: " << requested_loose_edges
                << std::endl;

            for (int run = 0; run < RUNS; ++run) {
                if (augmentation_ratio == 0.0) {
                    system.replace_graph(
                        generate_random_series_parallel_graph(
                            size,
                            DATA_IN_MB
                        )
                    );
                }
                else {
                    system.replace_graph(
                        generate_random_almost_series_parallel_graph(
                            size,
                            DATA_IN_MB,
                            requested_loose_edges
                        )
                    );
                }

                TestRun test_run;

                SeriesParallelDecompositionMapper<
                    BasePoliciesForSharedDecomposition
                > decomposition_mapper;

                Decomposition decomposition =
                    decomposition_mapper.get_decomposition(
                        system.get_task_graph()
                    );

                run_shared_decomposition_mappings(
                    system,
                    test_run,
                    decomposition,
                    false,
                    false
                );


                TestResult const* original =
                    find_result(
                        test_run,
                        "SeriesParallelMapping"
                    );

                TestResult const* original_firstfit =
                    find_result(
                        test_run,
                        "SPFirstFitMapping"
                    );

                TestResult const* two_phase =
                    find_result(
                        test_run,
                        "SeriesParallelTwoPhaseReplicationMapping"
                    );

                TestResult const* two_phase_firstfit =
                    find_result(
                        test_run,
                        "SeriesParallelTwoPhaseReplicationFirstFitMapping"
                    );

                TestResult const* interleaved =
                    find_result(
                        test_run,
                        "SeriesParallelInterleavedReplicationMapping"
                    );

                TestResult const* interleaved_firstfit =
                    find_result(
                        test_run,
                        "SeriesParallelInterleavedReplicationFirstFitMapping"
                    );


                if (original != nullptr &&
                    original_firstfit != nullptr &&
                    two_phase != nullptr &&
                    two_phase_firstfit != nullptr &&
                    interleaved != nullptr &&
                    interleaved_firstfit != nullptr &&

                    !original->timeout &&
                    !original_firstfit->timeout &&
                    !two_phase->timeout &&
                    !two_phase_firstfit->timeout &&
                    !interleaved->timeout &&
                    !interleaved_firstfit->timeout)
                {
                    double two_phase_improvement_vs_original = 0.0;
                    double interleaved_improvement_vs_original = 0.0;

                    double original_firstfit_improvement_vs_original = 0.0;

                    double two_phase_firstfit_improvement_vs_original = 0.0;
                    double interleaved_firstfit_improvement_vs_original = 0.0;

                    double two_phase_firstfit_improvement_vs_original_firstfit = 0.0;
                    double interleaved_firstfit_improvement_vs_original_firstfit = 0.0;


                    if (original->objective != 0) {
                        two_phase_improvement_vs_original =
                            (original->objective - two_phase->objective)
                            / original->objective;

                        interleaved_improvement_vs_original =
                            (original->objective - interleaved->objective)
                            / original->objective;

                        original_firstfit_improvement_vs_original =
                            (original->objective - original_firstfit->objective)
                            / original->objective;

                        two_phase_firstfit_improvement_vs_original =
                            (original->objective - two_phase_firstfit->objective)
                            / original->objective;

                        interleaved_firstfit_improvement_vs_original =
                            (original->objective - interleaved_firstfit->objective)
                            / original->objective;
                    }


                    if (original_firstfit->objective != 0) {
                        two_phase_firstfit_improvement_vs_original_firstfit =
                            (original_firstfit->objective - two_phase_firstfit->objective)
                            / original_firstfit->objective;

                        interleaved_firstfit_improvement_vs_original_firstfit =
                            (original_firstfit->objective - interleaved_firstfit->objective)
                            / original_firstfit->objective;
                    }


                    double original_runtime =
                        static_cast<double>(original->runtime_ms.count());

                    double original_firstfit_runtime =
                        static_cast<double>(original_firstfit->runtime_ms.count());


                    double two_phase_runtime_ratio_vs_original = 0.0;
                    double interleaved_runtime_ratio_vs_original = 0.0;

                    double original_firstfit_runtime_ratio_vs_original = 0.0;

                    double two_phase_firstfit_runtime_ratio_vs_original = 0.0;
                    double interleaved_firstfit_runtime_ratio_vs_original = 0.0;

                    double two_phase_firstfit_runtime_ratio_vs_original_firstfit = 0.0;
                    double interleaved_firstfit_runtime_ratio_vs_original_firstfit = 0.0;


                    if (original_runtime != 0.0) {
                        two_phase_runtime_ratio_vs_original =
                            two_phase->runtime_ms.count()
                            / original_runtime;

                        interleaved_runtime_ratio_vs_original =
                            interleaved->runtime_ms.count()
                            / original_runtime;

                        original_firstfit_runtime_ratio_vs_original =
                            original_firstfit->runtime_ms.count()
                            / original_runtime;

                        two_phase_firstfit_runtime_ratio_vs_original =
                            two_phase_firstfit->runtime_ms.count()
                            / original_runtime;

                        interleaved_firstfit_runtime_ratio_vs_original =
                            interleaved_firstfit->runtime_ms.count()
                            / original_runtime;
                    }


                    if (original_firstfit_runtime != 0.0) {
                        two_phase_firstfit_runtime_ratio_vs_original_firstfit =
                            two_phase_firstfit->runtime_ms.count()
                            / original_firstfit_runtime;

                        interleaved_firstfit_runtime_ratio_vs_original_firstfit =
                            interleaved_firstfit->runtime_ms.count()
                            / original_firstfit_runtime;
                    }


                    /*
                     * The MOVE-only phase of TWO_PHASES follows the
                     * corresponding Series-Parallel search.
                     *
                     * Therefore:
                     * - original MOVE count is taken from normal TWO_PHASES
                     * - original FirstFit MOVE count is taken from
                     *   FirstFit TWO_PHASES
                     */
                    size_t original_move_operations =
                        two_phase->move_count;

                    size_t original_firstfit_move_operations =
                        two_phase_firstfit->move_count;


                    csv
                        << size << ","
                        << run + 1 << ","
                        << augmentation_ratio << ","
                        << requested_loose_edges << ","

                        << original->objective << ","
                        << two_phase->objective << ","
                        << interleaved->objective << ","

                        << two_phase_improvement_vs_original << ","
                        << interleaved_improvement_vs_original << ","

                        << original->runtime_ms.count() << ","
                        << two_phase->runtime_ms.count() << ","
                        << interleaved->runtime_ms.count() << ","

                        << two_phase_runtime_ratio_vs_original << ","
                        << interleaved_runtime_ratio_vs_original << ","

                        << original_move_operations << ","
                        << two_phase->move_count << ","
                        << interleaved->move_count << ","

                        << two_phase->replication_count << ","
                        << interleaved->replication_count << ","

                        << two_phase->final_replica_count << ","
                        << interleaved->final_replica_count << ","

                        << original_firstfit->objective << ","
                        << two_phase_firstfit->objective << ","
                        << interleaved_firstfit->objective << ","

                        << original_firstfit_improvement_vs_original << ","
                        << two_phase_firstfit_improvement_vs_original << ","
                        << interleaved_firstfit_improvement_vs_original << ","
                        << two_phase_firstfit_improvement_vs_original_firstfit << ","
                        << interleaved_firstfit_improvement_vs_original_firstfit << ","

                        << original_firstfit->runtime_ms.count() << ","
                        << two_phase_firstfit->runtime_ms.count() << ","
                        << interleaved_firstfit->runtime_ms.count() << ","

                        << original_firstfit_runtime_ratio_vs_original << ","
                        << two_phase_firstfit_runtime_ratio_vs_original << ","
                        << interleaved_firstfit_runtime_ratio_vs_original << ","
                        << two_phase_firstfit_runtime_ratio_vs_original_firstfit << ","
                        << interleaved_firstfit_runtime_ratio_vs_original_firstfit << ","

                        << original_firstfit_move_operations << ","
                        << two_phase_firstfit->move_count << ","
                        << interleaved_firstfit->move_count << ","

                        << two_phase_firstfit->replication_count << ","
                        << interleaved_firstfit->replication_count << ","

                        << two_phase_firstfit->final_replica_count << ","
                        << interleaved_firstfit->final_replica_count

                        << "\n";

                    csv.flush();
                }

                all_results.push_back(std::move(test_run));
            }
        }
    }


    csv.close();

    std::cout << std::endl;
    std::cout << "Results saved to augmented_graph_results.csv" << std::endl;

    std::cout << std::endl;
    std::cout << "======================================" << std::endl;
    std::cout << "Evaluation Finished" << std::endl;
    std::cout << "======================================" << std::endl;

    return 0;
}