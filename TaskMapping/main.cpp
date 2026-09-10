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


int main()
{
    const int SEED = 1;
    const int DATA_IN_MB = 100;

    const int FROM = 10;
    const int STEP = 10;
    const int TO = 200;
    const int RUNS = 5;

    srand(SEED);

    std::cout << "======================================" << std::endl;
    std::cout << "SP Replication Evaluation" << std::endl;
    std::cout << "======================================" << std::endl;

    ComputationBasedSystem system(
        TaskGraph(),
        create_platform(nbr_fpgas(Configuration::CGF))
    );

    std::vector<TestRun> all_results;

    std::ofstream csv("baseline_results.csv");

    csv
        << "graph_size,"
        << "run,"
        << "original_objective,"
        << "two_phase_objective,"
        << "interleaved_objective,"
        << "two_phase_improvement,"
        << "interleaved_improvement,"
        << "original_runtime_ms,"
        << "two_phase_runtime_ms,"
        << "interleaved_runtime_ms,"
        << "original_move_operations,"
        << "two_phase_move_operations,"
        << "interleaved_move_operations,"
        << "two_phase_replication_operations,"
        << "interleaved_replication_operations,"
        << "two_phase_final_replica_count,"
        << "interleaved_final_replica_count"
        << "\n";


    for (int size = FROM; size <= TO; size += STEP) {
        std::cout << "Graph size: " << size << std::endl;

        for (int run = 0; run < RUNS; ++run) {
            system.replace_graph(
                generate_random_series_parallel_graph(size, DATA_IN_MB)
            );

            TestRun test_run;

            run_mappings(
                system,
                test_run,
                {
                    MappingType::CPU,
                    MappingType::SeriesParallel,
                    MappingType::SeriesParallelTwoPhaseReplication,
                    MappingType::SeriesParallelInterleavedReplication
                },
                false,
                false
            );


            TestResult const* original =
                find_result(
                    test_run,
                    "SeriesParallelMapping"
                );

            TestResult const* two_phase =
                find_result(
                    test_run,
                    "SeriesParallelTwoPhaseReplicationMapping"
                );

            TestResult const* interleaved =
                find_result(
                    test_run,
                    "SeriesParallelInterleavedReplicationMapping"
                );


            if (original != nullptr &&
                two_phase != nullptr &&
                interleaved != nullptr &&
                !original->timeout &&
                !two_phase->timeout &&
                !interleaved->timeout)
            {
                double two_phase_improvement = 0.0;
                double interleaved_improvement = 0.0;

                if (original->objective != 0) {
                    two_phase_improvement =
                        (original->objective - two_phase->objective)
                        / original->objective * 100.0;

                    interleaved_improvement =
                        (original->objective - interleaved->objective)
                        / original->objective * 100.0;
                }


                /*
                 * The MOVE-only phase of TWO_PHASES follows the
                 * original Series-Parallel search. We therefore use
                 * its MOVE count for the original algorithm as well.
                 */
                size_t original_move_operations =
                    two_phase->move_count;


                csv
                    << size << ","
                    << run + 1 << ","

                    << original->objective << ","
                    << two_phase->objective << ","
                    << interleaved->objective << ","

                    << two_phase_improvement << ","
                    << interleaved_improvement << ","

                    << original->runtime_ms.count() << ","
                    << two_phase->runtime_ms.count() << ","
                    << interleaved->runtime_ms.count() << ","

                    << original_move_operations << ","
                    << two_phase->move_count << ","
                    << interleaved->move_count << ","

                    << two_phase->replication_count << ","
                    << interleaved->replication_count << ","

                    << two_phase->final_replica_count << ","
                    << interleaved->final_replica_count

                    << "\n";
            }

            all_results.push_back(std::move(test_run));
        }
    }


    csv.close();

    std::cout << std::endl;
    std::cout << "Results saved to baseline_results.csv" << std::endl;

    std::cout << std::endl;
    std::cout << "======================================" << std::endl;
    std::cout << "Evaluation Finished" << std::endl;
    std::cout << "======================================" << std::endl;

    return 0;
}