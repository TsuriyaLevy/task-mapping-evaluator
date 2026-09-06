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


int main()
{
    const int SEED = 1;
    const int DATA_IN_MB = 100;

    const int FROM = 10;
    const int STEP = 10;
    const int TO = 100;
    const int RUNS = 5;

    srand(SEED);

    std::cout << "======================================" << std::endl;
    std::cout << "SP vs SP + Replication Evaluation" << std::endl;
    std::cout << "======================================" << std::endl;

    ComputationBasedSystem system(
        TaskGraph(),
        create_platform(nbr_fpgas(Configuration::CGF))
    );

    std::vector<TestRun> all_results;

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
                    MappingType::SeriesParallelReplication
                },
                false,
                false
            );

            all_results.push_back(std::move(test_run));
        }
    }

    print_replication_statistics(all_results);

    std::cout << std::endl;
    std::cout << "======================================" << std::endl;
    std::cout << "Evaluation Finished" << std::endl;
    std::cout << "======================================" << std::endl;

    return 0;
}