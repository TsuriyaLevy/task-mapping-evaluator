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
#include <filesystem>
#include <unordered_map>
#include <iomanip>
#include <sstream>


struct BasePoliciesForSharedDecomposition
{
    typedef EvaluateAll EvaluationPolicy;
    typedef GreedyBase BaseMappingPolicy;
};


std::string augmentation_folder_name(double augmentation_ratio)
{
    std::ostringstream stream;
    stream << "aug_" << static_cast<int>(std::round(augmentation_ratio * 100.0));
    return stream.str();
}


std::string run_folder_name(int run)
{
    std::ostringstream stream;
    stream << "run_" << std::setw(2) << std::setfill('0') << (run + 1);
    return stream.str();
}


std::unordered_map<Task*, size_t> build_task_ids(TaskGraph const& graph)
{
    std::unordered_map<Task*, size_t> task_ids;

    std::vector<Task*> const& tasks = graph.get_tasks();

    for (size_t i = 0; i < tasks.size(); ++i) {
        task_ids[tasks[i]] = i;
    }

    return task_ids;
}


void save_graph(
    TaskGraph const& graph,
    std::filesystem::path const& directory)
{
    std::filesystem::create_directories(directory);

    std::unordered_map<Task*, size_t> task_ids =
        build_task_ids(graph);

    std::ofstream tasks_file(directory / "tasks.csv");
    tasks_file
        << "task_id,complexity,parallelizability,streamability,"
        << "area_requirement,input_size_mb,output_size_mb\n";

    for (Task* task : graph.get_tasks()) {
        tasks_file
            << task_ids.at(task) << ","
            << task->get_complexity() << ","
            << task->get_parallelizability() << ","
            << task->get_streamability() << ","
            << task->get_area_requirement() << ","
            << task->get_input_size() << ","
            << task->get_output_size()
            << "\n";
    }

    std::ofstream edges_file(directory / "edges.csv");
    edges_file << "source_task_id,destination_task_id\n";

    for (Edge* edge : graph.get_edges()) {
        edges_file
            << task_ids.at(edge->get_src()) << ","
            << task_ids.at(edge->get_snk())
            << "\n";
    }
}


void save_mapping(
    TaskGraph const& graph,
    Mapping const& mapping,
    std::filesystem::path const& filename)
{
    std::unordered_map<Task*, size_t> task_ids =
        build_task_ids(graph);

    std::ofstream file(filename);
    file << "task_id,processor,input_memory,output_memory\n";

    for (Task* task : graph.get_tasks()) {
        Processor const* processor = mapping.get_processor(task);
        Memory const* mem_in = mapping.get_mem_in(task);
        Memory const* mem_out = mapping.get_mem_out(task);

        file
            << task_ids.at(task) << ","
            << (processor != nullptr ? processor->get_label() : "") << ","
            << (mem_in != nullptr ? mem_in->get_label() : "") << ","
            << (mem_out != nullptr ? mem_out->get_label() : "")
            << "\n";
    }
}


void save_multi_mapping(
    TaskGraph const& graph,
    MultiMapping const& mapping,
    std::filesystem::path const& filename)
{
    std::unordered_map<Task*, size_t> task_ids =
        build_task_ids(graph);

    std::ofstream file(filename);
    file << "task_id,replica_index,processor,input_memory,output_memory\n";

    for (Task* task : graph.get_tasks()) {
        size_t replica_index = 0;

        for (auto const& replica : mapping.get_replicas(task)) {
            file
                << task_ids.at(task) << ","
                << replica_index++ << ","
                << replica.processor->get_label() << ","
                << replica.memory_in->get_label() << ","
                << replica.memory_out->get_label()
                << "\n";
        }
    }
}


void save_edge_case_metadata(
    std::filesystem::path const& directory,
    int size,
    int run,
    double augmentation_ratio,
    size_t requested_loose_edges,
    std::string const& comparison,
    double improvement,
    TestResult const& baseline,
    TestResult const& replication)
{
    std::ofstream file(directory / "metadata.csv");

    file
        << "graph_size,run,augmentation_ratio,requested_loose_edges,"
        << "comparison,improvement,baseline_objective,replication_objective\n";

    file
        << size << ","
        << run + 1 << ","
        << augmentation_ratio << ","
        << requested_loose_edges << ","
        << comparison << ","
        << improvement << ","
        << baseline.objective << ","
        << replication.objective
        << "\n";
}


void save_edge_case(
    TaskGraph const& graph,
    std::filesystem::path const& edge_cases_root,
    int size,
    int run,
    double augmentation_ratio,
    size_t requested_loose_edges,
    std::string const& comparison,
    double improvement,
    TestResult const& baseline_result,
    TestResult const& replication_result,
    Mapping const& baseline_mapping,
    MultiMapping const& replication_mapping)
{
    std::string direction =
        improvement > 0.0 ? "positive" : "negative";

    std::filesystem::path directory =
        edge_cases_root
        / direction
        / ("size_" + std::to_string(size))
        / augmentation_folder_name(augmentation_ratio)
        / run_folder_name(run)
        / comparison;

    std::filesystem::create_directories(directory);

    save_graph(graph, directory);
    save_mapping(
        graph,
        baseline_mapping,
        directory / "baseline_mapping.csv"
    );
    save_multi_mapping(
        graph,
        replication_mapping,
        directory / "replication_mapping.csv"
    );

    save_edge_case_metadata(
        directory,
        size,
        run,
        augmentation_ratio,
        requested_loose_edges,
        comparison,
        improvement,
        baseline_result,
        replication_result
    );
}


void write_breakdown_header(
    std::ofstream& csv,
    std::string const& prefix)
{
    csv
        << prefix << "_computation_work,"
        << prefix << "_processor_memory_communication_work,"
        << prefix << "_inter_task_communication_work,"
        << prefix << "_total_communication_work,"
        << prefix << "_inter_memory_data_mb,";
}


void write_breakdown(
    std::ofstream& csv,
    TestResult const& result,
    bool trailing_comma = true)
{
    csv
        << result.computation_work << ","
        << result.processor_memory_communication_work << ","
        << result.inter_task_communication_work << ","
        << result.total_communication_work << ","
        << result.inter_memory_data_mb;

    if (trailing_comma) {
        csv << ",";
    }
}


int main()
{
    const int SEED = 1;
    const int DATA_IN_MB = 100;

    const int FROM = 10;
    const int STEP = 10;
    const int TO = 350;
    const int RUNS = 10;

    const double EDGE_CASE_THRESHOLD = 0.10;

    const std::vector<double> AUGMENTATION_RATIOS =
    { 0.00, 0.05, 0.10, 0.20 };

    const std::filesystem::path OUTPUT_ROOT =
        "replication_experiment_detailed";

    const std::filesystem::path GRAPHS_ROOT =
        OUTPUT_ROOT / "graphs";

    const std::filesystem::path EDGE_CASES_ROOT =
        OUTPUT_ROOT / "edge_cases";

    const std::filesystem::path RESULTS_FILE =
        OUTPUT_ROOT / "replication_experiment_detailed.csv";

    std::filesystem::create_directories(GRAPHS_ROOT);
    std::filesystem::create_directories(EDGE_CASES_ROOT);

    srand(SEED);

    std::cout << "======================================" << std::endl;
    std::cout << "SP / Almost-SP Replication Evaluation" << std::endl;
    std::cout << "======================================" << std::endl;

    ComputationBasedSystem system(
        TaskGraph(),
        create_platform(nbr_fpgas(Configuration::CGF))
    );

    std::vector<TestRun> all_results;

    std::ofstream csv(RESULTS_FILE);

    csv
        << "graph_size,"
        << "run,"
        << "augmentation_ratio,"
        << "requested_loose_edges,"

        << "original_objective,"
        << "two_phase_objective,"
        << "interleaved_objective,";

    write_breakdown_header(csv, "original");
    write_breakdown_header(csv, "two_phase");
    write_breakdown_header(csv, "interleaved");

    csv
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
        << "interleaved_firstfit_objective,";

    write_breakdown_header(csv, "original_firstfit");
    write_breakdown_header(csv, "two_phase_firstfit");
    write_breakdown_header(csv, "interleaved_firstfit");

    csv
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
                static_cast<size_t>(
                    std::ceil(size * augmentation_ratio)
                    );

            if (requested_loose_edges == 0 &&
                augmentation_ratio != 0.0)
            {
                requested_loose_edges = 1;
            }

            std::cout
                << "Graph size: " << size
                << ", requested loose edges: "
                << requested_loose_edges
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

                TaskGraph const& graph =
                    system.get_task_graph();

                std::filesystem::path graph_directory =
                    GRAPHS_ROOT
                    / ("size_" + std::to_string(size))
                    / augmentation_folder_name(augmentation_ratio)
                    / run_folder_name(run);

                save_graph(
                    graph,
                    graph_directory
                );

                TestRun test_run;

                SeriesParallelDecompositionMapper<
                    BasePoliciesForSharedDecomposition
                > decomposition_mapper;

                Decomposition decomposition =
                    decomposition_mapper.get_decomposition(
                        graph
                    );

                SharedDecompositionFinalMappings final_mappings;

                run_shared_decomposition_mappings(
                    system,
                    test_run,
                    decomposition,
                    false,
                    false,
                    &final_mappings
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
                        static_cast<double>(
                            original->runtime_ms.count()
                            );

                    double original_firstfit_runtime =
                        static_cast<double>(
                            original_firstfit->runtime_ms.count()
                            );


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
                        << interleaved->objective << ",";

                    write_breakdown(csv, *original);
                    write_breakdown(csv, *two_phase);
                    write_breakdown(csv, *interleaved);

                    csv
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
                        << interleaved_firstfit->objective << ",";

                    write_breakdown(csv, *original_firstfit);
                    write_breakdown(csv, *two_phase_firstfit);
                    write_breakdown(csv, *interleaved_firstfit);

                    csv
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


                    if (std::abs(two_phase_improvement_vs_original) >
                        EDGE_CASE_THRESHOLD)
                    {
                        save_edge_case(
                            graph,
                            EDGE_CASES_ROOT,
                            size,
                            run,
                            augmentation_ratio,
                            requested_loose_edges,
                            "two_phase_vs_original",
                            two_phase_improvement_vs_original,
                            *original,
                            *two_phase,
                            final_mappings.original,
                            final_mappings.two_phase
                        );
                    }

                    if (std::abs(interleaved_improvement_vs_original) >
                        EDGE_CASE_THRESHOLD)
                    {
                        save_edge_case(
                            graph,
                            EDGE_CASES_ROOT,
                            size,
                            run,
                            augmentation_ratio,
                            requested_loose_edges,
                            "interleaved_vs_original",
                            interleaved_improvement_vs_original,
                            *original,
                            *interleaved,
                            final_mappings.original,
                            final_mappings.interleaved
                        );
                    }

                    if (std::abs(
                        two_phase_firstfit_improvement_vs_original_firstfit
                    ) > EDGE_CASE_THRESHOLD)
                    {
                        save_edge_case(
                            graph,
                            EDGE_CASES_ROOT,
                            size,
                            run,
                            augmentation_ratio,
                            requested_loose_edges,
                            "two_phase_firstfit_vs_original_firstfit",
                            two_phase_firstfit_improvement_vs_original_firstfit,
                            *original_firstfit,
                            *two_phase_firstfit,
                            final_mappings.original_firstfit,
                            final_mappings.two_phase_firstfit
                        );
                    }

                    if (std::abs(
                        interleaved_firstfit_improvement_vs_original_firstfit
                    ) > EDGE_CASE_THRESHOLD)
                    {
                        save_edge_case(
                            graph,
                            EDGE_CASES_ROOT,
                            size,
                            run,
                            augmentation_ratio,
                            requested_loose_edges,
                            "interleaved_firstfit_vs_original_firstfit",
                            interleaved_firstfit_improvement_vs_original_firstfit,
                            *original_firstfit,
                            *interleaved_firstfit,
                            final_mappings.original_firstfit,
                            final_mappings.interleaved_firstfit
                        );
                    }
                }

                all_results.push_back(
                    std::move(test_run)
                );
            }
        }
    }


    csv.close();

    std::cout << std::endl;
    std::cout
        << "Results saved to "
        << RESULTS_FILE.string()
        << std::endl;

    std::cout
        << "Graphs saved to "
        << GRAPHS_ROOT.string()
        << std::endl;

    std::cout
        << "Edge cases saved to "
        << EDGE_CASES_ROOT.string()
        << std::endl;

    std::cout << std::endl;
    std::cout << "======================================" << std::endl;
    std::cout << "Evaluation Finished" << std::endl;
    std::cout << "======================================" << std::endl;

    return 0;
}
