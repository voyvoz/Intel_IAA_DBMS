#include "sm_join.hpp"

int main() {
    qpl_path_t execution_path = qpl_path_hardware;
    const size_t L_size = 200000;
    const size_t R_size = 20000;
    
    const size_t num_threads = 1;
    const size_t num_jobs = 1; 

    // Generate sorted input data
    std::vector<uint32_t> L(L_size), R(R_size);
    for (size_t i = 0; i < L_size; i++) L[i] = i * 2;
    for (size_t i = 0; i < R_size; i++) R[i] = i * 2;
    
    std::vector<uint32_t> joined_cpu, joined_iaa, joined_iaac, joined_cpu_parallel;
    joined_cpu.reserve(std::min(L_size, R_size));
    joined_iaa.reserve(std::min(L_size, R_size));
    joined_iaac.reserve(std::min(L_size, R_size));
    joined_cpu_parallel.reserve(std::min(L_size, R_size));

    // Benchmark CPU Sort-Merge Join
    auto start_cpu = std::chrono::high_resolution_clock::now();
    sort_merge_join_cpu(L, R, joined_cpu);
    auto end_cpu = std::chrono::high_resolution_clock::now();
    std::cout << "CPU Sort-Merge Join:\t" << std::chrono::duration<double>(end_cpu - start_cpu).count() << " seconds\t" << joined_cpu.size() << " matches\n";

    // Initialize IAA Job
    uint32_t job_size = 0;
    qpl_get_job_size(execution_path, &job_size);
    std::unique_ptr<uint8_t[]> job_buffer = std::make_unique<uint8_t[]>(job_size);
    qpl_job* job = reinterpret_cast<qpl_job*>(job_buffer.get());
    qpl_init_job(execution_path, job);

    // Benchmark IAA Sort-Merge Join using op_scan and value extraction
    auto start_iaa = std::chrono::high_resolution_clock::now();
    sort_merge_join_iaa(execution_path, job, L, R, joined_iaa);
    auto end_iaa = std::chrono::high_resolution_clock::now();
    std::cout << "IAA Sort-Merge Join (op_scan + extract):\t" << std::chrono::duration<double>(end_iaa - start_iaa).count() << " seconds\t" << joined_iaa.size() << " matches\n";

    sort_merge_join_iaa_chunked(execution_path, L, R, joined_iaa, num_jobs);

    auto start_cpu_parallel = std::chrono::high_resolution_clock::now();
    sort_merge_join_cpu_parallel(L, R, joined_cpu_parallel, num_threads);
    auto end_cpu_parallel = std::chrono::high_resolution_clock::now();
    std::cout << "CPU Sort-Merge Join (Parallel):\t" << std::chrono::duration<double>(end_cpu_parallel - start_cpu_parallel).count() << " seconds\t" << joined_cpu_parallel.size() << " matches\n";

    std::vector<uint32_t> joined_hybrid;

    std::cout << "H";
    auto start_hybrid = std::chrono::high_resolution_clock::now();
    sort_merge_join_hybrid(execution_path, L, R, joined_hybrid, num_threads, num_jobs);
    auto end_hybrid = std::chrono::high_resolution_clock::now();
    std::cout << "Hybrid Sort-Merge Join:\t" << std::chrono::duration<double>(end_hybrid - start_hybrid).count() << " seconds\t" << joined_hybrid.size() << " matches\n";

    std::vector<uint32_t> joined_iaa_preallocated;

    sort_merge_join_iaa_preallocated(execution_path, L, R, joined_iaa_preallocated, num_jobs);

    return 0;
}