#include "nl_join.hpp"

int main() {
    qpl_path_t execution_path = qpl_path_hardware;
    const size_t L_size = 200;
    const size_t R_size = 20000;
    

    std::vector<uint32_t> L(L_size);
    std::vector<uint32_t> R(R_size);

    for (size_t i = 0; i < L_size; i++) L[i] = 3;
    for (size_t i = 0; i < R_size; i++) R[i] = 4;

    L[0] = 4;

    std::vector<uint32_t> joined_sync, joined_async, joined_naive;

    std::cout << "Alloc done" << std::endl;

    std::cout << "\n================= Benchmark Results =================\n";
    std::cout << "Method\t\tTime (seconds)\tMatches Found\n";
    std::cout << "-----------------------------------------------------\n";

    uint32_t job_size = 0;
    qpl_status status = qpl_get_job_size(execution_path, &job_size);
    if (status != QPL_STS_OK) {
        std::cerr << "Error getting job size: " << status << "\n";
        return 0;
    }

    std::vector<uint32_t> joined_multi;
    std::vector<std::unique_ptr<uint8_t[]>> job_buffers(L.size());
    std::vector<qpl_job*> jobs(L.size());
    std::vector<std::vector<uint8_t>> bit_vectors(L.size(), std::vector<uint8_t>((R.size()), 0));
    std::vector<std::vector<uint32_t>> extracted_values(L.size(), std::vector<uint32_t>(R.size(), 0));

    for(int i = 0; i < L.size(); i++) {
        job_buffers[i] = std::make_unique<uint8_t[]>(job_size);
        jobs[i] = reinterpret_cast<qpl_job*>(job_buffers[i].get());
        qpl_init_job(execution_path, jobs[i]);
    }

    std::cout << "Job Alloc done" << std::endl;

    std::unique_ptr<uint8_t[]> job_buffer_sync = std::make_unique<uint8_t[]>(job_size);
    if (!job_buffer_sync) {
        std::cerr << "Error: Failed to allocate job buffer for sync." << std::endl;
        return 0;
    }

    qpl_job* job_sync = reinterpret_cast<qpl_job*>(job_buffer_sync.get());
    status = qpl_init_job(execution_path, job_sync);
    
    if (status != QPL_STS_OK) {
        std::cerr << "Error initializing QPL job: " << status << "\n";
        return 0 ;
    }

    std::cout << "Start bench" << std::endl;

    const size_t num_threads = 8;

    size_t bit_vector_size = (R.size() + 7) / 8 * 8;
    std::vector<uint8_t> bit_vector_sync(bit_vector_size, 0);
    std::vector<uint32_t> extracted_values_sync(R.size(), 0);

    auto start_sync = std::chrono::high_resolution_clock::now();
    iaa_nested_loop_join_sync(execution_path, job_sync, bit_vector_sync, L, R, joined_sync);
    auto end_sync = std::chrono::high_resolution_clock::now();
    std::cout << "IAA Sync\t" << std::chrono::duration<double>(end_sync - start_sync).count() << "\t" << joined_sync.size() << "\n";

    auto start_async = std::chrono::high_resolution_clock::now();
    iaa_nested_loop_join_async(execution_path, jobs, bit_vectors, L, R, joined_async);
    auto end_async = std::chrono::high_resolution_clock::now();
    std::cout << "IAA Async\t" << std::chrono::duration<double>(end_async - start_async).count() << "\t" << joined_async.size() << "\n";

    auto start_naive = std::chrono::high_resolution_clock::now();
    nested_loop_join(L, R, joined_naive);
    auto end_naive = std::chrono::high_resolution_clock::now();
    std::cout << "Naive\t\t" << std::chrono::duration<double>(end_naive - start_naive).count() << "\t" << joined_naive.size() << "\n";

    iaa_nested_loop_join_multi(execution_path, L, R, joined_multi, num_threads);
    
    std::vector<uint32_t> joined_parallel;
    
    auto start_parallel = std::chrono::high_resolution_clock::now();
    nested_loop_join_parallel(L, R, joined_parallel, num_threads);
    auto end_parallel = std::chrono::high_resolution_clock::now();

    hybrid_nested_loop_join(execution_path, L, R, joined_multi, num_threads);


    std::cout << "Multithreaded Nested Loop Join Completed!\n";
    std::cout << "Time Taken: " << std::chrono::duration<double>(end_parallel - start_parallel).count() << " seconds\n";
    std::cout << "Matches Found: " << joined_parallel.size() << "\n";
    
    std::vector<uint32_t> joined_iaap;

    iaa_nested_loop_join_preallocated(execution_path, L, R, joined_iaap, 7);
    
    return 0;
}