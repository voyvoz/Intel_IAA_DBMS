#include <chrono>
#include <iostream>

#include "hash_join.hpp"

int main() {
    qpl_path_t execution_path = qpl_path_hardware;
    const size_t L_size = 200;
    const size_t R_size = 20000;
    const size_t num_threads = 1;
    
    std::vector<uint32_t> L(L_size), R(R_size);
    for (size_t i = 0; i < L_size; i++) L[i] = i * 2;
    for (size_t i = 0; i < R_size; i++) R[i] = i * 3;
    
    std::vector<uint32_t> joined_cpu, joined_iaa, joined_cpu_parallel, joined_iaa_chunked;

    auto start_cpu = std::chrono::high_resolution_clock::now();
    hash_join_cpu(L, R, joined_cpu);
    auto end_cpu = std::chrono::high_resolution_clock::now();
    std::cout << "CPU Hash Join:\t" << std::chrono::duration<double>(end_cpu - start_cpu).count() << " seconds\t" << joined_cpu.size() << " matches\n";

    auto start_iaa = std::chrono::high_resolution_clock::now();
    hash_join_iaa(execution_path, L, R, joined_iaa);
    auto end_iaa = std::chrono::high_resolution_clock::now();
    std::cout << "IAA Hash Join:\t" << std::chrono::duration<double>(end_iaa - start_iaa).count() << " seconds\t" << joined_iaa.size() << " matches\n";

    auto start_cpu_parallel = std::chrono::high_resolution_clock::now();
    hash_join_cpu_parallel(L, R, joined_cpu_parallel, num_threads);
    auto end_cpu_parallel = std::chrono::high_resolution_clock::now();
    std::cout << "CPU Parallel Hash Join:\t" << std::chrono::duration<double>(end_cpu_parallel - start_cpu_parallel).count() << " seconds\t" << joined_cpu_parallel.size() << " matches\n";

    hash_join_iaa_chunked(execution_path, L, R, joined_iaa_chunked, num_threads);

    std::vector<uint32_t> joined_hybrid;

    hash_join_hybrid(execution_path, L, R, joined_hybrid, num_threads);

    return 0;
}