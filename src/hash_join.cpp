#include <iostream>
#include <vector>
#include <chrono>
#include <thread>
#include <unordered_map>
#include "utils.hpp"
#include "qpl/qpl.h"

#include "hash_join.hpp"

// CPU-based Hash Join
void hash_join_cpu(const std::vector<uint32_t>& L, const std::vector<uint32_t>& R, std::vector<uint32_t>& joined) {
    std::unordered_map<uint32_t, bool> hash_table;
    for (const auto& r_val : R) {
        hash_table[r_val] = true;
    }
    for (const auto& l_val : L) {
        if (hash_table.find(l_val) != hash_table.end()) {
            joined.push_back(l_val);
        }
    }
}

// IAA-based Hash Join
void hash_join_iaa(qpl_path_t execution_path, const std::vector<uint32_t>& L, const std::vector<uint32_t>& R,
                   std::vector<uint32_t>& joined) {
    uint32_t job_size = 0;
    qpl_get_job_size(execution_path, &job_size);
    std::unique_ptr<uint8_t[]> job_buffer = std::make_unique<uint8_t[]>(job_size);
    qpl_job* job = reinterpret_cast<qpl_job*>(job_buffer.get());
    qpl_init_job(execution_path, job);

    std::vector<uint64_t> hash_values(R.size());
    for (size_t i = 0; i < R.size(); i++) {
        job->op = qpl_op_crc64;
        job->next_in_ptr = reinterpret_cast<uint8_t*>(const_cast<void*>(static_cast<const void*>(&R[i])));
        job->available_in = sizeof(uint32_t);
        job->crc64_poly = poly;
        qpl_execute_job(job);
        hash_values[i] = job->crc64;
    }

    std::unordered_map<uint64_t, uint32_t> hash_table;
    for (size_t i = 0; i < R.size(); i++) {
        hash_table[hash_values[i]] = R[i];
    }

    for (const auto& l_val : L) {
        job->op = qpl_op_crc64;
        job->next_in_ptr = reinterpret_cast<uint8_t*>(const_cast<void*>(static_cast<const void*>(&l_val)));
        job->available_in = sizeof(uint32_t);
        job->crc64_poly = poly;
        qpl_execute_job(job);
        uint64_t hash = job->crc64;

        if (hash_table.find(hash) != hash_table.end()) {
            joined.push_back(hash_table[hash]);
        }
    }

    qpl_fini_job(job);
}

void hash_join_cpu_parallel(const std::vector<uint32_t>& L, const std::vector<uint32_t>& R, std::vector<uint32_t>& joined, size_t num_threads) {
    std::unordered_map<uint32_t, bool> hash_table;
    for (const auto& r_val : R) {
        hash_table[r_val] = true;
    }

    std::vector<std::thread> threads;
    std::vector<std::vector<uint32_t>> partial_results(num_threads);
    size_t chunk_size = L.size() / num_threads;

    auto worker = [&](size_t start, size_t end, std::vector<uint32_t>& local_joined) {
        for (size_t i = start; i < end; i++) {
            if (hash_table.find(L[i]) != hash_table.end()) {
                local_joined.push_back(L[i]);
            }
        }
    };

    for (size_t i = 0; i < num_threads; i++) {
        size_t start = i * chunk_size;
        size_t end = (i == num_threads - 1) ? L.size() : start + chunk_size;
        threads.emplace_back(worker, start, end, std::ref(partial_results[i]));
    }

    for (auto& t : threads) {
        t.join();
    }

    for (const auto& part : partial_results) {
        joined.insert(joined.end(), part.begin(), part.end());
    }
}

void hash_join_iaa_chunked(qpl_path_t execution_path, const std::vector<uint32_t>& L, const std::vector<uint32_t>& R,
                           std::vector<uint32_t>& joined, size_t num_jobs) {
    size_t chunk_size = L.size() / num_jobs;
    std::vector<std::thread> threads;
    std::vector<std::vector<uint32_t>> partial_results(num_jobs);

    // Preallocate and preconfigure jobs
    uint32_t job_size = 0;
    qpl_get_job_size(execution_path, &job_size);
    std::vector<std::unique_ptr<uint8_t[]>> job_buffers(num_jobs);
    std::vector<qpl_job*> jobs(num_jobs);
    
    for (size_t i = 0; i < num_jobs; i++) {
        job_buffers[i] = std::make_unique<uint8_t[]>(job_size);
        jobs[i] = reinterpret_cast<qpl_job*>(job_buffers[i].get());
        qpl_init_job(execution_path, jobs[i]);
        
        // Preconfigure jobs
        jobs[i]->op = qpl_op_crc64;
        jobs[i]->available_in = sizeof(uint32_t);
        jobs[i]->crc64_poly = poly;
    }

    // Build Phase: Hash R table asynchronously with preconfigured jobs
    std::unordered_map<uint64_t, uint32_t> hash_table;
    for (size_t i = 0; i < R.size(); i++) {
        jobs[i % num_jobs]->next_in_ptr = reinterpret_cast<uint8_t*>(const_cast<void*>(static_cast<const void*>(&R[i])));
        qpl_submit_job(jobs[i % num_jobs]);
    }
    

    auto worker = [&](size_t job_id, size_t start, size_t end, std::vector<uint32_t>& local_joined) {
        qpl_job* job = jobs[job_id];

        for (size_t i = start; i < end; i++) {
            job->next_in_ptr = reinterpret_cast<uint8_t*>(const_cast<void*>(static_cast<const void*>(&L[i])));
            qpl_submit_job(job);
        }

        for (size_t i = start; i < end; i++) {
            qpl_wait_job(job);
            uint64_t hash = job->crc64;
            if (hash_table.find(hash) != hash_table.end()) {
                local_joined.push_back(hash_table[hash]);
            }
        }
    };

    auto start_iaa_chunked = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < R.size(); i++) {
        qpl_wait_job(jobs[i % num_jobs]);
        hash_table[jobs[i % num_jobs]->crc64] = R[i];
    }

    
    for (size_t i = 0; i < num_jobs; i++) {
        size_t start = i * chunk_size;
        size_t end = (i == num_jobs - 1) ? L.size() : start + chunk_size;
        threads.emplace_back(worker, i, start, end, std::ref(partial_results[i]));
    }

    for (auto& t : threads) {
        t.join();
    }

    auto end_iaa_chunked = std::chrono::high_resolution_clock::now();
    std::cout << "IAA Chunked Hash Join:\t" << std::chrono::duration<double>(end_iaa_chunked - start_iaa_chunked).count() << " seconds\t" << partial_results.size() << " matches\n";


    for (const auto& part : partial_results) {
        joined.insert(joined.end(), part.begin(), part.end());
    }

    // Finalize jobs
    for (size_t i = 0; i < num_jobs; i++) {
        qpl_fini_job(jobs[i]);
    }
}

void hash_join_hybrid(qpl_path_t execution_path, const std::vector<uint32_t>& L, const std::vector<uint32_t>& R,
                      std::vector<uint32_t>& joined, size_t num_threads) {
    // Split the dataset into CPU and IAA processing
    size_t cpu_split = (L.size() * 2) / 3;
    size_t iaa_split = L.size() - cpu_split;

    std::vector<uint32_t> L_cpu(L.begin(), L.begin() + cpu_split);
    std::vector<uint32_t> L_iaa(L.begin() + cpu_split, L.end());

    std::vector<uint32_t> joined_cpu, joined_iaa;
    std::unordered_map<uint64_t, uint32_t> hash_table;

    // **Build Phase using IAA**
    uint32_t job_size = 0;
    qpl_get_job_size(execution_path, &job_size);
    std::unique_ptr<uint8_t[]> job_buffer = std::make_unique<uint8_t[]>(job_size);
    qpl_job* job = reinterpret_cast<qpl_job*>(job_buffer.get());
    qpl_init_job(execution_path, job);


    for (const auto& r_val : R) {
        job->op = qpl_op_crc64;
        job->next_in_ptr = reinterpret_cast<uint8_t*>(const_cast<void*>(static_cast<const void*>(&r_val)));
        job->available_in = sizeof(uint32_t);
        job->crc64_poly = poly;
        qpl_execute_job(job);
        hash_table[job->crc64] = r_val;
    }

    qpl_fini_job(job);

    // **Probe Phase: CPU and IAA run concurrently**
    auto cpu_worker = [&]() {
        for (const auto& l_val : L_cpu) {
            if (hash_table.find(l_val) != hash_table.end()) {
                joined_cpu.push_back(l_val);
            }
        }
    };

    auto iaa_worker = [&]() {
        uint32_t job_size = 0;
        qpl_get_job_size(execution_path, &job_size);
        std::unique_ptr<uint8_t[]> job_buffer = std::make_unique<uint8_t[]>(job_size);
        qpl_job* job = reinterpret_cast<qpl_job*>(job_buffer.get());
        qpl_init_job(execution_path, job);

        for (const auto& l_val : L_iaa) {
            job->op = qpl_op_crc64;
            job->next_in_ptr = reinterpret_cast<uint8_t*>(const_cast<void*>(static_cast<const void*>(&l_val)));
            job->available_in = sizeof(uint32_t);
            job->crc64_poly = poly;
            qpl_submit_job(job);
        }

        for (const auto& l_val : L_iaa) {
            qpl_wait_job(job);
            uint64_t hash = job->crc64;
            if (hash_table.find(hash) != hash_table.end()) {
                joined_iaa.push_back(hash_table[hash]);
            }
        }

        qpl_fini_job(job);
    };

    auto start_hybrid = std::chrono::high_resolution_clock::now();
    std::thread cpu_thread(cpu_worker);
    std::thread iaa_thread(iaa_worker);

    cpu_thread.join();
    iaa_thread.join();

    auto end_hybrid = std::chrono::high_resolution_clock::now();
    std::cout << "Hybrid Hash Join:\t" << std::chrono::duration<double>(end_hybrid - start_hybrid).count() << " seconds\t" << joined.size() << " matches\n";



    // Merge results
    joined.reserve(joined_cpu.size() + joined_iaa.size());
    joined.insert(joined.end(), joined_cpu.begin(), joined_cpu.end());
    joined.insert(joined.end(), joined_iaa.begin(), joined_iaa.end());
}