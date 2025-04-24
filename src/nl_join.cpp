#include <iostream>
#include <vector>
#include <memory>
#include <chrono>
#include <iomanip>  
#include "utils.hpp"
#include "qpl/qpl.h"
#include <thread>
#include <mutex>

#include "nl_join.hpp"

void iaa_nested_loop_join_preallocated(qpl_path_t execution_path, std::vector<uint32_t>& L,
                                       std::vector<uint32_t>& R, std::vector<uint32_t>& joined,
                                       size_t num_threads) {
    size_t chunk_size = L.size() / num_threads;
    std::vector<std::thread> threads;
    std::vector<std::vector<uint32_t>> partial_results(num_threads);

    // Preallocate and preconfigure jobs
    uint32_t job_size = 0;
    qpl_get_job_size(execution_path, &job_size);
    std::vector<std::unique_ptr<uint8_t[]>> job_buffers(num_threads);
    std::vector<qpl_job*> jobs(num_threads);
    std::vector<std::vector<uint8_t>> bit_vectors(num_threads, std::vector<uint8_t>((R.size() + 7) / 8 * 8, 0));

    for (size_t i = 0; i < num_threads; i++) {
        job_buffers[i] = std::make_unique<uint8_t[]>(job_size);
        jobs[i] = reinterpret_cast<qpl_job*>(job_buffers[i].get());
        qpl_init_job(execution_path, jobs[i]);

        // Preconfigure jobs
        jobs[i]->next_in_ptr = const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(R.data()));
        jobs[i]->available_in = R.size() * sizeof(uint32_t);
        jobs[i]->next_out_ptr = bit_vectors[i].data();
        jobs[i]->num_input_elements = R.size();
        jobs[i]->available_out = bit_vectors[i].size();
        jobs[i]->src1_bit_width = 32;
        jobs[i]->op = qpl_op_scan_eq;
        jobs[i]->out_bit_width = qpl_ow_nom;
    }

    auto worker = [&](size_t job_id, size_t start, size_t end, std::vector<uint32_t>& local_joined) {
        qpl_job* job = jobs[job_id];
        auto& bit_vector = bit_vectors[job_id];

        for (size_t i = start; i < end; i++) {
            job->param_low = L[i];
            qpl_submit_job(job);
        }

        for (size_t i = start; i < end; i++) {
            qpl_wait_job(job);
            uint32_t bitmask_size = job->total_out;
            for (size_t j = 0; j < R.size(); j++) {
                if (bit_vector[j / 8] & (1 << (j % 8))) {
                    local_joined.push_back(R[j]);
                }
            }
        }
    };

    auto start_iaap = std::chrono::high_resolution_clock::now();
    
    for (size_t i = 0; i < num_threads; i++) {
        size_t start = i * chunk_size;
        size_t end = (i == num_threads - 1) ? L.size() : start + chunk_size;
        threads.emplace_back(worker, i, start, end, std::ref(partial_results[i]));
    }

    for (auto& t : threads) {
        t.join();
    }

    auto end_iaap = std::chrono::high_resolution_clock::now();
    std::cout << "IAA Preallocated Nested Loop Join:	" << std::chrono::duration<double>(end_iaap - start_iaap).count() << " seconds\t" << partial_results.size() << " matches\n";

    
    for (const auto& part : partial_results) {
        joined.insert(joined.end(), part.begin(), part.end());
    }

    for (size_t i = 0; i < num_threads; i++) {
        qpl_fini_job(jobs[i]);
    }
}

void nested_loop_worker(const std::vector<uint32_t>& L, size_t start, size_t end, 
                        const std::vector<uint32_t>& R, std::vector<uint32_t>& local_results) {
    for (size_t i = start; i < end; ++i) {
        uint32_t l_val = L[i];
        for (uint32_t r_val : R) {
            if (l_val == r_val) {
                local_results.push_back(r_val);
            }
        }
    }
}

void nested_loop_join_parallel(const std::vector<uint32_t>& L, const std::vector<uint32_t>& R, 
                               std::vector<uint32_t>& joined, size_t num_threads) {
    std::vector<std::thread> threads;
    std::vector<std::vector<uint32_t>> thread_results(num_threads); // Each thread has its own results buffer

    size_t chunk_size = L.size() / num_threads;

    for (size_t i = 0; i < num_threads; i++) {
        size_t start = i * chunk_size;
        size_t end = (i == num_threads - 1) ? L.size() : start + chunk_size;

        // Pass indices instead of copying sub-vectors
        threads.emplace_back(nested_loop_worker, std::cref(L), start, end, std::cref(R), std::ref(thread_results[i]));
    }

    for (auto& t : threads) {
        t.join();
    }

    // Merge results efficiently
    size_t total_size = 0;
    for (const auto& partial_result : thread_results) {
        total_size += partial_result.size();
    }
    joined.reserve(total_size);

    for (const auto& partial_result : thread_results) {
        joined.insert(joined.end(), partial_result.begin(), partial_result.end());
    }
}

void iaa_nested_loop_partition(qpl_path_t execution_path, qpl_job *job1, qpl_job *job2, const std::vector<uint32_t>& L_part,
                               const std::vector<uint32_t>& R, std::vector<uint32_t>& joined) {
    size_t l_size = L_part.size();
    size_t i = 0;
    size_t bit_vector_size = (R.size() + 7) / 8 * 8;
    std::vector<uint8_t> bit_vector(bit_vector_size, 0);

    for (; i + 1 < l_size; i += 2) {  // Process two elements per iteration
        // **Configure Job 1**
        job1->next_in_ptr = const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(R.data()));
        job1->available_in = R.size() * sizeof(uint32_t);
        job1->next_out_ptr = bit_vector.data();  // **Shared Bit Vector**
        job1->num_input_elements = R.size();
        job1->available_out = bit_vector.size();
        job1->src1_bit_width = 32;
        job1->op = qpl_op_scan_eq;
        job1->param_low = L_part[i];  // First search value
        job1->out_bit_width = qpl_ow_nom;
        auto status = qpl_submit_job(job1);
        
        if (status != QPL_STS_OK) {
            std::cerr << "Error getting job size: " << status << "\n";
            return;
        }

        // **Configure Job 2**
        job2->next_in_ptr = const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(R.data()));
        job2->available_in = R.size() * sizeof(uint32_t);
        job2->next_out_ptr = bit_vector.data();  // **Shared Bit Vector**
        job2->num_input_elements = R.size();
        job2->available_out = bit_vector.size();
        job2->src1_bit_width = 32;
        job2->op = qpl_op_scan_eq;
        job2->param_low = L_part[i + 1];  // Second search value
        job2->out_bit_width = qpl_ow_nom;

        status = qpl_submit_job(job2);

        if (status != QPL_STS_OK) {
            std::cerr << "Error getting job size: " << status << "\n";
            return;
        }


        status = qpl_wait_job(job1);

        if (status != QPL_STS_OK) {
            std::cerr << "Error getting job size: " << status << "\n";
            return;
        }

        status = qpl_wait_job(job2);

        if (status != QPL_STS_OK) {
            std::cerr << "Error getting job size: " << status << "\n";
            return;
        }
    }

    /*
        if (status != QPL_STS_OK) {
            std::cerr << "IAA Scan Job failed with error: " << status << "\n";
            continue;
        }

        uint32_t bitmask_size = job->total_out;

        for (size_t i = 0; i < R.size(); i++) {
            if(bit_vector[0] > 0)
            if (bit_vector[i / 8] & (1 << (i % 8))) {  // Check bit in bitmask
                joined.emplace_back(R[i]);  // Store matching value
            }
        }
    }

    qpl_fini_job(job);*/
}

void iaa_nested_loop_join_multi(qpl_path_t execution_path, std::vector<uint32_t>& L,
                                std::vector<uint32_t>& R, std::vector<uint32_t>& joined,
                                size_t num_threads) {
    if (num_threads == 0) {
        num_threads = std::thread::hardware_concurrency();  // Auto-detect if not specified
    }
    
    std::vector<std::unique_ptr<uint8_t[]>> buffers;
    std::vector<qpl_job*> jobs;

    std::vector<std::unique_ptr<uint8_t[]>> buffers2;
    std::vector<qpl_job*> jobs2;

    std::vector<std::vector<uint32_t>> L_parts(num_threads);
    size_t chunk_size = L.size() / num_threads;

    for(int i = 0; i < num_threads; i++) {
        size_t start = i * chunk_size;
        size_t end = (i == num_threads - 1) ? L.size() : start + chunk_size;
        L_parts[i] = std::vector<uint32_t>(L.begin() + start, L.begin() + end);

        uint32_t job_size = 0;
        qpl_status status = qpl_get_job_size(execution_path, &job_size);
        if (status != QPL_STS_OK) {
            std::cerr << "Error getting job size: " << status << "\n";
            return;
        }
        
        buffers.push_back(std::make_unique<uint8_t[]>(job_size));
        qpl_job* job = reinterpret_cast<qpl_job*>(buffers[i].get());
        status = qpl_init_job(execution_path, job);

        buffers2.push_back(std::make_unique<uint8_t[]>(job_size));
        qpl_job* job2 = reinterpret_cast<qpl_job*>(buffers2[i].get());
        status = qpl_init_job(execution_path, job2);
        if (status != QPL_STS_OK) {
            std::cerr << "Sync Error initializing QPL job: " << status << "\n";
            return;
        }
        jobs.push_back(job);
        jobs2.push_back(job2);
        
    }

    std::vector<std::thread> threads;
    std::vector<std::vector<uint32_t>> thread_results(num_threads);  // Separate results for each thread
    
    auto start_multi = std::chrono::high_resolution_clock::now();
    
    for (size_t i = 0; i < num_threads; i++) {
        threads.emplace_back(iaa_nested_loop_partition, execution_path, jobs[i], jobs2[i],
                             std::cref(L_parts[i]), std::cref(R), std::ref(thread_results[i]));
    }

    for (auto& t : threads) {
        t.join();
    }

    auto end_multi = std::chrono::high_resolution_clock::now();

    // **Efficiently Merge Results**
    size_t total_size = 0;
    for (const auto& partial_result : thread_results) {
        total_size += partial_result.size();
    }
    joined.reserve(total_size);  // Preallocate for efficiency

    for (const auto& partial_result : thread_results) {
        joined.insert(joined.end(), partial_result.begin(), partial_result.end());
    }

    std::cout << "Chunked Sync\t" << std::chrono::duration<double>(end_multi - start_multi).count() << "\t" << joined.size() << "\n";
    
}


// **Synchronous IAA Nested Loop Join**
void iaa_nested_loop_join_sync(qpl_path_t execution_path, qpl_job *job, std::vector<uint8_t> &bit_vector, 
                               std::vector<uint32_t>& L, std::vector<uint32_t>& R, std::vector<uint32_t>& joined) {
    for (uint32_t l_val : L) {
        // **Step 1: Scan for Matches**
        job->next_in_ptr = const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(R.data()));
        job->available_in = R.size() * sizeof(uint32_t);
        job->next_out_ptr = bit_vector.data();
        job->num_input_elements = R.size();
        job->available_out = bit_vector.size();
        job->src1_bit_width = 32;
        job->op = qpl_op_scan_eq;
        job->param_low = l_val;
        job->out_bit_width = qpl_ow_nom;

        auto status = qpl_execute_job(job);
        if (status != QPL_STS_OK) {
            std::cerr << "IAA Scan Job failed with error: " << status << "\n";
            continue;
        }

        uint32_t bitmask_size = job->total_out;

        // **Step 2: Merge Results Using CPU**
        size_t prev_size = joined.size();
        for (size_t i = 0; i < R.size(); i++) {
            if (bit_vector[i / 8] & (1 << (i % 8))) {  // Check bit in bitmask
                joined.push_back(R[i]);  // Store matching value
            }
        }
    }

    qpl_fini_job(job);
}


// **Asynchronous IAA Nested Loop Join**
void iaa_nested_loop_join_async(qpl_path_t execution_path, std::vector<qpl_job*> jobs,
                                std::vector<std::vector<uint8_t>>& bit_vectors,
                                std::vector<uint32_t>& L, std::vector<uint32_t>& R,
                                std::vector<uint32_t>& joined) {
    
    size_t num_jobs = L.size();
    std::vector<bool> job_completed(num_jobs, false);  // Track job completion
    size_t active_jobs = 0;

    // **Step 1: Submit Scan Jobs Asynchronously**
    for (size_t i = 0; i < num_jobs; i++) {
        jobs[i]->next_in_ptr = const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(R.data()));
        jobs[i]->available_in = R.size() * sizeof(uint32_t);
        jobs[i]->next_out_ptr = bit_vectors[i].data();
        jobs[i]->num_input_elements = R.size();
        jobs[i]->available_out = bit_vectors[i].size();
        jobs[i]->src1_bit_width = 32;
        jobs[i]->op = qpl_op_scan_eq;
        jobs[i]->param_low = L[i];
        jobs[i]->out_bit_width = qpl_ow_nom;

        qpl_submit_job(jobs[i]);  // **Asynchronous execution**
        active_jobs++;
    }

    // **Step 2: Process Jobs as Soon as They Complete**
    while (active_jobs > 0) {
        for (size_t i = 0; i < num_jobs; i++) {
            if (!job_completed[i] && qpl_check_job(jobs[i]) != QPL_STS_BEING_PROCESSED) {
                job_completed[i] = true;  // Mark job as completed
                active_jobs--;

                uint32_t bitmask_size = jobs[i]->total_out;

                // **Step 3: Merge Results Using CPU**
                size_t prev_size = joined.size();
                for (size_t j = 0; j < R.size(); j++) {
                    if (bit_vectors[i][j / 8] & (1 << (j % 8))) {  // Check bit in bitmask
                        joined.push_back(R[j]);  // Store matching value
                    }
                }
            }
        }
    }
}


// **Naive Nested Loop Join**
void nested_loop_join(const std::vector<uint32_t>& L, const std::vector<uint32_t>& R, std::vector<uint32_t>& joined) {
    for (uint32_t l_val : L) {
        for (uint32_t r_val : R) {
            if (l_val == r_val) {
                joined.push_back(r_val);
            }
        }
    }
}

void hybrid_nested_loop_join(qpl_path_t execution_path, std::vector<uint32_t>& L,
                             std::vector<uint32_t>& R, std::vector<uint32_t>& joined,
                             size_t num_threads) {
    if (num_threads == 0) {
        num_threads = std::thread::hardware_concurrency();  // Auto-detect if not specified
    }

    size_t split_cpu = (L.size() * 2) / 3;  // 2/3 for CPU
    size_t split_iaa = L.size() - split_cpu;  // 1/3 for IAA

    //size_t split_iaa = (L.size() * 9) / 10;
    //size_t split_cpu = L.size() - split_iaa;

    // **Split L into CPU and IAA portions**
    std::vector<uint32_t> L_part_cpu(L.begin(), L.begin() + split_cpu);   // 2/3 for CPU
    std::vector<uint32_t> L_part_iaa(L.begin() + split_cpu, L.end());     // 1/3 for IAA

    // **Result containers**
    std::vector<uint32_t> joined_cpu, joined_iaa;

    // **Determine Thread Distribution**
    size_t cpu_threads = (num_threads * 2) / 3;  // 2/3 of threads for CPU
    size_t iaa_threads = num_threads - cpu_threads;  // 1/3 of threads for IAA

    auto start_hybrid = std::chrono::high_resolution_clock::now();    
    // **Launch Multithreaded Nested Loop Join (CPU)**
    std::thread cpu_thread(nested_loop_join_parallel, std::cref(L_part_cpu), std::cref(R), 
                           std::ref(joined_cpu), 64);

    // **Launch IAA-Based Nested Loop Join (IAA)**
    std::thread iaa_thread(iaa_nested_loop_join_multi, execution_path, std::ref(L_part_iaa), 
                           std::ref(R), std::ref(joined_iaa), 64);

    // **Wait for both to finish**
    cpu_thread.join();
    iaa_thread.join();

    auto end_hybrid = std::chrono::high_resolution_clock::now();
    std::cout << "Hybrid\t\t" << std::chrono::duration<double>(end_hybrid - start_hybrid).count() << "\t" << joined.size() << "\n";


    // **Merge results**
    joined.reserve(joined_cpu.size() + joined_iaa.size());
    joined.insert(joined.end(), joined_cpu.begin(), joined_cpu.end());
    joined.insert(joined.end(), joined_iaa.begin(), joined_iaa.end());
}