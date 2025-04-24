#include <iostream>
#include <vector>
#include <chrono>
#include <memory>
#include "utils.hpp"
#include "qpl/qpl.h"
#include <thread>

#include "sm_join.hpp"

// CPU-based Sort-Merge Join
void sort_merge_join_cpu(const std::vector<uint32_t>& L, const std::vector<uint32_t>& R, std::vector<uint32_t>& joined) {
    size_t i = 0, j = 0;
    while (i < L.size() && j < R.size()) {
        if (L[i] < R[j]) {
            i++;
        } else if (L[i] > R[j]) {
            j++;
        } else {
            joined.push_back(L[i]);  // Match found
            i++;
            j++;
        }
    }
}

// Pre-allocated IAA-based Chunked Sort-Merge Join with Preconfigured Parameters
void sort_merge_join_iaa_preallocated(qpl_path_t execution_path, const std::vector<uint32_t>& L,
                                      const std::vector<uint32_t>& R, std::vector<uint32_t>& joined,
                                      size_t num_jobs) {
    size_t chunk_size = L.size() / num_jobs;
    std::vector<std::thread> threads;
    std::vector<std::vector<uint32_t>> partial_results(num_jobs);
    
    // Pre-allocate jobs
    uint32_t job_size = 0;
    qpl_get_job_size(execution_path, &job_size);
    std::vector<std::unique_ptr<uint8_t[]>> job_buffers(num_jobs);
    std::vector<qpl_job*> jobs(num_jobs);
    std::vector<std::vector<uint8_t>> bit_vectors(num_jobs, std::vector<uint8_t>((R.size() + 7) / 8 * 8, 0));
    std::vector<std::vector<uint32_t>> extracted_values(num_jobs, std::vector<uint32_t>(R.size(), 0));
    
    for (size_t i = 0; i < num_jobs; i++) {
        job_buffers[i] = std::make_unique<uint8_t[]>(job_size);
        jobs[i] = reinterpret_cast<qpl_job*>(job_buffers[i].get());
        qpl_init_job(execution_path, jobs[i]);

        // Preconfigure job parameters
        jobs[i]->next_in_ptr = reinterpret_cast<uint8_t*>(const_cast<uint32_t*>(R.data()));
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
        auto& extracted = extracted_values[job_id];

        size_t i = start, j = 0;
        while (i < end && j < R.size()) {
            if (L[i] < R[j]) {
                i++;
            } else if (L[i] > R[j]) {
                j++;
            } else {
                // Only update parameter_low, execution is pre-configured
                job->param_low = L[i];
                job->next_in_ptr = reinterpret_cast<uint8_t*>(const_cast<uint32_t*>(&R[j]));
                job->num_input_elements = R.size() - j;
                qpl_submit_job(job);
/*
                uint32_t bitmask_size = job->total_out;

                // Execute extraction step
                job->next_in_ptr = reinterpret_cast<uint8_t*>(const_cast<uint32_t*>(R.data()));
                job->available_in = R.size() * sizeof(uint32_t);
                job->next_out_ptr = reinterpret_cast<uint8_t*>(extracted.data());
                job->available_out = extracted.size() * sizeof(uint32_t);
                job->next_src2_ptr = bit_vector.data();
                job->available_src2 = bitmask_size;
                job->op = qpl_op_select;
                job->src2_bit_width = 1;
                job->out_bit_width = qpl_ow_32;

                qpl_submit_job(job);*/

               // uint32_t num_extracted = job->total_out / sizeof(uint32_t);
               // local_joined.insert(local_joined.end(), extracted.begin(), extracted.begin() + num_extracted);
                i++;
                j++;
                qpl_wait_job(job);
            }
        }
    };

    auto start_iaa_preallocated = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < num_jobs; i++) {
        size_t start = i * chunk_size;
        size_t end = (i == num_jobs - 1) ? L.size() : start + chunk_size;
        threads.emplace_back(worker, i, start, end, std::ref(partial_results[i]));
    }

    for (auto& t : threads) {
        t.join();
    }

    auto end_iaa_preallocated = std::chrono::high_resolution_clock::now();
    std::cout << "IAA Sort-Merge Join (Preallocated):\t" << std::chrono::duration<double>(end_iaa_preallocated - start_iaa_preallocated).count() << " seconds\t" << partial_results.size() << " matches\n";


    for (const auto& part : partial_results) {
        joined.insert(joined.end(), part.begin(), part.end());
    }
}

// IAA-based Sort-Merge Join using op_scan (batch processing)
void sort_merge_join_iaa(qpl_path_t execution_path, qpl_job* job, const std::vector<uint32_t>& L, 
                          const std::vector<uint32_t>& R, std::vector<uint32_t>& joined) {
    size_t bit_vector_size = (R.size() + 7) / 8 * 8;
    std::vector<uint8_t> bit_vector(bit_vector_size, 0);
    std::vector<uint32_t> extracted_values(R.size(), 0);

    size_t i = 0, j = 0;
    while (i < L.size() && j < R.size()) {
        if (L[i] < R[j]) {
            i++;
        } else if (L[i] > R[j]) {
            j++;
        } else {
            // Configure job for scan operation
            job->next_in_ptr = const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(R.data()));
            job->available_in = R.size() * sizeof(uint32_t);
            job->next_out_ptr = bit_vector.data();
            job->num_input_elements = R.size();
            job->available_out = bit_vector.size();
            job->src1_bit_width = 32;
            job->op = qpl_op_scan_eq;
            job->param_low = L[i];
            job->out_bit_width = qpl_ow_nom;

            auto status = qpl_execute_job(job);
            if (status != QPL_STS_OK) {
                std::cerr << "IAA Scan Job failed with error: " << status << "\n";
                i++;
                continue;
            }
/*
            uint32_t bitmask_size = job->total_out;

            // Extract matching values using op_select
            job->next_in_ptr = const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(R.data()));
            job->available_in = R.size() * sizeof(uint32_t);
            job->next_out_ptr = reinterpret_cast<uint8_t*>(extracted_values.data());
            job->available_out = extracted_values.size() * sizeof(uint32_t);
            job->next_src2_ptr = bit_vector.data();
            job->available_src2 = bitmask_size;
            job->op = qpl_op_select;
            job->src2_bit_width = 1;
            job->out_bit_width = qpl_ow_32;

            status = qpl_execute_job(job);
            if (status != QPL_STS_OK) {
                std::cerr << "IAA Extract Job failed with error: " << status << "\n";
                i++;
                continue;
            }

            //uint32_t num_extracted = job->total_out / sizeof(uint32_t);
            //joined.insert(joined.end(), extracted_values.begin(), extracted_values.begin() + num_extracted);*/
            i++;
            j++;
        }
    }
}

void sort_merge_join_iaa_chunked(qpl_path_t execution_path, const std::vector<uint32_t>& L,
                                 const std::vector<uint32_t>& R, std::vector<uint32_t>& joined,
                                 size_t num_jobs) {
    size_t chunk_size = L.size() / num_jobs;
    std::vector<std::thread> threads;
    std::vector<std::vector<uint32_t>> partial_results(num_jobs);

    auto worker = [&](size_t start, size_t end, std::vector<uint32_t>& local_joined) {
        uint32_t job_size = 0;
        qpl_get_job_size(execution_path, &job_size);
        std::unique_ptr<uint8_t[]> job_buffer = std::make_unique<uint8_t[]>(job_size);
        qpl_job* job = reinterpret_cast<qpl_job*>(job_buffer.get());
        qpl_init_job(execution_path, job);

        size_t bit_vector_size = (R.size() + 7) / 8 * 8;
        std::vector<uint8_t> bit_vector(bit_vector_size, 0);
        std::vector<uint32_t> extracted_values(R.size(), 0);

        size_t i = start, j = 0;
        while (i < end && j < R.size()) {
            if (L[i] < R[j]) {
                i++;
            } else if (L[i] > R[j]) {
                j++;
            } else {
                // Configure job for scan operation
                job->next_in_ptr = reinterpret_cast<uint8_t*>(const_cast<uint32_t*>(&R[j]));
                job->available_in = (R.size() - j) * sizeof(uint32_t);
                job->next_out_ptr = bit_vector.data();
                job->num_input_elements = R.size() - j;
                job->available_out = bit_vector.size();
                job->src1_bit_width = 32;
                job->op = qpl_op_scan_eq;
                job->param_low = L[i];
                job->out_bit_width = qpl_ow_nom;

                auto status = qpl_submit_job(job);
                if (status != QPL_STS_OK) {
                    std::cerr << "IAA Scan Job failed with error: " << status << "\n";
                    i++;
                    continue;
                }
/*
                uint32_t bitmask_size = job->total_out;

                // Extract matching values using op_select
                job->next_in_ptr = const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(R.data()));
                job->available_in = R.size() * sizeof(uint32_t);
                job->next_out_ptr = reinterpret_cast<uint8_t*>(extracted_values.data());
                job->available_out = extracted_values.size() * sizeof(uint32_t);
                job->next_src2_ptr = bit_vector.data();
                job->available_src2 = bitmask_size;
                job->op = qpl_op_select;
                job->src2_bit_width = 1;
                job->out_bit_width = qpl_ow_32;

                status = qpl_submit_job(job);
                if (status != QPL_STS_OK) {
                    std::cerr << "IAA Extract Job failed with error: " << status << "\n";
                    i++;
                    continue;
                }

                //uint32_t num_extracted = job->total_out / sizeof(uint32_t);
                //local_joined.insert(local_joined.end(), extracted_values.begin(), extracted_values.begin() + num_extracted);*/
                i++;
                j++;
            }
            qpl_wait_job(job);
        }
        qpl_wait_job(job);
    };

    auto start_iaac = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < num_jobs; i++) {
        size_t start = i * chunk_size;
        size_t end = (i == num_jobs - 1) ? L.size() : start + chunk_size;
        threads.emplace_back(worker, start, end, std::ref(partial_results[i]));
    }

    for (auto& t : threads) {
        t.join();
    }
    auto end_iaac = std::chrono::high_resolution_clock::now();
    std::cout << "IAA Sort-Merge Join (Chunked):\t" << std::chrono::duration<double>(end_iaac - start_iaac).count() << " seconds\t" << joined.size() << " matches\n";

    for (const auto& part : partial_results) {
        joined.insert(joined.end(), part.begin(), part.end());
    }
}

void sort_merge_join_cpu_parallel(const std::vector<uint32_t>& L, const std::vector<uint32_t>& R, std::vector<uint32_t>& joined, size_t num_threads) {
    size_t chunk_size = L.size() / num_threads;
    std::vector<std::thread> threads;
    std::vector<std::vector<uint32_t>> partial_results(num_threads);

    auto worker = [&](size_t start, size_t end, std::vector<uint32_t>& local_joined) {
        size_t i = start, j = 0;
        while (i < end && j < R.size()) {
            if (L[i] < R[j]) {
                i++;
            } else if (L[i] > R[j]) {
                j++;
            } else {
                local_joined.push_back(L[i]);  // Match found
                i++;
                j++;
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

void sort_merge_join_hybrid(qpl_path_t execution_path, const std::vector<uint32_t>& L, 
                            const std::vector<uint32_t>& R, std::vector<uint32_t>& joined, 
                            size_t num_threads, size_t num_jobs) {
    size_t cpu_split = (L.size() * 2) / 3;
    size_t iaa_split = L.size() - cpu_split;

    std::vector<uint32_t> L_cpu(L.begin(), L.begin() + cpu_split);
    std::vector<uint32_t> L_iaa(L.begin() + cpu_split, L.end());

    std::vector<uint32_t> joined_cpu, joined_iaa;

    std::thread cpu_thread(sort_merge_join_cpu_parallel, std::cref(L_cpu), std::cref(R), std::ref(joined_cpu), num_threads);
    std::thread iaa_thread(sort_merge_join_iaa_preallocated, execution_path, std::cref(L_iaa), std::cref(R), std::ref(joined_iaa), num_jobs);

    cpu_thread.join();
    iaa_thread.join();

    joined.reserve(joined_cpu.size() + joined_iaa.size());
    joined.insert(joined.end(), joined_cpu.begin(), joined_cpu.end());
    joined.insert(joined.end(), joined_iaa.begin(), joined_iaa.end());
}
