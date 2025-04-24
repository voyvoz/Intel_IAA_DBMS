#ifndef IAA_JOIN_HPP
#define IAA_JOIN_HPP

#include <iostream>
#include <vector>
#include <memory>
#include <chrono>
#include <iomanip>
#include <thread>
#include <mutex>

#include "utils.hpp"
#include "qpl/qpl.h"

struct PreAllocatedJob {
    std::unique_ptr<uint8_t[]> job_buffer;
    qpl_job* job;
    std::vector<uint8_t> bit_vector;
};

void iaa_nested_loop_join_preallocated(qpl_path_t execution_path, std::vector<uint32_t>& L,
                                       std::vector<uint32_t>& R, std::vector<uint32_t>& joined,
                                       size_t num_threads);

void nested_loop_worker(const std::vector<uint32_t>& L, size_t start, size_t end, 
                        const std::vector<uint32_t>& R, std::vector<uint32_t>& local_results);

void nested_loop_join_parallel(const std::vector<uint32_t>& L, const std::vector<uint32_t>& R, 
                               std::vector<uint32_t>& joined, size_t num_threads);

void iaa_nested_loop_partition(qpl_path_t execution_path, qpl_job *job1, qpl_job *job2,
                               const std::vector<uint32_t>& L_part, const std::vector<uint32_t>& R,
                               std::vector<uint32_t>& joined);

void iaa_nested_loop_join_multi(qpl_path_t execution_path, std::vector<uint32_t>& L,
                                std::vector<uint32_t>& R, std::vector<uint32_t>& joined,
                                size_t num_threads);

void iaa_nested_loop_join_sync(qpl_path_t execution_path, qpl_job *job, std::vector<uint8_t> &bit_vector, 
                               std::vector<uint32_t>& L, std::vector<uint32_t>& R, std::vector<uint32_t>& joined);

void iaa_nested_loop_join_async(qpl_path_t execution_path, std::vector<qpl_job*> jobs,
                                std::vector<std::vector<uint8_t>>& bit_vectors,
                                std::vector<uint32_t>& L, std::vector<uint32_t>& R,
                                std::vector<uint32_t>& joined);

void nested_loop_join(const std::vector<uint32_t>& L, const std::vector<uint32_t>& R,
                      std::vector<uint32_t>& joined);

void hybrid_nested_loop_join(qpl_path_t execution_path, std::vector<uint32_t>& L,
                             std::vector<uint32_t>& R, std::vector<uint32_t>& joined,
                             size_t num_threads);

#endif // IAA_JOIN_HPP
