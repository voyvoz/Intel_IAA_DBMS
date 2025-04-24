#ifndef SORT_MERGE_JOIN_HPP
#define SORT_MERGE_JOIN_HPP

#include <iostream>
#include <vector>
#include <chrono>
#include <memory>
#include <thread>

#include "utils.hpp"
#include "qpl/qpl.h"

// CPU-based Sort-Merge Join
void sort_merge_join_cpu(const std::vector<uint32_t>& L, const std::vector<uint32_t>& R, std::vector<uint32_t>& joined);

// CPU-based Sort-Merge Join (Multithreaded)
void sort_merge_join_cpu_parallel(const std::vector<uint32_t>& L, const std::vector<uint32_t>& R,
                                   std::vector<uint32_t>& joined, size_t num_threads);

// IAA-based Sort-Merge Join using qpl_op_scan (synchronous)
void sort_merge_join_iaa(qpl_path_t execution_path, qpl_job* job, const std::vector<uint32_t>& L,
                         const std::vector<uint32_t>& R, std::vector<uint32_t>& joined);

// IAA-based Sort-Merge Join (preallocated jobs, multithreaded)
void sort_merge_join_iaa_preallocated(qpl_path_t execution_path, const std::vector<uint32_t>& L,
                                      const std::vector<uint32_t>& R, std::vector<uint32_t>& joined,
                                      size_t num_jobs);

// IAA-based Sort-Merge Join (chunked worker threads)
void sort_merge_join_iaa_chunked(qpl_path_t execution_path, const std::vector<uint32_t>& L,
                                 const std::vector<uint32_t>& R, std::vector<uint32_t>& joined,
                                 size_t num_jobs);

// Hybrid Join: CPU and IAA Sort-Merge combined
void sort_merge_join_hybrid(qpl_path_t execution_path, const std::vector<uint32_t>& L,
                            const std::vector<uint32_t>& R, std::vector<uint32_t>& joined,
                            size_t num_threads, size_t num_jobs);

#endif // SORT_MERGE_JOIN_HPP
