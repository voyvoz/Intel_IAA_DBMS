#ifndef HASH_JOIN_HPP
#define HASH_JOIN_HPP

#include <vector>
#include <unordered_map>
#include <cstdint>
#include "qpl/qpl.h"

// Constant CRC64 polynomial used for hashing in IAA joins
constexpr const uint64_t poly = 0x04C11DB700000000;

// CPU-based Hash Join
void hash_join_cpu(const std::vector<uint32_t>& L, const std::vector<uint32_t>& R, std::vector<uint32_t>& joined);

// CPU-based Hash Join (Multithreaded)
void hash_join_cpu_parallel(const std::vector<uint32_t>& L, const std::vector<uint32_t>& R,
                            std::vector<uint32_t>& joined, size_t num_threads);

// IAA-based Hash Join (Single-threaded)
void hash_join_iaa(qpl_path_t execution_path, const std::vector<uint32_t>& L, const std::vector<uint32_t>& R,
                   std::vector<uint32_t>& joined);

// IAA-based Hash Join (Chunked multithreaded)
void hash_join_iaa_chunked(qpl_path_t execution_path, const std::vector<uint32_t>& L,
                           const std::vector<uint32_t>& R, std::vector<uint32_t>& joined,
                           size_t num_jobs);

// Hybrid Hash Join (2/3 CPU, 1/3 IAA)
void hash_join_hybrid(qpl_path_t execution_path, const std::vector<uint32_t>& L,
                      const std::vector<uint32_t>& R, std::vector<uint32_t>& joined,
                      size_t num_threads);

#endif // HASH_JOIN_HPP
