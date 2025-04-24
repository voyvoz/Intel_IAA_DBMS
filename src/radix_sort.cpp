#include <qpl/qpl.h>
#include <vector>
#include <cstdint>
#include <iostream>
#include <memory>
#include <future>
#include <algorithm>
#include <cstring>
#include <numeric>
#include <mutex>
#include <queue>
#include <functional>
#include <execution>

#include "sort.hpp"
#include "utils.hpp"

constexpr size_t MAX_THREADS = 128;

size_t calculate_max_bit_width(const std::vector<uint32_t>& data) {
    uint32_t max_val = *std::max_element(data.begin(), data.end());
    return std::bit_width(max_val);
}

void qpl_radix_sort(QplJobPool* job_pool, std::vector<uint32_t>& data,
                    std::vector<std::vector<uint32_t>>& temp_buckets,
                    std::vector<std::vector<uint32_t>>& masks,
                    size_t bits_per_pass) {
    size_t size = data.size();
    std::vector<uint32_t> buffer(size);
    std::vector<uint32_t>* in = &data;
    std::vector<uint32_t>* out = &buffer;

    size_t max_bits = calculate_max_bit_width(data);
    size_t passes = (max_bits + bits_per_pass - 1) / bits_per_pass;
    size_t radix = 1ull << bits_per_pass;

    std::vector<size_t> bucket_counts(radix);
    std::vector<size_t> bucket_offsets(radix);

    for (size_t pass = 0; pass < passes; ++pass) {
        size_t shift = pass * bits_per_pass;

        //std::fill(bucket_counts.begin(), bucket_counts.end(), 0);
        //std::fill(bucket_offsets.begin(), bucket_offsets.end(), 0);

        for (size_t i = 0; i < size; ++i) {
            uint32_t key = ((*in)[i] >> shift) & (radix - 1);
            bucket_counts[key]++;
        }

        for (size_t i = 1; i < radix; ++i)
            bucket_offsets[i] = bucket_offsets[i - 1] + bucket_counts[i - 1];

        std::vector<size_t> active_buckets;
        for (size_t b = 0; b < radix; ++b) {
            if (bucket_counts[b] != 0)
                active_buckets.push_back(b);
        }

        std::for_each(std::execution::par_unseq, active_buckets.begin(), active_buckets.end(), [&](size_t b) {
            qpl_job* job = job_pool->acquire();
            size_t thread_id = job_pool->next - 1;

            auto& temp_bucket = temp_buckets[thread_id];
            auto& mask = masks[thread_id];

            job->op = qpl_op_scan_range;
            job->next_in_ptr = reinterpret_cast<uint8_t*>(in->data());
            job->available_in = size * sizeof(uint32_t);
            job->next_out_ptr = reinterpret_cast<uint8_t*>(mask.data());
            job->available_out = static_cast<uint32_t>(mask.size() * sizeof(uint32_t));
            job->param_low = b << shift;
            job->param_high = ((b + 1) << shift) - 1;
            job->src1_bit_width = 32;
            job->out_bit_width = qpl_ow_nom;
            job->num_input_elements = static_cast<uint32_t>(size);
            job->flags = QPL_FLAG_OMIT_AGGREGATES;
            check_qpl(qpl_execute_job(job), "scan_range");

            uint32_t mask_len = job->total_out;

            job->op = qpl_op_select;
            job->next_in_ptr = reinterpret_cast<uint8_t*>(in->data());
            job->available_in = size * sizeof(uint32_t);
            job->next_out_ptr = reinterpret_cast<uint8_t*>(temp_bucket.data());
            job->available_out = size * sizeof(uint32_t);
            job->next_src2_ptr = reinterpret_cast<uint8_t*>(mask.data());
            job->available_src2 = mask_len;
            job->src2_bit_width = 1;
            job->src1_bit_width = 32;
            job->out_bit_width = qpl_ow_32;
            job->num_input_elements = static_cast<uint32_t>(size);
            job->flags = QPL_FLAG_OMIT_AGGREGATES;
            check_qpl(qpl_execute_job(job), "select");

            size_t out_pos = bucket_offsets[b];
            size_t num_selected = job->total_out / sizeof(uint32_t);
            std::memcpy(out->data() + out_pos, temp_bucket.data(), num_selected * sizeof(uint32_t));
        });

        std::swap(in, out);
    }

    if (in != &data)
        data = *in;
}

void chunked_radix_sort(QplJobPool& job_pool, std::vector<uint32_t>& data,
                        size_t max_threads, size_t bits_per_pass) {
    const size_t total_size = data.size();
    const size_t chunk_count = std::min(max_threads, static_cast<size_t>(std::thread::hardware_concurrency()));
    const size_t chunk_size = (total_size + chunk_count - 1) / chunk_count;


    std::vector<std::vector<uint32_t>> sorted_chunks(chunk_count);
    std::vector<std::future<void>> futures;

    for (size_t i = 0; i < chunk_count; ++i) {
        size_t offset = i * chunk_size;
        size_t end = std::min(offset + chunk_size, total_size);
        size_t this_chunk_size = end - offset;

        futures.emplace_back(std::async(std::launch::async, [&, offset, this_chunk_size, i]() {
            std::vector<uint32_t> local_data(data.begin() + offset, data.begin() + offset + this_chunk_size);
            std::vector<std::vector<uint32_t>> temp_buckets(2048, std::vector<uint32_t>(local_data.size()));
            std::vector<std::vector<uint32_t>> masks(2048, std::vector<uint32_t>((local_data.size() + 31) / 32));

            qpl_radix_sort(&job_pool, local_data, temp_buckets, masks, bits_per_pass);
            sorted_chunks[i] = std::move(local_data);
        }));
    }

    for (auto& f : futures)
        f.get();

    // K-way merge
    struct ChunkCursor {
        const std::vector<uint32_t>* chunk;
        size_t index;

        bool operator>(const ChunkCursor& other) const {
            return (*chunk)[index] > (*other.chunk)[other.index];
        }
    };

    std::priority_queue<ChunkCursor, std::vector<ChunkCursor>, std::greater<>> heap;
    for (const auto& chunk : sorted_chunks) {
        if (!chunk.empty()) {
            heap.push({&chunk, 0});
        }
    }

    std::vector<uint32_t> result;
    result.reserve(total_size);
    while (!heap.empty()) {
        auto top = heap.top();
        heap.pop();
        result.push_back((*top.chunk)[top.index]);
        if (top.index + 1 < top.chunk->size()) {
            heap.push({top.chunk, top.index + 1});
        }
    }

    data = std::move(result);
}