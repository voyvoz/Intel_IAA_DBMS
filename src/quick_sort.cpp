#include <qpl/qpl.h>
#include <numeric>
#include <vector>
#include <algorithm>
#include <iostream>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <thread>
#include <limits>
#include <chrono>
#include <future>
#include <queue>

#include "sort.hpp"
#include "utils.hpp"

void qpl_partition(std::vector<uint32_t>& input, uint32_t pivot,
                   qpl_job* job_scan, qpl_job* job_less, qpl_job* job_ge,
                   std::vector<uint32_t>& mask, std::vector<uint32_t>& nmask,
                   std::vector<uint32_t>& temp_less, std::vector<uint32_t>& temp_greater_equal) {
    size_t size = input.size();
    size_t bitmask_size = (size + 31) / 32;

    mask.resize(bitmask_size);
    nmask.resize(bitmask_size);
    temp_less.resize(size);
    temp_greater_equal.resize(size);

    // Step 1: SCAN for input < pivot
    job_scan->op = qpl_op_scan_lt;
    job_scan->next_in_ptr = reinterpret_cast<uint8_t*>(input.data());
    job_scan->available_in = static_cast<uint32_t>(size * sizeof(uint32_t));
    job_scan->next_out_ptr = reinterpret_cast<uint8_t*>(mask.data());
    job_scan->available_out = static_cast<uint32_t>(mask.size() * sizeof(uint32_t));
    job_scan->param_low = pivot;
    job_scan->param_high = pivot;
    job_scan->src1_bit_width = 32;
    job_scan->out_bit_width = qpl_ow_nom;
    job_scan->num_input_elements = static_cast<uint32_t>(size);
    //job_scan->flags = QPL_FLAG_OMIT_AGGREGATES;

    check_qpl(qpl_execute_job(job_scan), "qpl_scan_lt");
    //check_qpl(qpl_wait_job(job_scan), "wait_job_scan");

    uint32_t mask_len = job_scan->total_out;
    uint32_t count_less = job_scan->sum_value;  // # of values < pivot

    if (count_less == 0) {
        // Everything >= pivot
        std::copy(input.begin(), input.end(), temp_greater_equal.begin());
        job_less->total_out = 0;
        job_ge->total_out = static_cast<uint32_t>(size * sizeof(uint32_t));
        std::memcpy(temp_greater_equal.data(), input.data(), size * sizeof(uint32_t));
        return;
    }

    if (count_less == size) {
        // Everything < pivot
        std::copy(input.begin(), input.end(), temp_less.begin());
        job_less->total_out = static_cast<uint32_t>(size * sizeof(uint32_t));
        job_ge->total_out = 0;
        std::memset(temp_greater_equal.data(), 0, size * sizeof(uint32_t));
        return;
    }

    // Step 2: SELECT values < pivot
    job_less->op = qpl_op_select;
    job_less->next_in_ptr = reinterpret_cast<uint8_t*>(input.data());
    job_less->available_in = static_cast<uint32_t>(size * sizeof(uint32_t));
    job_less->next_out_ptr = reinterpret_cast<uint8_t*>(temp_less.data());
    job_less->available_out = static_cast<uint32_t>(size * sizeof(uint32_t));
    job_less->next_src2_ptr = reinterpret_cast<uint8_t*>(mask.data());
    job_less->available_src2 = mask_len;
    job_less->src2_bit_width = 1;
    job_less->src1_bit_width = 32;
    job_less->out_bit_width = qpl_ow_32;
    job_less->num_input_elements = static_cast<uint32_t>(size);
    //job_less->flags = QPL_FLAG_OMIT_AGGREGATES;

    check_qpl(qpl_submit_job(job_less), "submit_job_less");

    // Invert mask
    for (size_t i = 0; i < bitmask_size; ++i)
        nmask[i] = ~mask[i];
    if (size % 32 != 0) {
        uint32_t valid_mask = (1u << (size % 32)) - 1;
        nmask.back() &= valid_mask;
    }

    // Step 3: SELECT values >= pivot
    job_ge->op = qpl_op_select;
    job_ge->next_in_ptr = reinterpret_cast<uint8_t*>(input.data());
    job_ge->available_in = static_cast<uint32_t>(size * sizeof(uint32_t));
    job_ge->next_out_ptr = reinterpret_cast<uint8_t*>(temp_greater_equal.data());
    job_ge->available_out = static_cast<uint32_t>(size * sizeof(uint32_t));
    job_ge->next_src2_ptr = reinterpret_cast<uint8_t*>(nmask.data());
    job_ge->available_src2 = static_cast<uint32_t>(nmask.size() * sizeof(uint32_t));
    job_ge->src2_bit_width = 1;
    job_ge->src1_bit_width = 32;
    job_ge->out_bit_width = qpl_ow_32;
    job_ge->num_input_elements = static_cast<uint32_t>(size);
    //job_ge->flags = QPL_FLAG_OMIT_AGGREGATES;

    check_qpl(qpl_submit_job(job_ge), "submit_job_ge");
}


void iaa_quick_sort(std::vector<uint32_t>& data,
                    qpl_job* job_scan, qpl_job* job_less, qpl_job* job_ge,
                    uint32_t pivot,
                    std::vector<uint32_t>& mask, std::vector<uint32_t>& nmask,
                    std::vector<uint32_t>& temp_less, std::vector<uint32_t>& temp_greater_equal) {
    if (data.size() <= 1) return;

    qpl_partition(data, pivot,
                  job_scan, job_less, job_ge,
                  mask, nmask, temp_less, temp_greater_equal);

    check_qpl(qpl_wait_job(job_less), "wait_job_less");
    uint32_t count_less = job_less->total_out / sizeof(uint32_t);

    if (count_less == data.size()) {
        uint32_t new_pivot = data.back();
        if (new_pivot != pivot)
            iaa_quick_sort(data, job_scan, job_less, job_ge, new_pivot, mask, nmask, temp_less, temp_greater_equal);
        return;
    }

    check_qpl(qpl_wait_job(job_ge), "wait_job_ge");
    uint32_t count_ge = job_ge->total_out / sizeof(uint32_t);

    if (count_ge == data.size()) {
        uint32_t new_pivot = data.front();
        if (new_pivot != pivot)
            iaa_quick_sort(data, job_scan, job_less, job_ge, new_pivot, mask, nmask, temp_less, temp_greater_equal);
        return;
    }

    std::vector<uint32_t> left(temp_less.begin(), temp_less.begin() + count_less);
    std::vector<uint32_t> right(temp_greater_equal.begin(), temp_greater_equal.begin() + count_ge);

    uint32_t lpivot = left[left.size() / 2];
    uint32_t rpivot = right[right.size() / 2];

    iaa_quick_sort(left, job_scan, job_less, job_ge, lpivot, mask, nmask, temp_less, temp_greater_equal);
    iaa_quick_sort(right, job_scan, job_less, job_ge, rpivot, mask, nmask, temp_less, temp_greater_equal);

    data.clear();
    data.reserve(count_less + count_ge);
    std::move(left.begin(), left.end(), std::back_inserter(data));
    std::move(right.begin(), right.end(), std::back_inserter(data));
}

void async_quicksort_chunk(qpl_path_t path, std::vector<uint32_t>& chunk_data) {
    qpl_job* job_scan = init_qpl_job(path);
    qpl_job* job_less = init_qpl_job(path);
    qpl_job* job_ge   = init_qpl_job(path);

    std::vector<uint32_t> mask, nmask, temp_less, temp_greater_equal;
    uint32_t pivot = chunk_data[chunk_data.size() / 2];
    iaa_quick_sort(chunk_data, job_scan, job_less, job_ge, pivot,
                   mask, nmask, temp_less, temp_greater_equal);

    std::free(job_scan);
    std::free(job_less);
    std::free(job_ge);
}

void chunked_iaa_quicksort(qpl_path_t path, std::vector<uint32_t>& data, std::vector<uint32_t>& sorted, size_t num_chunks) {
    size_t chunk_size = (data.size() + num_chunks - 1) / num_chunks;
    std::vector<std::vector<uint32_t>> chunks(num_chunks);
    std::vector<std::future<void>> futures;

    for (size_t i = 0; i < num_chunks; ++i) {
        size_t start = i * chunk_size;
        size_t end = std::min(data.size(), start + chunk_size);
        chunks[i] = std::vector<uint32_t>(data.begin() + start, data.begin() + end);
        futures.emplace_back(std::async(std::launch::async, async_quicksort_chunk, path, std::ref(chunks[i])));
    }

    for (auto& f : futures)
        f.get();

    using Entry = std::pair<uint32_t, size_t>;
    std::priority_queue<Entry, std::vector<Entry>, std::greater<>> heap;
    std::vector<size_t> indices(num_chunks, 0);
    sorted.clear();
    sorted.reserve(data.size());

    for (size_t i = 0; i < num_chunks; ++i) {
        if (!chunks[i].empty()) {
            heap.emplace(chunks[i][0], i);
        }
    }

    while (!heap.empty()) {
        auto [val, chunk_idx] = heap.top(); heap.pop();
        sorted.push_back(val);
        if (++indices[chunk_idx] < chunks[chunk_idx].size()) {
            heap.emplace(chunks[chunk_idx][indices[chunk_idx]], chunk_idx);
        }
    }
}