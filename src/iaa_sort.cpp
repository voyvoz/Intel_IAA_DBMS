#include <iostream>
#include <memory>
#include <numeric>
#include <vector>
#include <algorithm>
#include <chrono>
#include <thread>
#include <future>
#include <queue>
#include <sys/resource.h>

#include "qpl/qpl.h"

#include "sort.hpp"
#include "utils.hpp"

constexpr const uint32_t source_size         = 1024 * 1024 * 128;
constexpr const uint32_t input_vector_width  = 32;
constexpr const uint32_t output_vector_width = 32;
constexpr const uint32_t lower_boundary      = 0;
constexpr const uint32_t upper_boundary      = 9;
constexpr const uint32_t byte_bit_length     = 8;

constexpr const uint32_t num_threads = 384;
constexpr const uint32_t num_chunks = 25000;

double getCPUTimeUsed() {
    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);
    return usage.ru_utime.tv_sec + usage.ru_utime.tv_usec / 1e6;
}

uint32_t extract_minmax(qpl_path_t path, qpl_job* job, std::vector<uint32_t> &source, std::vector<uint32_t> &dest, 
                        std::vector<uint8_t> &mask_after_scan, uint32_t *min, uint32_t *max, int dest_size, bool compressed = false) {

    // Setup job for extraction
    job->next_in_ptr        = reinterpret_cast<uint8_t*>(source.data());
    job->available_in       = source.size() * sizeof(uint32_t);
    job->next_out_ptr       = reinterpret_cast<uint8_t*>(dest.data());
    job->available_out      = dest.size() * sizeof(uint32_t);
    job->op                 = qpl_op_extract;
    job->src1_bit_width     = 32;
    job->num_input_elements = source.size();
    job->out_bit_width      = qpl_ow_nom;
    job->param_low          = 0;
    job->param_high         = source.size() - 1;
    if(compressed)
        job->flags = QPL_FLAG_DECOMPRESS_ENABLE;

    qpl_execute_job(job);

    *min = job->first_index_min_value;
    *max = job->last_index_max_value;

    if (dest.size() == 2) {
        qpl_fini_job(job);
        return 0;
    }

    // Scan range without min/max
    job->next_in_ptr        = reinterpret_cast<uint8_t*>(source.data());
    job->available_in       = source.size() * sizeof(uint32_t);
    job->next_out_ptr       = mask_after_scan.data();
    job->available_out      = mask_after_scan.size();
    job->op                 = qpl_op_scan_range;
    job->param_low          = *min + 1;
    job->param_high         = *max - 1;
    if(compressed)
        job->flags = QPL_FLAG_DECOMPRESS_ENABLE;

    qpl_execute_job(job);

    uint32_t mask_length = job->total_out;

    // Adjust destination size
    dest.resize(dest_size);

    // Extract values without min/max
    job->next_in_ptr        = reinterpret_cast<uint8_t*>(source.data());
    job->available_in       = source.size() * sizeof(uint32_t);
    job->next_out_ptr       = reinterpret_cast<uint8_t*>(dest.data());
    job->available_out      = dest_size * sizeof(uint32_t);
    job->op                 = qpl_op_select;
    job->out_bit_width      = qpl_ow_32;
    job->next_src2_ptr      = mask_after_scan.data();
    job->available_src2     = mask_length;
    job->src2_bit_width     = 1;
    if(compressed)
        job->flags = QPL_FLAG_DECOMPRESS_ENABLE;

    qpl_execute_job(job);

    uint32_t extracted = job->total_out;
    qpl_fini_job(job);

    source.swap(dest);  

    return extracted;
}

void iaa_sort(qpl_path_t path, qpl_job* job, std::vector<uint32_t> &source, std::vector<uint32_t> &dest, 
              std::vector<uint8_t> &mask_after_scan, std::vector<uint32_t> &sorted, bool compress) {
    int first = 0;
    int last = source.size() - 1;
    uint32_t min = 0, max = 0;
    int dest_size = source.size() - 2;
    
    sorted.resize(source.size()); 

    bool compressed = compress;

    while (!source.empty()) {
        uint32_t extracted = extract_minmax(path, job, source, dest, mask_after_scan, &min, &max, dest_size, compressed);
        if (extracted == 0) break; // Stop when no more elements are extracted

        sorted[first++] = min; // Place min at the beginning
        sorted[last--] = max;  // Place max at the end

        dest_size -= 2;

        if(compressed)
            compressed = false;
    }
}

void iaa_sort_thread(qpl_path_t path, std::vector<uint32_t>& source, std::vector<uint32_t>& sorted, size_t start, size_t end) {
    uint32_t size = 0;
    qpl_get_job_size(path, &size);
    auto job_buffer = std::make_unique<uint8_t[]>(size);
    qpl_job* job = reinterpret_cast<qpl_job*>(job_buffer.get());
    qpl_init_job(path, job);

    std::vector<uint32_t> dest(end - start, 4);
    std::vector<uint8_t> mask_after_scan(end - start, 0);
    std::vector<uint32_t> thread_sorted(end - start, 0);

    std::vector<uint32_t> sub_source(source.begin() + start, source.begin() + end);
    iaa_sort(path, job, sub_source, dest, mask_after_scan, thread_sorted);

    std::copy(thread_sorted.begin(), thread_sorted.end(), sorted.begin() + start);
    qpl_fini_job(job);
}

void async_iaa_sort(qpl_path_t path, qpl_job* job, std::vector<uint32_t> sub_source, std::vector<uint32_t>& chunk_sorted, bool compress = false) {
    std::vector<uint32_t> dest(sub_source.size(), 4);
    std::vector<uint8_t> mask_after_scan(sub_source.size(), 0);
    iaa_sort(path, job, sub_source, dest, mask_after_scan, chunk_sorted, compress);
}

void chunked_iaa_sort(qpl_path_t path, std::vector<uint32_t>& source, std::vector<uint32_t>& sorted, bool compress) {
    uint32_t size = 0;
    qpl_get_job_size(path, &size);
    
    std::vector<std::unique_ptr<uint8_t[]>> job_buffers(num_chunks);
    std::vector<qpl_job*> jobs(num_chunks);
    
    for (size_t i = 0; i < num_chunks; i++) {
        job_buffers[i] = std::make_unique<uint8_t[]>(size);
        jobs[i] = reinterpret_cast<qpl_job*>(job_buffers[i].get());
        qpl_init_job(path, jobs[i]);
    }
    
    size_t chunk_size = source_size / num_chunks;
    std::vector<std::vector<uint32_t>> chunk_sorted(num_chunks);
    std::vector<std::future<void>> futures;
    
    auto start_iaa = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < num_chunks; i++) {
        size_t start = i * chunk_size;
        size_t end = (i == num_chunks - 1) ? source_size : start + chunk_size;
        
        std::vector<uint32_t> sub_source(source.begin() + start, source.begin() + end);
        chunk_sorted[i].resize(end - start);
        
        futures.emplace_back(std::async(std::launch::async, async_iaa_sort, path, jobs[i], std::move(sub_source), std::ref(chunk_sorted[i]), compress));
    }
    
    for (auto& f : futures) {
        f.get();
    }
    auto end_iaa = std::chrono::high_resolution_clock::now();
    
    for (size_t i = 0; i < num_chunks; i++) {
        qpl_fini_job(jobs[i]);
    }
    
    sorted.clear();
    sorted.reserve(source.size());
    
    using Entry = std::pair<uint32_t, size_t>; 
    std::priority_queue<Entry, std::vector<Entry>, std::greater<>> min_heap;
    std::vector<size_t> indices(num_chunks, 0);
    
    
    for (size_t i = 0; i < num_chunks; i++) {
        if (!chunk_sorted[i].empty()) {
            min_heap.emplace(chunk_sorted[i][0], i);
        }
    }
    
    while (!min_heap.empty()) {
        auto [val, chunk_idx] = min_heap.top();
        min_heap.pop();
        sorted.push_back(val);
        
        if (++indices[chunk_idx] < chunk_sorted[chunk_idx].size()) {
            min_heap.emplace(chunk_sorted[chunk_idx][indices[chunk_idx]], chunk_idx);
        }
    }
    
    std::chrono::duration<double> elapsed_iaa = end_iaa - start_iaa;
    
    std::cout << "iac::sort Time: " << elapsed_iaa.count() << std::endl;
}

void threaded_iaa_sort(qpl_path_t path, qpl_job* job, std::vector<uint32_t>& sub_source, std::vector<uint32_t>& chunk_sorted) {
    std::vector<uint32_t> dest(sub_source.size(), 4);
    std::vector<uint8_t> mask_after_scan(sub_source.size(), 0);
    iaa_sort(path, job, sub_source, dest, mask_after_scan, chunk_sorted);
}

void threaded_chunked_iaa_sort(qpl_path_t path, std::vector<uint32_t>& source, std::vector<uint32_t>& sorted) {
    uint32_t size = 0;
    qpl_get_job_size(path, &size);
    
    std::vector<std::unique_ptr<uint8_t[]>> job_buffers(num_threads);
    std::vector<qpl_job*> jobs(num_threads);
    
    for (size_t i = 0; i < num_threads; i++) {
        job_buffers[i] = std::make_unique<uint8_t[]>(size);
        jobs[i] = reinterpret_cast<qpl_job*>(job_buffers[i].get());
        if (qpl_init_job(path, jobs[i]) != QPL_STS_OK) {
            std::cerr << "Error initializing QPL job!" << std::endl;
            return;
        }
    }
    
    size_t chunk_size = source_size / num_threads;
    
    std::vector<std::vector<uint32_t>> sub_sources(num_threads);   // Store pre-copied data
    std::vector<std::vector<uint32_t>> chunk_sorted(num_threads);  // Store sorted data per thread
    std::vector<std::vector<uint32_t>> dest_buffers(num_threads);  // Store dest buffers
    std::vector<std::vector<uint8_t>> mask_buffers(num_threads);   // Store mask buffers

    for (size_t i = 0; i < num_threads; i++) {
        size_t start = i * chunk_size;
        size_t end = (i == num_threads - 1) ? source_size : start + chunk_size;
        
        // **Pre-copy the source chunk**
        sub_sources[i].assign(source.begin() + start, source.begin() + end);
        
        // **Pre-allocate buffers**
        chunk_sorted[i].resize(end - start);
        dest_buffers[i].resize(std::min(end - start, static_cast<size_t>(1000000)), 4);  // Limit max size
        mask_buffers[i].resize(end - start, 0);  // Mask buffer same size as chunk
    }

    std::vector<std::thread> threads;
    auto start_iaa = std::chrono::high_resolution_clock::now();  

    for (size_t i = 0; i < num_threads; i++) {
        threads.emplace_back(
            [&path, &jobs, i](std::vector<uint32_t>& sub_source, std::vector<uint32_t>& chunk_sorted, 
                              std::vector<uint32_t>& dest, std::vector<uint8_t>& mask) {
                if (jobs[i] == nullptr) return;
                iaa_sort(path, jobs[i], sub_source, dest, mask, chunk_sorted);
            },
            std::ref(sub_sources[i]), std::ref(chunk_sorted[i]), 
            std::ref(dest_buffers[i]), std::ref(mask_buffers[i])
        );
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    auto end_iaa = std::chrono::high_resolution_clock::now();
    
    for (size_t i = 0; i < num_threads; i++) {
        qpl_fini_job(jobs[i]);
    }
    
    sorted.clear();
    sorted.reserve(source.size());

    using Entry = std::pair<uint32_t, size_t>;
    std::priority_queue<Entry, std::vector<Entry>, std::greater<>> min_heap;
    std::vector<size_t> indices(num_threads, 0);
    
    for (size_t i = 0; i < num_threads; i++) {
        if (!chunk_sorted[i].empty()) {
            min_heap.emplace(chunk_sorted[i][0], i);
        }
    }
    
    while (!min_heap.empty()) {
        auto [val, chunk_idx] = min_heap.top();
        min_heap.pop();
        sorted.push_back(val);
        
        if (++indices[chunk_idx] < chunk_sorted[chunk_idx].size()) {
            min_heap.emplace(chunk_sorted[chunk_idx][indices[chunk_idx]], chunk_idx);
        }
    }

    std::chrono::duration<double> elapsed_iaa = end_iaa - start_iaa;
    std::cout << "Threaded chunked iaa_sort Time: " << elapsed_iaa.count() << " seconds" << std::endl;
}

void compress(qpl_path_t execution_path, std::vector<uint32_t> &in, std::vector<uint32_t> &out) {
    uint32_t job_size = 0;
    qpl_status status = qpl_get_job_size(execution_path, &job_size);
    if (status != QPL_STS_OK) {
        std::cerr << "Error getting job size\n";
        return;
    }

    std::unique_ptr<uint8_t[]> job_buffer = std::make_unique<uint8_t[]>(job_size);
    qpl_job* job = reinterpret_cast<qpl_job*>(job_buffer.get());

    status = qpl_init_job(execution_path, job);
    if (status != QPL_STS_OK || !job) {
        std::cerr << "Error initializing QPL job\n";
        return;
    }

    // Step 1: Compression
    job->next_in_ptr = reinterpret_cast<uint8_t*>(in.data());
    job->available_in = in.size() * sizeof(uint32_t);
    job->next_out_ptr = reinterpret_cast<uint8_t*>(out.data());
    job->available_out = out.size() * sizeof(uint32_t);
    job->op = qpl_op_compress;
    job->level = qpl_default_level;
    job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST | QPL_FLAG_DYNAMIC_HUFFMAN | QPL_FLAG_OMIT_VERIFY;

    status = qpl_execute_job(job);

    if (status != QPL_STS_OK) {
        std::cerr << "Compression failed with error: " << status << "\n";
        return;
    }

}

 /*
    for(auto s : sortedc)
        std::cout << s << " " << std::endl;
 
    std::vector<std::thread> threads;
    size_t chunk_size = source_size / num_threads;

    auto start_iaa = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < num_threads; i++) {
        size_t start = i * chunk_size;
        size_t end = (i == num_threads - 1) ? source_size : start + chunk_size;
        threads.emplace_back(iaa_sort_thread, execution_path, std::ref(source_threads), std::ref(sortedm), start, end);
    }
    
    for (auto& t : threads) {
        t.join();
    }
    auto end_iaa = std::chrono::high_resolution_clock::now();
    
    std::sort(sorted.begin(), sorted.end()); // Merge sorted chunks
    std::chrono::duration<double> elapsed_iaa = end_iaa - start_iaa;
    std::cout << "iam::sort Time: " << elapsed_iaa.count() << " seconds\n";

    if (sortedm != reference_sorted) {
        std::cout << "Sorting is incorrect!" << std::endl;
    } 
*/
