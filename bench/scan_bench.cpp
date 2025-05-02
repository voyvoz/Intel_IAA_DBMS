#include <iostream>
#include <memory>
#include <numeric>
#include <vector>
#include <chrono>
#include <thread>
#include <future>  
#include "qpl/qpl.h"

#include "utils.hpp" // For argument parsing

constexpr const uint32_t input_bit_width     = 8;
constexpr const uint32_t output_vector_width = 32;
constexpr const uint32_t value_to_find       = 48;
constexpr const uint32_t byte_bit_length     = 8;

void print_source_size(uint32_t size) {
    if (size >= (1024 * 1024 * 1024)) {
        std::cout << "Source Size: " << size / (1024 * 1024 * 1024) << " GB";
    } else if (size >= (1024 * 1024)) {
        std::cout << "Source Size: " << size / (1024 * 1024) << " MB";
    } else if (size >= 1024) {
        std::cout << "Source Size: " << size / 1024 << " KB";
    } else {
        std::cout << "Source Size: " << size << " Bytes";
    }
}

double benchmark_scan(qpl_path_t execution_path, uint8_t* source, uint8_t* destination, uint32_t size) {
    std::unique_ptr<uint8_t[]> job_buffer;
    uint32_t                   job_size = 0;
    qpl_status status = qpl_get_job_size(execution_path, &job_size);
    if (status != QPL_STS_OK) {
        std::cerr << "Error " << status << " while getting job size.\n";
        return -1;
    }

    job_buffer   = std::make_unique<uint8_t[]>(job_size);
    qpl_job* job = reinterpret_cast<qpl_job*>(job_buffer.get());

    status = qpl_init_job(execution_path, job);
    if (status != QPL_STS_OK) {
        std::cerr << "Error " << status << " while initializing job.\n";
        return -1;
    }

    job->next_in_ptr        = source;
    job->available_in       = size;
    job->next_out_ptr       = destination;
    job->available_out      = size;
    job->op                 = qpl_op_scan_eq;
    job->src1_bit_width     = input_bit_width;
    job->num_input_elements = size;
    job->out_bit_width      = qpl_ow_nom;
    job->param_low          = value_to_find;
    job->flags              = QPL_FLAG_OMIT_CHECKSUMS |
                              QPL_FLAG_OMIT_AGGREGATES;

    auto start_time = std::chrono::high_resolution_clock::now();
    status = qpl_execute_job(job);
    auto end_time = std::chrono::high_resolution_clock::now();

    if (status != QPL_STS_OK) {
        std::cerr << "Error " << status << " during scan execution.\n";
        return -1;
    }

    double execution_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
    qpl_fini_job(job);

    return execution_time_ms;
}

void benchmark_multithreaded(qpl_path_t execution_path, uint32_t source_size, int num_threads) {
    std::vector<uint8_t> source(source_size, 0);
    std::vector<uint8_t> destination(source_size, 4);

    std::iota(std::begin(source), std::end(source), 0);
    source[3] = 48;  // Ensuring at least one occurrence of the value

    uint32_t chunk_size = source_size / num_threads;
    std::vector<std::future<double>> futures;

    auto start_time = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < num_threads; ++i) {
        uint8_t* src_part = source.data() + i * chunk_size;
        uint8_t* dest_part = destination.data() + i * chunk_size;
        futures.push_back(std::async(std::launch::async, benchmark_scan, execution_path, src_part, dest_part, chunk_size));
    }

    double total_execution_time = 0;
    for (auto& f : futures) {
        total_execution_time = std::max(total_execution_time, f.get());  
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    double actual_execution_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
    double throughput_mbps = (source_size / (actual_execution_time_ms / 1000.0)) / (1024 * 1024);

    print_source_size(source_size);
    std::cout << ", Threads: " << num_threads
              << ", Execution Time: " << actual_execution_time_ms << " ms"
              << ", Throughput: " << throughput_mbps << " MB/s\n";
}

int main(int argc, char** argv) {
    std::cout << "Intel(R) Query Processing Library version is " << qpl_get_library_version() << ".\n";

    qpl_path_t execution_path = qpl_path_software;
    if (parse_execution_path(argc, argv, &execution_path, 0) != 0) {
        return 1;
    }

    std::vector<uint32_t> source_sizes = {128 * 1024 * 1024,
                                          256 * 1024 * 1024, 512 * 1024 * 1024, 1 * 1024 * 1024 * 1024};

    std::vector<int> thread = {1, 2, 4, 8, 16, 32, 64, 96, 128, 256, 384};
    //std::vector<int> thread = {1, 2, 4}; 
    for (uint32_t size : source_sizes) {
        for (auto num_threads: thread) {
            benchmark_multithreaded(execution_path, size, num_threads);
        }
    }

    return 0;
}

