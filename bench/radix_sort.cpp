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

int main(int argc, char** argv) {
    qpl_path_t execution_path = qpl_path_hardware;
    if (parse_execution_path(argc, argv, &execution_path, 0) != 0) return 1;

    QplJobPool job_pool(execution_path, 2048);

    std::vector<uint32_t> data(1024 * 1024); // 1M elements
    std::iota(data.begin(), data.end(), 1);
    std::reverse(data.begin(), data.end());

    std::cout << "Start full chunked radix sort\n";
    auto start = std::chrono::high_resolution_clock::now();

    chunked_radix_sort(job_pool, data, 128, 24);  // Pass bits_per_pass = 16 (or 8, 24, etc.)

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    std::cout << "Full chunked radix sort time: " << elapsed.count() << " sec\n";

    for (size_t i = 0; i < std::min<size_t>(data.size(), 20); ++i)
        std::cout << data[i] << " ";
    std::cout << "...\n";

    return 0;
}