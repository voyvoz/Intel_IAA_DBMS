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

#include "utils.hpp"
#include "sort.hpp"


constexpr const uint32_t source_size         = 1024 * 1024 * 128;

auto main(int argc, char** argv) -> int {
    std::cout << "Intel(R) Query Processing Library version is " << qpl_get_library_version() << ".\n";

    qpl_path_t execution_path = qpl_path_hardware;
    if (parse_execution_path(argc, argv, &execution_path, 0) != 0) { return 1; }

    std::vector<uint32_t> source(source_size);
    std::vector<uint32_t> sorted(source_size, 0);
    std::vector<uint32_t> sortedm(source_size, 0);
    std::vector<uint32_t> sortedc(source_size, 0);
    std::iota(source.begin(), source.end(), 1);
    std::reverse(source.begin(), source.end());

    //uint32_t compression_size = qpl_get_safe_deflate_compression_buffer_size(source_size);
    std::vector<uint32_t> comp_source(source_size, 0);

    std::vector<uint32_t> source_iaa = source;
    std::vector<uint32_t> source_threads = source;
    std::vector<uint32_t> reference_sorted = source;

    //compress(execution_path, source, comp_source);

    auto start_std = std::chrono::high_resolution_clock::now();

    std::sort(reference_sorted.begin(), reference_sorted.end());

    auto end_std = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_std = end_std - start_std;
    std::cout << "std::sort Time: " << elapsed_std.count() << std::endl;

    chunked_iaa_sort(execution_path, source, sortedc, false);
  
    if (sortedc != reference_sorted) {
        std::cout << "Sorting is incorrect!" << std::endl;
    } 

    return 0;
}
