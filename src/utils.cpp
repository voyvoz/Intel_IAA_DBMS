#include <vector>
#include <random>
#include <iostream>
#include <set>
#include <algorithm>

#include "utils.hpp"

int parse_execution_path(int argc, char** argv, qpl_path_t* path_ptr, int extra_arg = 0) {
    // Get path from input argument
    if (extra_arg == 0) {
        if (argc < 2) {
            std::cout << "Missing the execution path as the first parameter. Use either hardware_path or software_path."
                      << std::endl;
            return 1;
        }
    } else {
        if (argc < 3) {
            std::cout
                    << "Missing the execution path as the first parameter and/or the dataset path as the second parameter."
                    << std::endl;
            return 1;
        }
    }

    const std::string path = argv[1];
    if (path == "hardware_path") {
        *path_ptr = qpl_path_hardware;
        std::cout << "The example will be run on the hardware path." << std::endl;
    } else if (path == "software_path") {
        *path_ptr = qpl_path_software;
        std::cout << "The example will be run on the software path." << std::endl;
    } else if (path == "auto_path") {
        *path_ptr = qpl_path_auto;
        std::cout << "The example will be run on the auto path." << std::endl;
    } else {
        std::cout << "Unrecognized value for parameter. Use hardware_path, software_path or auto_path." << std::endl;
        return 1;
    }

    return 0;
}

void insert_random(std::vector<uint32_t> &vec) {
    std::random_device rd;  
    std::mt19937 gen(rd()); 
    std::uniform_int_distribution<uint32_t> dist(0, 2323232); 

    std::set<uint8_t> unique_numbers; // Track unique values

    while (unique_numbers.size() < vec.size()) {
        uint32_t num = dist(gen);
        if (unique_numbers.insert(num).second) { // Inserts only if unique
            vec[unique_numbers.size() - 1] = num;
        }
    }
}

void insert_random(std::vector<uint8_t> &vec) {
    std::random_device rd;  
    std::mt19937 gen(rd()); 
    std::uniform_int_distribution<uint8_t> dist(0, 255); 

    std::set<uint8_t> unique_numbers; // Track unique values

    while (unique_numbers.size() < vec.size()) {
        uint32_t num = dist(gen);
        if (unique_numbers.insert(num).second) { // Inserts only if unique
            vec[unique_numbers.size() - 1] = num;
        }
    }
}

void insert_seq(std::vector<uint32_t> &vec) {
    int i = 0;
    for (auto& byte : vec) {
        byte = 5;
    }
}

void printBitPositions(uint32_t number) {
    const int bitSize = 32; // Size of uint32_t in bits
    std::vector<int> positions;

    // Convert to bit vector and collect positions of 1's
    for (int i = 0; i < bitSize; ++i) {
        if (number & (1U << i)) { // Check if bit at position i is set
            positions.push_back(i);
        }
    }

    // Print positions of 1's
    std::cout << "Positions of 1's: ";
    for (int pos : positions) {
        std::cout << pos << " ";
    }
    std::cout << std::endl;
}

void insert_unique_random(std::vector<uint32_t> &vec, uint32_t size, uint32_t min_value = 0, uint32_t max_value = UINT32_MAX) {
    if (size > (max_value - min_value + 1)) {
        throw std::runtime_error("Size exceeds unique value range!");
    }

    std::iota(vec.begin(), vec.end(), min_value); // Fill with sequential numbers

    std::random_device rd;
    std::mt19937 rng(rd()); // High-quality random generator
    std::shuffle(vec.begin(), vec.end(), rng); // Shuffle elements
}
