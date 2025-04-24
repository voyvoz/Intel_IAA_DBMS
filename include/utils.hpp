#pragma once

#include <vector>
#include <cstdint>
#include <qpl/qpl.h> 

// Parses the execution path from command-line arguments
int parse_execution_path(int argc, char** argv, qpl_path_t* path_ptr, int extra_arg);

// Fills a vector with unique random uint32_t values
void insert_random(std::vector<uint32_t>& vec);

// Fills a vector with unique random uint8_t values
void insert_random(std::vector<uint8_t>& vec);

// Fills a vector with a fixed value (currently hardcoded to 5)
void insert_seq(std::vector<uint32_t>& vec);

// Prints the positions of 1-bits in a uint32_t
void printBitPositions(uint32_t number);

// Inserts unique values in a given range and shuffles them
void insert_unique_random(std::vector<uint32_t>& vec, uint32_t size, uint32_t min_value, uint32_t max_value);
