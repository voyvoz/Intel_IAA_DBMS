# DBMS operators with Intel IAA accelerators
Intel® In-Memory Analytics Accelerator (Intel® IAA) is a hardware accelerator designed to boost performance for data-intensive workloads.
Different SKUs come with different accelerator configurations in terms of number of accelerators and capabilities. 
It provides hardware operators to offload common tasks such as data scanning, filtering, extraction, and compression. On the programming side, it can be accessed via the Intel® QPL (Query Processing Library), where developers define jobs with specific operators to delegate these tasks to the accelerator.


This repository provides additional algorithms optimized for **DBMS workloads**, including **sort** and **join** operators. These are built on top of Intel® IAA’s hardware-accelerated primitives such as `scan`, `extract`, `select`, and `compression`.

### Implemented Operators and Algorithms

#### Sort
- **Quick Sort**
- **Radix Sort**
- **IAA Sort**: A selection-sort-like algorithm leveraging IAA's aggregation results during scans.

#### Joins
- **Nested Loop Join**
- **Sort-Merge Join**
- **Hash Join**

Each sorting algorithm is implemented in two variants:
- A **single job queue** version.
- A **chunked parallel queue** version for higher throughput.

Join algorithms can operate in a **hybrid mode**, distributing workload between the **CPU** and available **Intel IAA devices** for maximum performance.
IAASort can be used on previously compressed data by passing a flag accordingly.

> All implementations support both **software mode** (without Intel IAA devices) and **hardware-accelerated mode** (with Intel IAA devices).

### Requirements
- idxd-config (https://github.com/intel/idxd-config)
- Intel QPL (https://github.com/intel/qpl)
- C++ 23

Example usage and simple benchmarks for execution time are given in *bench*.
