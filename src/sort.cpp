#include <vector>
#include <mutex>
#include <qpl/qpl.h>
#include <stdexcept>
#include <memory>

#include "sort.hpp"

void check_qpl(qpl_status status, const char* msg) {
    if (status != QPL_STS_OK) {
        throw std::runtime_error(std::string(msg) + ": " + std::to_string(status));
    }
}

qpl_job* init_qpl_job(qpl_path_t path) {
    uint32_t job_size = 0;
    check_qpl(qpl_get_job_size(path, &job_size), "get_job_size");
    void* buffer = std::malloc(job_size);
    if (!buffer) throw std::bad_alloc();
    qpl_job* job = reinterpret_cast<qpl_job*>(buffer);
    check_qpl(qpl_init_job(path, job), "init_job");
    return job;
}

QplJobPool::QplJobPool(qpl_path_t path, size_t count) {
    uint32_t size = 0;
    check_qpl(qpl_get_job_size(path, &size), "get_job_size");

    for (size_t i = 0; i < count; ++i) {
        buffers.emplace_back(std::make_unique<uint8_t[]>(size));
        qpl_job* job = reinterpret_cast<qpl_job*>(buffers.back().get());
        check_qpl(qpl_init_job(path, job), "init_job");
        jobs.push_back(job);
    }
}

qpl_job* QplJobPool::acquire() {
    std::lock_guard<std::mutex> guard(lock);
    if (next == jobs.size()) next = 0;
    return jobs[next++];
}

void QplJobPool::reset() {
    std::lock_guard<std::mutex> guard(lock);
    next = 0;
}

QplJobPool::~QplJobPool() {
    for (auto job : jobs) {
        qpl_fini_job(job);
    }
}