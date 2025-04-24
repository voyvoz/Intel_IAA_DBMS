#include <vector>
#include <qpl/qpl.h>

// Simple pool of pre-initialized QPL jobs for reuse
struct QplJobPool {
    std::vector<qpl_job*> jobs;
    std::vector<std::unique_ptr<uint8_t[]>> buffers;
    std::mutex lock;
    size_t next = 0;

    QplJobPool(qpl_path_t path, size_t count);
    qpl_job* acquire();
    void reset();
    ~QplJobPool();
};

// Utility function to check QPL status and throw on error
void check_qpl(qpl_status status, const char* msg);

// Initializes a QPL job and returns a pointer
qpl_job* init_qpl_job(qpl_path_t path);


// Aggregation-based sorting

void iaa_sort(qpl_path_t path, qpl_job* job, std::vector<uint32_t> &source, std::vector<uint32_t> &dest, 
    std::vector<uint8_t> &mask_after_scan, std::vector<uint32_t> &sorted, bool compress = false);

void chunked_iaa_sort(qpl_path_t path, std::vector<uint32_t>& source, std::vector<uint32_t>& sorted, bool compress = false);

// Quicksort

void iaa_quick_sort(std::vector<uint32_t>& data,
    qpl_job* job_scan, qpl_job* job_less, qpl_job* job_ge,
    uint32_t pivot,
    std::vector<uint32_t>& mask, std::vector<uint32_t>& nmask,
    std::vector<uint32_t>& temp_less, std::vector<uint32_t>& temp_greater_equal);

void chunked_iaa_quicksort(qpl_path_t path, std::vector<uint32_t>& data, std::vector<uint32_t>& sorted, size_t num_chunks = 256);

// Radix sort

void qpl_radix_sort(QplJobPool* job_pool, std::vector<uint32_t>& data,
    std::vector<std::vector<uint32_t>>& temp_buckets,
    std::vector<std::vector<uint32_t>>& masks,
    size_t bits_per_pass);

void chunked_radix_sort(QplJobPool& job_pool, std::vector<uint32_t>& data,
    size_t max_threads = 128, size_t bits_per_pass = 16);
