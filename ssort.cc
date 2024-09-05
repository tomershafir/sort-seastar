// MIT License
//
// Copyright (c) 2024 Tomer Shafir
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
//
// External sorting is required for sorting datasets that are too large
// to fit main memory, and reside in secondary storage.
//
// There are generally 2 generic algorithm families for external sorting: 
// quicksort based and merge-sort based. The merge-sort algorithm is usually faster.
// Both are asymptotically optimal. Therefore, this is a C++ implementation of 
// an external merge-sort.
// 
// The implementation is based on the highly efficient shard per core framework Seastar.
// It distributes the work between the available shards, which avoids context switches and locks,
// and improves cache locality. It also uses an efficient I/O subsystem. The input files should be static.
//
// High level algorithm:
// 1. Partition the file and assign parts to shards.
// 2. Create sorted runs on the storage device.
// 3. Merge multiple parts at a time on each shard, until there is a single sorted file.

#include <ranges>
#include <cstdlib>
#include <cmath>
#include <vector>
#include <deque>
#include <cstring>
#include <algorithm>
#include <signal.h>
#include <atomic>

#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/prometheus.hh>
#include <seastar/http/httpd.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/core/future.hh>
#include <seastar/util/log.hh>
#include <seastar/core/signal.hh>

#define SIG_TERM_EXIT_BASE 128
#define SIGINT_EXIT SIG_TERM_EXIT_BASE + SIGINT

namespace ssort {

using seastar_file_smart = seastar::file;

// Reserved memory for userspace tasks beside the parts
static const uint64_t memory_reserve_userspace_total_bytes = 134217728; // 128Mib

static const uint64_t record_size_bytes = 4096; // 4 KiB

static const uint64_t merge_k_way = 2;

static seastar::logger ssort_logger("ssort");

struct part {
    // Unique only in a single pass scope, reused across passes
    uint64_t id;

    uint64_t start_aligned;
    uint64_t limit_aligned;
    uint64_t record_count;
    uint64_t pass;
    seastar::sstring path_cache;

    part() = default;
    part(const uint64_t id, const uint64_t start_aligned, const uint64_t limit_aligned, const uint64_t record_count, const uint64_t pass, const seastar::sstring&& path) : 
        id(id),
        start_aligned(start_aligned),
        limit_aligned(limit_aligned),
        record_count(record_count),
        pass(pass),
        path_cache(path) {}
};

class coordinator {
    const seastar::sstring& source_file_path;
    uint64_t source_file_size;
    seastar_file_smart source_file_ro_cache;
    
    uint64_t disk_read_dma_alignment_cache;
    uint64_t disk_write_dma_alignment_cache;
    
    const unsigned int shard_count;

    uint64_t memory_part_whole_bytes_aligned;
    uint64_t record_count_per_part_whole;

    uint32_t memory_part_whole_bytes_aligned_merge_read;
    uint32_t memory_part_whole_bytes_aligned_merge_write;

    uint64_t part_count_total;
    bool last_part_short;
    uint64_t part_count_per_shard_uniform;
    unsigned int extra_part_count_whole;

    uint64_t pass;
    // Dequeue is implemented similar to a hashed array tree. It supports efficient random access
    // and efficient inserts/erases at the front/back, using a cache local contiguous storage.
    std::vector<std::deque<part>> parts_per_shard;
    
    std::atomic_bool _interrupted = false;
    
    seastar::sstring to_string();
    
    seastar::sstring new_file_path(const int pass, const int part_id);

    seastar::future<> probe_block_device();
    seastar::future<> plan_memory_usage();

    seastar::future<> sort_init();
    seastar::future<> sort_part(const int shard_id, const int part_idx);
    seastar::future<> sort_all_interruptible();
    
    void merge_pass_init();
    seastar::future<> merge_two_parts(const int shard_id, const int part_idx1, const int part_idx2);
    void merge_pass_finalize();
    seastar::future<> merge_all_interruptible();

public:
    coordinator(const seastar::sstring& source_file_path);
    seastar::future<> external_sort();
    void interrupt();
    bool interrupted();
};

void coordinator::interrupt() {
    _interrupted.store(true);
}

bool coordinator::interrupted() {
    return _interrupted.load();
}

seastar::sstring coordinator::to_string() {
    seastar::sstring str = "{";
    str += "\"source_file_size\":" + seastar::to_sstring(source_file_size);
    str += ",\"disk_read_dma_alignment\":" + seastar::to_sstring(disk_read_dma_alignment_cache);
    str += ",\"disk_write_dma_alignment\":" + seastar::to_sstring(disk_write_dma_alignment_cache);
    str += ",\"shard_count\":" + seastar::to_sstring(shard_count);
    str += ",\"pass\":" + seastar::to_sstring(pass);
    str += ",\"parts_per_shard\":[";
    unsigned int s = 0;
    for (; s < shard_count; ++s) {
        if (s > 0) {
            str += ",";
        }
        str += "{\"shard\":" + seastar::to_sstring(s);
        str += ",\"parts\":[";
        for (unsigned int p = 0; p < parts_per_shard[s].size(); ++p) {
            auto& sp = parts_per_shard[s][p];
            if (p > 0) {
                str += ",";
            }
            str += "{\"id\":" + seastar::to_sstring(sp.id);
            str += ",\"start_aligned\":" + seastar::to_sstring(sp.start_aligned);
            str += ",\"limit_aligned\":" + seastar::to_sstring(sp.limit_aligned);
            str += ",\"record_count\":" + seastar::to_sstring(sp.record_count);
            str += ",\"pass\":" + seastar::to_sstring(sp.pass);
            str += ",\"path\":\"" + sp.path_cache;
            str += "\"}";
        }
        str += "]}";
    }
    str += "]}";
    return str;
}

coordinator::coordinator(const seastar::sstring& source_file_path) :
    source_file_path(source_file_path),
    shard_count(seastar::smp::count),
    parts_per_shard(shard_count) /*reserves space for vector and default inserts dequeues*/ {}

seastar::sstring coordinator::new_file_path(const int pass, const int part_id) {
    return source_file_path + ".sorted." + seastar::to_sstring(pass) + "." + seastar::to_sstring(part_id);
}

// Probe the attached block device
seastar::future<> coordinator::probe_block_device() {
    source_file_ro_cache = co_await seastar::open_file_dma(source_file_path, seastar::open_flags::ro);
    disk_read_dma_alignment_cache = source_file_ro_cache.disk_read_dma_alignment();
    disk_write_dma_alignment_cache = source_file_ro_cache.disk_write_dma_alignment();
    assert(record_size_bytes == disk_read_dma_alignment_cache 
        && record_size_bytes == disk_write_dma_alignment_cache
        && "bad block size");
}

// Plans memory usage, to saturate available memory and also not OOM
seastar::future<> coordinator::plan_memory_usage() {
    auto memory_parts_total_bytes = seastar::memory::stats().total_memory()/*After OS reservation*/ - memory_reserve_userspace_total_bytes;
    source_file_size = co_await source_file_ro_cache.size();
    record_count_per_part_whole = std::min(source_file_size, memory_parts_total_bytes) / shard_count / record_size_bytes;
    memory_part_whole_bytes_aligned = record_count_per_part_whole * record_size_bytes;

    memory_part_whole_bytes_aligned_merge_read = record_count_per_part_whole / (merge_k_way + 1) * record_size_bytes;
    memory_part_whole_bytes_aligned_merge_write = memory_part_whole_bytes_aligned_merge_read;
}

seastar::future<> coordinator::sort_init() {
    ++pass;

    co_await probe_block_device();    
    co_await plan_memory_usage();

    part_count_total = source_file_size / memory_part_whole_bytes_aligned;
    last_part_short = source_file_size % memory_part_whole_bytes_aligned > 0;
    part_count_per_shard_uniform = part_count_total / shard_count;
    extra_part_count_whole = part_count_total % shard_count;
    
    if (last_part_short) {
        ++part_count_total;
    }

    // Assign sequential parts to shards
    uint64_t accum = 0, id = 0;
    for (unsigned int s = 0; s < shard_count; ++s) {
        for (unsigned int _ = 0; _ < part_count_per_shard_uniform; ++_) {
            parts_per_shard[s].emplace_back(id, accum, memory_part_whole_bytes_aligned, record_count_per_part_whole, pass, new_file_path(pass, id));
            ++id;
            accum += memory_part_whole_bytes_aligned;
        }
        // Assign extra parts sequentially
        if (s < extra_part_count_whole) {
            parts_per_shard[s].emplace_back(id, accum, memory_part_whole_bytes_aligned, record_count_per_part_whole, pass, new_file_path(pass, id));
            ++id;
            accum += memory_part_whole_bytes_aligned;
        }
    }
    // Assign the last short part to the last shard
    if (last_part_short) {
        auto memory_part_short_bytes_aligned = source_file_size - accum;
        parts_per_shard[shard_count - 1].emplace_back(id, accum, memory_part_short_bytes_aligned, memory_part_short_bytes_aligned / record_size_bytes, pass, new_file_path(pass, id));
    }
    ssort_logger.debug("{}", to_string());
}

static int cmp(const void *rhs, const void *lhs) {
    return strncmp((const char*)rhs, (const char*)lhs, record_size_bytes);
}

seastar::future<> coordinator::sort_part(const int shard_id, const int part_idx) {
    // Sequential read of a part from the storage
    // dma_read() args must be disk_read_dma_alignment() aligned
    auto& part = parts_per_shard[shard_id][part_idx];
    auto in = co_await seastar::open_file_dma(source_file_path, seastar::open_flags::ro);
    auto buf = seastar::allocate_aligned_buffer<char>(part.limit_aligned, disk_write_dma_alignment_cache);
    auto buf_ptr = buf.get();
    co_await in.dma_read<char>(part.start_aligned, buf_ptr, part.limit_aligned);
    
    // Sort part in memory
    qsort(buf_ptr, part.record_count, record_size_bytes, cmp);

    // Sequantial write of a new sorted part to the storage
    // dma_write() args must be disk_write_dma_alignment() aligned
    auto out = co_await seastar::open_file_dma(part.path_cache, seastar::open_flags::create | seastar::open_flags::truncate | seastar::open_flags::rw);
    co_await out.dma_write<char>(0, buf_ptr, part.limit_aligned);

    // Zero start in all parts
    part.start_aligned = 0;
}

seastar::future<> coordinator::sort_all_interruptible() {
    if (_interrupted.load()) {
        co_return;
    }
    co_await sort_init();
    if (_interrupted.load()) {
        co_return;
    }
    co_await seastar::parallel_for_each(std::views::iota(0, static_cast<int>(shard_count)), [this] (int shard_id) {
        if (!parts_per_shard[shard_id].empty()) {
            return seastar::smp::submit_to(shard_id, [this, shard_id] {
                return seastar::do_with(0, [this, shard_id] (int part_idx) -> seastar::future<> {
                    auto part_count_per_shard = parts_per_shard[shard_id].size();
                    co_await seastar::do_until(
                        [this, part_count_per_shard, &part_idx] { return part_idx >= part_count_per_shard || _interrupted.load(); }, 
                        [this, shard_id, &part_idx] -> seastar::future<> {
                            auto f = sort_part(shard_id, part_idx);
                            ++part_idx;
                            return f;
                        }
                    );
                });
            });
        }
        return seastar::make_ready_future<>();
    });
}

void coordinator::merge_pass_init() {
    ++pass;
    
    // A greedy load balancing algorithm to associate sequential part pairs to shards for the next merge.
    for (unsigned int s = 0; s < shard_count - 1;) {
        auto n = s + 1;
        if (1 == parts_per_shard[s].size() % 2) {
            for (; n < shard_count && parts_per_shard[n].empty(); ++n);
            if (n < shard_count) {
                parts_per_shard[s].push_back(parts_per_shard[n].front());
                parts_per_shard[n].pop_front();
            }
        }
        s = n;
    }
    ssort_logger.debug("{}", to_string());
}

static bool cmp(const seastar::temporary_buffer<char>& lhs, const seastar::temporary_buffer<char>& rhs) {
    return std::strncmp(lhs.get(), rhs.get(), record_size_bytes) < 0;
}

seastar::future<> coordinator::merge_two_parts(const int shard_id, const int part_idx1, const int part_idx2) {
    auto& p1 = parts_per_shard[shard_id][part_idx1];
    auto& p2 = parts_per_shard[shard_id][part_idx2];
    
    auto in_file1 = co_await seastar::open_file_dma(p1.path_cache, seastar::open_flags::ro);
    auto in_buf1 = seastar::make_file_input_stream(
        std::move(in_file1),
        p1.start_aligned,
        {.buffer_size = memory_part_whole_bytes_aligned_merge_read, .read_ahead = 0, .dynamic_adjustments = nullptr}
    );
    auto in_file2 = co_await seastar::open_file_dma(p2.path_cache, seastar::open_flags::ro);
    auto in_buf2 = seastar::make_file_input_stream(
        std::move(in_file2),
        p2.start_aligned,
        {.buffer_size = memory_part_whole_bytes_aligned_merge_read, .read_ahead = 0, .dynamic_adjustments = nullptr}
    );
    auto out_file = co_await seastar::open_file_dma(new_file_path(pass, p1.id), seastar::open_flags::create | seastar::open_flags::truncate | seastar::open_flags::rw);
    auto out_buf = co_await seastar::make_file_output_stream(
        std::move(out_file), 
        {.buffer_size = memory_part_whole_bytes_aligned_merge_write, .preallocation_size = 0, .write_behind = 1}
    );

    // Parts are not empty
    auto buf1 = co_await in_buf1.read_exactly(record_size_bytes);
    auto buf2 = co_await in_buf2.read_exactly(record_size_bytes);
    // Empty buffer from read_exactly() effectively means EOF for fixed length string aligned file
    while (!buf1.empty() && !buf2.empty()) {
        if (cmp(buf1, buf2)) {
            co_await out_buf.write(buf1.get(), record_size_bytes);
            buf1 = co_await in_buf1.read_exactly(record_size_bytes);
        } else {
            co_await out_buf.write(buf2.get(), record_size_bytes);
            buf2 = co_await in_buf2.read_exactly(record_size_bytes);
        }
    }
    // Merge remainder from part 1
    while (!buf1.empty()) {
        co_await out_buf.write(buf1.get(), record_size_bytes);
        buf1 = co_await in_buf1.read_exactly(record_size_bytes);
    }
    // Merge remainder from part 2
    while (!buf2.empty()) {
        co_await out_buf.write(buf2.get(), record_size_bytes);
        buf2 = co_await in_buf2.read_exactly(record_size_bytes);
    }

    // Flush output buffer and close the stream and the underlying file
    co_await out_buf.close();
    
    // Unmap old files after merge is finished
    co_await seastar::remove_file(p1.path_cache);
    co_await seastar::remove_file(p2.path_cache);
}

void coordinator::merge_pass_finalize() {
    // Squash merged parts
    for (unsigned int s = 0; s < shard_count; ++s) {
        auto part_count_per_shard = parts_per_shard[s].size();
        if (part_count_per_shard > 1) {
            for (uint64_t _ = 0; _ < part_count_per_shard / 2; ++_) {
                auto p1 = parts_per_shard[s].front();
                parts_per_shard[s].pop_front();
                auto p2 = parts_per_shard[s].front();
                parts_per_shard[s].pop_front();
                parts_per_shard[s].emplace_back(p1.id, p1.start_aligned, p1.limit_aligned + p2.limit_aligned, p1.record_count + p2.record_count, pass, new_file_path(pass, p1.id));
                --part_count_total;
            }
            // Rotate a remainder part to maintain sequentiality
            if (1 == part_count_per_shard % 2) {
                auto p1 = parts_per_shard[s].front();
                parts_per_shard[s].pop_front();
                parts_per_shard[s].push_back(p1);
            }
        }
    }
}

seastar::future<> coordinator::merge_all_interruptible() {
    if (_interrupted.load()) {
        co_return;
    }
    while (part_count_total > 1) {
        merge_pass_init();
        if (_interrupted.load()) {
            co_return;
        }
        co_await seastar::parallel_for_each(std::views::iota(0, static_cast<int>(shard_count)), [this] (int shard_id) {
            auto part_count_per_shard = parts_per_shard[shard_id].size();
            if (part_count_per_shard > 1) {
                return seastar::smp::submit_to(shard_id, [this, shard_id, part_count_per_shard] {
                    return seastar::do_with(0, [this, shard_id, part_count_per_shard] (int part_idx) -> seastar::future<> {
                        auto part_idx_limit = part_count_per_shard - 1;
                        co_await seastar::do_until(
                            [this, &part_idx, part_idx_limit] { return part_idx >= part_idx_limit || _interrupted.load(); }, 
                            [this, shard_id, &part_idx] {
                                auto f = merge_two_parts(shard_id, part_idx, part_idx + 1);
                                part_idx += 2;
                                return f;
                            }
                        );
                    });
                });
            } 
            return seastar::make_ready_future<>();
        });
        if (_interrupted.load()) {
            break;
        }
        merge_pass_finalize();
    }
    if (_interrupted.load()) {
        co_return;
    }
    // Remap final sorted file, the algorithm guarantees that parts_per_shard[0][0].path stores the path
    co_await seastar::rename_file(parts_per_shard[0][0].path_cache, source_file_path + ".sorted");
}

seastar::future<> coordinator::external_sort() {
    co_await sort_all_interruptible();
    co_await merge_all_interruptible();
}

}

int main(int argc, char** argv) {
    seastar::app_template::config cfg;
    cfg.auto_handle_sigint_sigterm = false;
    seastar::app_template app(std::move(cfg));

    static const auto arg_path = "path";
    // The sorted output is written into the path <path>.sorted
    app.add_positional_options({{
        arg_path, boost::program_options::value<seastar::sstring>()->default_value({}), "path of the file to sort", -1
    }});

    app.add_options()("prometheus_port", boost::program_options::value<uint16_t>()->default_value(9180), "Port to listen on. Set to zero in order to disable Prometheus.");
    app.add_options()("prometheus_address", boost::program_options::value<seastar::sstring>()->default_value("0.0.0.0"), "Address of the prometheus server.");

    // app.run() is noexcept
    return app.run(argc, argv, [&app] -> seastar::future<> {
        try {
            auto cfg = app.configuration();

            ssort::coordinator c(cfg[arg_path].as<seastar::sstring>());
            
            // Handle SIGINT/Ctrl+C
            seastar::handle_signal(SIGINT, [&c] {
                ssort::ssort_logger.info("{}", "Interrupted, shutting down...\n");
                c.interrupt();
            }, true);

            // Setup Prometheus server
            seastar::httpd::http_server_control prometheus_server;
            auto prom_port = cfg["prometheus_port"].as<uint16_t>();
            if (prom_port) {
                seastar::prometheus::config pctx;
                seastar::net::inet_address prom_addr(cfg["prometheus_address"].as<seastar::sstring>());
                pctx.metric_help = "ssort tool statistics";
                pctx.prefix = "ssort";

                co_await prometheus_server.start("prometheus");
                co_await seastar::prometheus::start(prometheus_server, pctx);
                co_await prometheus_server.listen(seastar::socket_address{prom_addr, prom_port})
                    .handle_exception([prom_addr, prom_port] (auto e) {
                        ssort::ssort_logger.error("Could not start Prometheus server on {}:{}\n", prom_addr, prom_port, e);
                        return seastar::make_exception_future<>(e);
                    });
            }
            
            // Perform external sort
            co_await c.external_sort();

            co_await prometheus_server.stop();

            // Waiting on external_sort() which supports interruption across shards,
            // implicitly forms a barrier
            if (c.interrupted()) {
                exit(SIGINT_EXIT);
            }
        } catch (...) {
            ssort::ssort_logger.error("Failed to run: {}", std::current_exception());
        }
    });
}
