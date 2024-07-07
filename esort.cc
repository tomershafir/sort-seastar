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

#include <iostream>
#include <memory>
#include <ranges>
#include <cstdlib>
#include <string>
#include <cmath>
#include <vector>
#include <deque>
#include <cstring>
#include <algorithm>

#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/sstring.hh>

namespace esort {

// Reserved memory for userspace tasks beside the parts
static const size_t memory_reserve_userspace_total_bytes = 134217728; // 128Mib

static const size_t record_size_bytes = 4096; // 4 KiB

struct part {
    // Unique only in a single pass scope, reused across passes
    uint64_t id;

    uint64_t start;
    size_t limit;
    size_t pass;
    seastar::sstring path;

    part(const uint64_t id, const uint64_t start, const size_t limit, const size_t pass, const seastar::sstring path) : 
        id(id), start(start), limit(limit), pass(pass), path(path) {}
};

class coordinator {
    const seastar::sstring& source_file_path;
    size_t source_file_size;
    
    const unsigned int shard_count;
    size_t memory_part_bytes_aligned;

    size_t part_count_total;
    bool last_part_short;
    size_t part_count_per_shard_uniform;
    unsigned int extra_part_count_whole;

    size_t pass;
    // Dequeue is implemented similar to a hashed array tree. It supports efficient random access
    // and efficient inserts/erases at the front/back.
    std::vector<std::deque<part>> parts_per_shard;

    seastar::sstring new_file_path(const int pass, const int part_id);

    seastar::future<> plan_memory_usage();
    seastar::future<> sort_init();
    seastar::future<> sort_part(const int shard_id, const int part_idx);
    seastar::future<> sort_all();
    
    void merge_pass_init();
    seastar::future<> merge_two_parts(const int shard_id, const int part_idx1, const int part_idx2);
    void merge_pass_finalize();
    seastar::future<> merge_all();

public:
    coordinator(const seastar::sstring& source_file_path);
    seastar::future<> external_sort();
};

coordinator::coordinator(const seastar::sstring& source_file_path) :
    source_file_path(source_file_path), 
    shard_count(seastar::smp::count),
    parts_per_shard(shard_count) {}

void assert_block_size(seastar::file f) {
    assert(record_size_bytes == f.disk_read_dma_alignment() 
        && record_size_bytes == f.disk_write_dma_alignment()
        && "bad block size");
}

seastar::sstring coordinator::new_file_path(const int pass, const int part_id) {
    return source_file_path + ".sorted." + seastar::to_sstring(pass) + "." + seastar::to_sstring(part_id);
}

// Plans memory usage, to saturate available memory and also not OOM
seastar::future<> coordinator::plan_memory_usage() {
    auto f = co_await seastar::open_file_dma(source_file_path, seastar::open_flags::ro);
    assert_block_size(f);
    auto memory_parts_total_bytes = seastar::memory::stats().total_memory()/*After OS reservation*/ - memory_reserve_userspace_total_bytes;
    source_file_size = co_await f.size();
    auto record_count_per_part_aligned = std::min(source_file_size, memory_parts_total_bytes) / shard_count / record_size_bytes;
    memory_part_bytes_aligned = record_count_per_part_aligned * record_size_bytes;
}

seastar::future<> coordinator::sort_init() {
    ++pass;
    
    co_await plan_memory_usage();

    part_count_total = source_file_size / memory_part_bytes_aligned;
    last_part_short = source_file_size % memory_part_bytes_aligned > 0;
    part_count_per_shard_uniform = part_count_total / shard_count;
    extra_part_count_whole = part_count_total % shard_count;
    
    if (last_part_short) {
        ++part_count_total;
    }

    // Assign sequential parts to shards
    uint64_t accum = 0, id = 0;
    for (unsigned int s = 0; s < shard_count; ++s) {
        for (unsigned int _ = 0; _ < part_count_per_shard_uniform; ++_) {
            parts_per_shard[s].emplace_back(id, accum, memory_part_bytes_aligned, pass, new_file_path(pass, id));
            ++id;
            accum += memory_part_bytes_aligned;
        }
        // Assign extra parts sequentially
        if (s < extra_part_count_whole) {
            parts_per_shard[s].emplace_back(id, accum, memory_part_bytes_aligned, pass, new_file_path(pass, id));
            ++id;
            accum += memory_part_bytes_aligned;
        }
    }
    // Assign the last short part to the last shard
    if (last_part_short) {
        parts_per_shard[shard_count - 1].emplace_back(id, accum, source_file_size - accum, pass, new_file_path(pass, id));
    }
}

static bool unwrap_strncmp(const seastar::temporary_buffer<char>& lhs, const seastar::temporary_buffer<char>& rhs) {
    return std::strncmp(lhs.get(), rhs.get(), record_size_bytes) < 0;
}

seastar::future<> coordinator::sort_part(const int shard_id, const int part_idx) {
    std::vector<seastar::temporary_buffer<char>> records;

    // Read part from storage
    auto& part = parts_per_shard[shard_id][part_idx];
    auto in = co_await seastar::open_file_dma(source_file_path, seastar::open_flags::ro);
    auto offset_read_limit = part.start + part.limit;
    for (auto offset = part.start; offset < offset_read_limit; offset += record_size_bytes) {
        auto buf = co_await in.dma_read<char>(offset, record_size_bytes);
        records.push_back(std::move(buf));
    }

    // Sort part in memory, char case matters
    std::sort(std::begin(records), std::end(records), unwrap_strncmp);

    // Write a new sorted part to storage
    auto out = co_await seastar::open_file_dma(part.path, seastar::open_flags::create | seastar::open_flags::truncate | seastar::open_flags::rw);
    auto records_count = records.size();
    uint64_t offset = 0;
    for (size_t r = 0; r < records_count; ++r, offset += record_size_bytes) {
        co_await out.dma_write<char>(offset, records[r].get(), record_size_bytes);
    }
    // Zero start in all parts
    part.start = 0;
}

seastar::future<> coordinator::sort_all() {
    co_await sort_init();
    co_await seastar::parallel_for_each(std::views::iota(0, static_cast<int>(shard_count)), [this] (int shard_id) {
        if (!parts_per_shard[shard_id].empty()) {
            return seastar::smp::submit_to(shard_id, [this, shard_id] mutable {
                const auto r = std::views::iota(0, static_cast<int>(parts_per_shard[shard_id].size()));
                return seastar::do_for_each(r, [this, shard_id] (int part_idx) mutable {
                    return sort_part(shard_id, part_idx);
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
}

seastar::future<> coordinator::merge_two_parts(const int shard_id, const int part_idx1, const int part_idx2) {
    auto& p1 = parts_per_shard[shard_id][part_idx1];
    auto& p2 = parts_per_shard[shard_id][part_idx2];
    auto in1 = co_await seastar::open_file_dma(p1.path, seastar::open_flags::ro);
    auto in2 = co_await seastar::open_file_dma(p2.path, seastar::open_flags::ro);
    auto out = co_await seastar::open_file_dma(new_file_path(pass, p1.id), seastar::open_flags::create | seastar::open_flags::truncate | seastar::open_flags::rw);

    // Parts are not empty
    auto offset_read1 = p1.start;
    auto offset_read_limit1 = p1.start + p1.limit;
    auto buf1 = co_await in1.dma_read<char>(offset_read1, record_size_bytes);
    auto offset_read2 = p2.start;
    auto offset_read_limit2 = p2.start + p2.limit;
    auto buf2 = co_await in2.dma_read<char>(offset_read2, record_size_bytes);
    uint64_t offset_write = 0;
    while (true) {
        if (unwrap_strncmp(buf1, buf2)) {
            co_await out.dma_write<char>(offset_write, buf1.get(), record_size_bytes);
            offset_read1 += record_size_bytes;
            if (offset_read1 >= offset_read_limit1) {
                break;
            }
            buf1 = co_await in1.dma_read<char>(offset_read1, record_size_bytes);
        } else {
            co_await out.dma_write<char>(offset_write, buf2.get(), record_size_bytes);
            offset_read2 += record_size_bytes;
            if (offset_read2 >= offset_read_limit2) {
                break;
            }
            buf2 = co_await in2.dma_read<char>(offset_read2, record_size_bytes);
        }
        offset_write += record_size_bytes;
    }
    offset_write += record_size_bytes;
    for (; offset_read1 < offset_read_limit1; offset_read1 += record_size_bytes, offset_write += record_size_bytes) {
        buf1 = co_await in1.dma_read<char>(offset_read1, record_size_bytes);
        co_await out.dma_write<char>(offset_write, buf1.get(), record_size_bytes);
    }
    for (; offset_read2 < offset_read_limit2; offset_read2 += record_size_bytes, offset_write += record_size_bytes) {
        buf2 = co_await in2.dma_read<char>(offset_read2, record_size_bytes);
        co_await out.dma_write<char>(offset_write, buf2.get(), record_size_bytes);
    }

    // Unmap old files after merge is finished
    co_await seastar::remove_file(p1.path);
    co_await seastar::remove_file(p2.path);
}

void coordinator::merge_pass_finalize() {
    // Squash merged parts
    for (unsigned int s = 0; s < shard_count; ++s) {
        auto part_count_per_shard = parts_per_shard[s].size();
        if (part_count_per_shard > 1) {
            for (size_t _ = 0; _ < part_count_per_shard / 2; _+=2) {
                auto p1 = parts_per_shard[s].front();
                parts_per_shard[s].pop_front();
                auto p2 = parts_per_shard[s].front();
                parts_per_shard[s].pop_front();
                parts_per_shard[s].emplace_back(p1.id, p1.start, p1.limit + p2.limit, pass, new_file_path(pass, p1.id));
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

seastar::future<> coordinator::merge_all() {
    while (part_count_total > 1) {
        merge_pass_init();
        co_await seastar::parallel_for_each(std::views::iota(0, static_cast<int>(shard_count)), [this] (int shard_id) {
            auto part_count_per_shard = parts_per_shard[shard_id].size();
            if (part_count_per_shard > 1) {
                return seastar::smp::submit_to(shard_id, [this, shard_id, part_count_per_shard] mutable {
                    const auto r = std::views::iota(0, static_cast<int>(part_count_per_shard / 2));
                    return seastar::do_for_each(r, [this, shard_id] (int part_idx) mutable {
                        return merge_two_parts(shard_id, part_idx * 2, part_idx * 2 + 1);
                    });
                });
            }
            return seastar::make_ready_future<>();
        });
        merge_pass_finalize();
    }
    // Remap final sorted file, the algorithm guarantees that parts_per_shard[0][0].path stores the path
    co_await seastar::rename_file(parts_per_shard[0][0].path, source_file_path + ".sorted");
}

seastar::future<> coordinator::external_sort() {
    co_await sort_all();
    co_await merge_all();
}

}

int main(int argc, char** argv) {
    seastar::app_template app;
    static const auto arg_path = "path";
    
    // The sorted output is written into the path <path>.sorted
    app.add_positional_options({{
        arg_path, boost::program_options::value<seastar::sstring>()->default_value({}), "path of the file to sort", -1
    }});

    // app.run() is noexcept
    return app.run(argc, argv, [&app] -> seastar::future<> {
        try {
            auto args = app.configuration();
            auto path = args[arg_path].as<seastar::sstring>();
            esort::coordinator c(path);
            co_await c.external_sort();
        } catch (...) {
            std::cerr << "Failed to run: " << std::current_exception() << "\n";
        }
    });
}
