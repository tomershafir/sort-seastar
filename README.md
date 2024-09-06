# sort-seastar

External sort utility, written in C++ using Seastar framework.

## Build

```Bash
cd sort-seastar &&
export seastar_dir=<seastar-build-parent-dir> CMAKE_PREFIX_PATH="$seastar_dir/build/release;$seastar_dir/build/release/_cooking/installed" CMAKE_MODULE_PATH=$seastar_dir/cmake && 
rm -rf build && 
mkdir -p build && 
cmake -S . -G Ninja -DCMAKE_CXX_COMPILER=clang++-18 -DENABLE_UBSAN=1 -DCMAKE_C_COMPILER=clang-18 -B build && cmake --build build
```

## Run

```Bash
./build/ssort <path>
```

## Test

```Bash
./test/regression.sh
```

### Benchmarking

```Bash
./test/perf/latency.sh
```

## Requirements

- The system should have at least 2X of the original file size free storage space available.
- The original file shouldn't be modified concurrently, otherwise it's UB.

## Compression

Compression is expected to be handled externally, following the Unix philosophy of single reponsibility.

## Encryption

Encryption is expected to be handled externally, following the Unix philosophy of single reponsibility.

## Improvements

- Investigate logs:

1. Reactor stalls.
2. Rate-limit: suppressed 12 backtraces on shard

- Probe HW and decide a specific execution plan accordingly. Do sequential reads and writes and parse the records in a memory buffer on HDDs (I/O is usually bottleneck on modern archs). Files may not be sequential, depending on the system state and the storage alogrithms. But for example a ScyllaDB server SSTable file should be mostly sequential. On the other hand, SSDs supports parallel random access. Partition the file into smaller parts based on available memory per shard and issue I/O ops in parallel using seastar::parallel_for_each instead of seastar::do_for_each. Consider using parallel buffered writes on the merge output buffer with `write_behind` option. Measure the later on HDDs, too.

- Distribute the shard 0 centralized coordinator and avoid inefficient remote memory access on a NUMA node. Part of this is to distribute interrupt flag - interrupt with message passing rather than atomic variable.
- Try to embed `merge_pass_finalize` into `merge_two_parts` to become `merge_two_front_parts` with a valid new part inter-shard allocation. Part of this is to memoize new file path on merge and remove redundant computation on interrupt handler.
- Handle exceptional exit and signals by deleting temporary files. There is a problem - shard_id gets corrupted.
- Probe the storage for block size (it may be 8KiB) and align record access to it.
- Assign parts to a subset of the shards if possible when part_count_per_shard_uniform == 0 using partial round-robin.
- Support other in memory sorting algorithms.
- On merge, do buffered sequential reads and writes per shard, based on allocated memory per shard, depending on the bulk I/O strategy as described above.
- Merge more parts at a time, as much as they fit in memory, to reduce merge pass count, depending on the bulk I/O strategy as described above.
- Optimize merge by applying an elevator algorithm that caches the last blocks on each pass.
- Cache open fds to reduce open() syscall count. Also rollover prev-prev pass files, using the fd cache, instead of additional opens and unmaps. Switch to use inter-shard file handles.
- Add a uuid to temporary file names to avoid collision.
- Consider decetralized coordination to improve parallelism. For example, we can leverge the tree model of merge-sort so that local serialization is needed only where nodes join bottom-up.
- Consider to use a radix sort for fixed length records.

- Try to sort a buffer in place using std::sort and a custom fixed_length_string_iterator.

- Add PGO options.
- Include CMake targets for LLVM tools, document `-DCMAKE_EXPORT_COMPILE_COMMANDS=ON`.
- Add XRay option.
- Add sanitizer options.

- Add larger files to the testdata corpus.
- Add integration and unit tests using `gtest`.
- Add fuzz tests.
- Benchmark with `google/becnhmark`.
- Add appropriate CMake test targets.
- Try add `perf` profiling targets.

- Evaluate other strategies to allocate available memory to read and write buffers on merge, depending on storage device type, e.g. add a knob to prefer read or write sequentiality, and measure.
- Support memory relative memory_reserve_userspace_total_bytes, benchmark and fine tune the default setting, measure total memory usage minus parts size, document the statistical study.
- Add a setting min_shard_size_bytes or similar to shut sharding off for small files, need to benchmark different file sizes between sharded vs serial sort until I find the point where the ration inverts, document the statistical study.
- Make sure buffer size is small for small files to avoid reservation of unsued space and increase preallocation_size for large files: [https://github.com/tomershafir/seastar/blob/908ccd936a63a37cd98470ad8bf44a20d969c51e/include/seastar/core/fstream.hh#L94](https://github.com/tomershafir/seastar/blob/908ccd936a63a37cd98470ad8bf44a20d969c51e/include/seastar/core/fstream.hh#L94).
- Generalize merge code to depend on merge_k_way rather than 2.

- Try remove doubling of space at merge time, and then remove this from Requirements.
