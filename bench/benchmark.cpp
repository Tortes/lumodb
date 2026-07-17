#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "lumodb/database.h"

namespace {

struct Options {
  std::filesystem::path directory =
      std::filesystem::temp_directory_path() / "lumodb-auto-row-bench";
  uint64_t entries = 10'000'000;
  uint32_t valueBytes = 16;
  uint32_t threads = std::max<uint32_t>(1, std::thread::hardware_concurrency());
  uint32_t rowShards = 0;
  uint64_t explicitRows = 0;
  uint64_t memoryBytes = 64ULL * 1024 * 1024 * 1024;
  uint64_t reads = 10'000;
  uint32_t batchSize = 1;
  uint32_t keyPrefixBytes = 0;
  bool keep = false;
};

Options ParseOptions(int argc, char** argv) {
  Options options;
  for (int index = 1; index < argc; ++index) {
    const std::string_view argument = argv[index];
    auto next = [&]() -> std::string_view {
      if (++index >= argc) {
        std::cerr << "missing value for " << argument << '\n';
        std::exit(EXIT_FAILURE);
      }
      return argv[index];
    };
    if (argument == "--dir") {
      options.directory = std::string(next());
    } else if (argument == "--entries") {
      options.entries = std::stoull(std::string(next()));
    } else if (argument == "--value-bytes") {
      options.valueBytes = static_cast<uint32_t>(std::stoul(std::string(next())));
    } else if (argument == "--threads") {
      options.threads = static_cast<uint32_t>(std::stoul(std::string(next())));
    } else if (argument == "--row-shards") {
      options.rowShards = static_cast<uint32_t>(std::stoul(std::string(next())));
    } else if (argument == "--explicit-rows") {
      options.explicitRows = std::stoull(std::string(next()));
    } else if (argument == "--memory-gb") {
      options.memoryBytes =
          static_cast<uint64_t>(std::stod(std::string(next())) * 1024 * 1024 * 1024);
    } else if (argument == "--reads") {
      options.reads = std::stoull(std::string(next()));
    } else if (argument == "--batch-size") {
      options.batchSize = static_cast<uint32_t>(std::stoul(std::string(next())));
    } else if (argument == "--key-prefix-bytes") {
      options.keyPrefixBytes = static_cast<uint32_t>(std::stoul(std::string(next())));
    } else if (argument == "--keep") {
      options.keep = true;
    } else if (argument == "--help") {
      std::cout << "lumodb_bench [--dir PATH] [--entries N] [--value-bytes N] "
                   "[--threads N] [--row-shards N] [--explicit-rows N] "
                   "[--memory-gb N] [--reads N] [--batch-size N] "
                   "[--key-prefix-bytes N] "
                   "[--keep]\n";
      std::exit(EXIT_SUCCESS);
    } else {
      std::cerr << "unknown argument: " << argument << '\n';
      std::exit(EXIT_FAILURE);
    }
  }
  if (options.entries == 0 || options.threads == 0 || options.memoryBytes == 0 ||
      options.batchSize == 0) {
    std::cerr << "entries, threads, memory-gb, and batch-size must be greater than zero\n";
    std::exit(EXIT_FAILURE);
  }
  if (options.explicitRows >= (1ULL << 63)) {
    std::cerr << "explicit-rows must be smaller than 2^63\n";
    std::exit(EXIT_FAILURE);
  }
  return options;
}

std::string Key(uint64_t index, uint32_t prefixBytes) {
  return std::string(prefixBytes, 'p') + "key-" + std::to_string(index);
}

double Seconds(std::chrono::steady_clock::duration duration) {
  return std::chrono::duration<double>(duration).count();
}

uint64_t DirectoryBytes(const std::filesystem::path& directory) {
  uint64_t bytes = 0;
  std::error_code error;
  for (const auto& entry : std::filesystem::recursive_directory_iterator(directory, error)) {
    if (entry.is_regular_file(error)) {
      bytes += entry.file_size(error);
    }
  }
  return bytes;
}

void Check(const LumoDB::Status& status, std::string_view operation) {
  if (!status) {
    std::cerr << operation << " failed: " << status.Message() << '\n';
    std::exit(EXIT_FAILURE);
  }
}

}  // namespace

int main(int argc, char** argv) {
  const Options arguments = ParseOptions(argc, argv);
  std::filesystem::remove_all(arguments.directory);

  LumoDB::DatabaseOptions options;
  options.expectedEntryCountPerColumn = arguments.entries;
  options.averageKeyBytes = 16 + arguments.keyPrefixBytes;
  options.averageValueBytes = arguments.valueBytes;
  options.expectedExplicitRowCount = arguments.explicitRows;
  options.expectedExplicitEntryCount = arguments.explicitRows == 0 ? 0 : arguments.entries;
  options.writerThreadCount = arguments.threads;
  options.rowShardCount = arguments.rowShards;
  options.memoryBudgetBytes = arguments.memoryBytes;

  LumoDB::Database database;
  Check(database.Open(arguments.directory, options), "Open");
  const LumoDB::DatabaseLayout layout = database.Layout();
  std::cout << "entries=" << arguments.entries << " value_bytes=" << arguments.valueBytes
            << " threads=" << arguments.threads
            << " target_entries_per_row=" << layout.targetEntriesPerRow
            << " routes=" << layout.routeCountPerColumn
            << " explicit_rows=" << arguments.explicitRows << " row_shards=" << layout.rowShardCount
            << " spill_partitions=" << layout.spillPartitionCount
            << " batch_size=" << arguments.batchSize
            << " key_prefix_bytes=" << arguments.keyPrefixBytes << '\n';

  std::vector<std::byte> payload(arguments.valueBytes, std::byte{0x5a});
  const auto putStart = std::chrono::steady_clock::now();
  std::vector<std::thread> writers;
  writers.reserve(arguments.threads);
  for (uint32_t threadId = 0; threadId < arguments.threads; ++threadId) {
    writers.emplace_back([&, threadId] {
      if (arguments.batchSize > 1) {
        std::vector<std::string> keys;
        std::vector<LumoDB::StructEntry> entries;
        std::vector<LumoDB::RowStructEntry> rowEntries;
        keys.reserve(arguments.batchSize);
        entries.reserve(arguments.batchSize);
        rowEntries.reserve(arguments.batchSize);
        auto flushAutomaticBatch = [&] {
          entries.clear();
          for (const std::string& key : keys) {
            entries.push_back({.key = key, .flatBufferBytes = payload});
          }
          Check(database.PutStructs("objects", entries), "PutStructs");
          keys.clear();
        };
        auto flushExplicitBatch = [&](uint64_t rowId) {
          rowEntries.clear();
          for (const std::string& key : keys) {
            rowEntries.push_back({.key = key, .flatBufferBytes = payload});
          }
          Check(database.PutRowStructs("objects", rowId, rowEntries), "PutRowStructs");
          keys.clear();
        };
        if (arguments.explicitRows == 0) {
          for (uint64_t index = threadId; index < arguments.entries; index += arguments.threads) {
            keys.push_back(Key(index, arguments.keyPrefixBytes));
            if (keys.size() == arguments.batchSize) {
              flushAutomaticBatch();
            }
          }
          if (!keys.empty()) {
            flushAutomaticBatch();
          }
        } else {
          const uint64_t usedRows = std::min(arguments.explicitRows, arguments.entries);
          for (uint64_t rowId = threadId; rowId < usedRows; rowId += arguments.threads) {
            for (uint64_t index = rowId; index < arguments.entries;
                 index += arguments.explicitRows) {
              keys.push_back(Key(index, arguments.keyPrefixBytes));
              if (keys.size() == arguments.batchSize) {
                flushExplicitBatch(rowId);
              }
            }
            if (!keys.empty()) {
              flushExplicitBatch(rowId);
            }
          }
        }
        return;
      }
      for (uint64_t index = threadId; index < arguments.entries; index += arguments.threads) {
        const LumoDB::Status status =
            arguments.explicitRows == 0
                ? database.Put("objects", Key(index, arguments.keyPrefixBytes), payload)
                : database.Put("objects", index % arguments.explicitRows,
                               Key(index, arguments.keyPrefixBytes), payload);
        Check(status, "Put");
      }
    });
  }
  for (std::thread& writer : writers) {
    writer.join();
  }
  const auto putEnd = std::chrono::steady_clock::now();
  Check(database.Flush(), "Flush");
  const auto flushEnd = std::chrono::steady_clock::now();
  Check(database.Close(), "Close");

  const double putSeconds = Seconds(putEnd - putStart);
  const double flushSeconds = Seconds(flushEnd - putEnd);
  std::cout << std::fixed << std::setprecision(3) << "stage=" << putSeconds << "s ("
            << arguments.entries / putSeconds / 1'000'000.0 << " Mkeys/s) "
            << "build_flush=" << flushSeconds << "s ("
            << arguments.entries / flushSeconds / 1'000'000.0 << " Mkeys/s) "
            << "disk=" << DirectoryBytes(arguments.directory) / (1024.0 * 1024 * 1024) << " GiB\n";

  Check(database.OpenReadOnly(arguments.directory), "OpenReadOnly");
  std::mt19937_64 random(0x12345678);
  std::vector<double> latencies;
  latencies.reserve(arguments.reads);
  for (uint64_t sample = 0; sample < arguments.reads; ++sample) {
    const uint64_t index = random() % arguments.entries;
    std::vector<std::byte> value;
    const auto start = std::chrono::steady_clock::now();
    const LumoDB::Status status =
        arguments.explicitRows == 0
            ? database.Get("objects", Key(index, arguments.keyPrefixBytes), value)
            : database.GetRowStruct("objects", index % arguments.explicitRows,
                                    Key(index, arguments.keyPrefixBytes), value);
    Check(status, "Get");
    const auto end = std::chrono::steady_clock::now();
    if (value != payload) {
      std::cerr << "read returned the wrong value\n";
      return EXIT_FAILURE;
    }
    latencies.push_back(std::chrono::duration<double, std::micro>(end - start).count());
  }
  std::sort(latencies.begin(), latencies.end());
  if (!latencies.empty()) {
    const size_t p50 = latencies.size() / 2;
    const size_t p99 = std::min(latencies.size() - 1, latencies.size() * 99 / 100);
    std::cout << "random_read p50=" << latencies[p50] << "us p99=" << latencies[p99] << "us\n";
  }
  Check(database.Close(), "CloseReadOnly");
  if (!arguments.keep) {
    std::filesystem::remove_all(arguments.directory);
  }
  return EXIT_SUCCESS;
}
