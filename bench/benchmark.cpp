#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "lumodb/database.h"

namespace {

enum class BenchmarkMode {
  kObject,
  kRowParallel,
};

struct BenchmarkOptions {
  std::filesystem::path directory = std::filesystem::temp_directory_path() / "lumodb-bench";
  std::vector<double> sizesGb = {1.0, 5.0, 10.0};
  BenchmarkMode mode = BenchmarkMode::kObject;
  uint64_t payloadSize = 4096;
  uint64_t objectBatchSize = 1;
  uint64_t rowEntries = 64;
  uint64_t readCount = 100;
  uint32_t threads = std::max<uint32_t>(1, std::thread::hardware_concurrency());
  uint32_t shards = 8;
  bool keepData = false;
};

uint64_t RoundUpPowerOfTwo(uint64_t value) {
  uint64_t result = 16;
  while (result < value) {
    result <<= 1;
  }
  return result;
}

std::vector<double> ParseSizes(std::string_view text) {
  std::vector<double> sizes;
  std::stringstream stream{std::string(text)};
  std::string item;
  while (std::getline(stream, item, ',')) {
    sizes.push_back(std::stod(item));
  }
  return sizes;
}

BenchmarkMode ParseMode(std::string_view text) {
  if (text == "object") {
    return BenchmarkMode::kObject;
  }
  if (text == "row-parallel") {
    return BenchmarkMode::kRowParallel;
  }
  std::cerr << "unknown mode: " << text << '\n';
  std::exit(EXIT_FAILURE);
}

BenchmarkOptions ParseArgs(int argc, char** argv) {
  BenchmarkOptions options;
  for (int index = 1; index < argc; ++index) {
    std::string_view arg = argv[index];
    auto nextValue = [&]() -> std::string_view {
      if (index + 1 >= argc) {
        std::cerr << "missing value for " << arg << '\n';
        std::exit(EXIT_FAILURE);
      }
      return argv[++index];
    };

    if (arg == "--dir") {
      options.directory = std::string(nextValue());
    } else if (arg == "--sizes") {
      options.sizesGb = ParseSizes(nextValue());
    } else if (arg == "--mode") {
      options.mode = ParseMode(nextValue());
    } else if (arg == "--payload-size") {
      options.payloadSize = std::stoull(std::string(nextValue()));
    } else if (arg == "--object-batch-size") {
      options.objectBatchSize = std::stoull(std::string(nextValue()));
    } else if (arg == "--row-entries") {
      options.rowEntries = std::stoull(std::string(nextValue()));
    } else if (arg == "--read-count") {
      options.readCount = std::stoull(std::string(nextValue()));
    } else if (arg == "--threads") {
      options.threads = static_cast<uint32_t>(std::stoul(std::string(nextValue())));
    } else if (arg == "--shards") {
      options.shards = static_cast<uint32_t>(std::stoul(std::string(nextValue())));
    } else if (arg == "--keep") {
      options.keepData = true;
    } else {
      std::cerr << "unknown argument: " << arg << '\n';
      std::exit(EXIT_FAILURE);
    }
  }
  if (options.objectBatchSize == 0 || options.rowEntries == 0 || options.threads == 0 ||
      options.shards == 0) {
    std::cerr << "object-batch-size, row-entries, threads, and shards must be greater than zero\n";
    std::exit(EXIT_FAILURE);
  }
  return options;
}

std::vector<std::byte> MakeFlatBufferPayload(uint64_t size, uint64_t seed) {
  std::vector<std::byte> payload(static_cast<size_t>(size));
  if (payload.empty()) {
    return payload;
  }

  std::mt19937_64 random(seed);
  for (std::byte& byte : payload) {
    byte = static_cast<std::byte>(random() & 0xFF);
  }

  // The DB treats FlatBuffers as opaque bytes; this marker only makes benchmark
  // files easy to identify in a hex dump.
  constexpr std::string_view marker = "LUMO";
  for (size_t index = 0; index < std::min(marker.size(), payload.size()); ++index) {
    payload[index] = static_cast<std::byte>(marker[index]);
  }
  return payload;
}

std::string FormatSeconds(std::chrono::steady_clock::duration duration) {
  const double seconds = std::chrono::duration<double>(duration).count();
  std::ostringstream output;
  output << std::fixed << std::setprecision(3) << seconds << "s";
  return output.str();
}

std::string FormatMilliseconds(std::chrono::steady_clock::duration duration) {
  const double milliseconds =
      std::chrono::duration<double, std::milli>(duration).count();
  std::ostringstream output;
  output << std::fixed << std::setprecision(3) << milliseconds << "ms";
  return output.str();
}

std::string FormatMbPerSecond(uint64_t bytes, std::chrono::steady_clock::duration duration) {
  const double seconds = std::chrono::duration<double>(duration).count();
  const double mb = static_cast<double>(bytes) / (1024.0 * 1024.0);
  std::ostringstream output;
  output << std::fixed << std::setprecision(2) << (mb / seconds) << " MB/s";
  return output.str();
}

std::string KeyForIndex(uint64_t index) {
  return "key-" + std::to_string(index);
}

void PrintReadPercentiles(std::vector<std::chrono::steady_clock::duration>& readDurations) {
  std::sort(readDurations.begin(), readDurations.end());
  auto percentile = [&](double p) {
    const size_t offset = static_cast<size_t>(
        std::min<double>(readDurations.size() - 1, std::floor(readDurations.size() * p)));
    return readDurations[offset];
  };

  std::cout << " p50=" << FormatMilliseconds(percentile(0.50))
            << " p95=" << FormatMilliseconds(percentile(0.95))
            << " p99=" << FormatMilliseconds(percentile(0.99)) << '\n';
}

void RunObjectMode(const BenchmarkOptions& options, double sizeGb) {
  const uint64_t targetBytes =
      static_cast<uint64_t>(std::ceil(sizeGb * 1024.0 * 1024.0 * 1024.0));
  const uint64_t objectCount =
      std::max<uint64_t>(1, (targetBytes + options.payloadSize - 1) / options.payloadSize);
  const uint64_t writtenBytes = objectCount * options.payloadSize;

  std::filesystem::path runDirectory =
      options.directory / ("size-" + std::to_string(static_cast<uint64_t>(sizeGb * 1000)));
  if (!options.keepData) {
    std::filesystem::remove_all(runDirectory);
  }
  std::filesystem::create_directories(runDirectory);

  LumoDB::DatabaseOptions databaseOptions;
  databaseOptions.initialBucketCount =
      RoundUpPowerOfTwo(static_cast<uint64_t>(objectCount / 0.70) + 1);

  LumoDB::Database database;
  LumoDB::Status status = database.Open(runDirectory, databaseOptions);
  if (!status) {
    std::cerr << "open failed: " << status.Message() << '\n';
    std::exit(EXIT_FAILURE);
  }

  std::chrono::steady_clock::duration prepareDuration{};
  std::chrono::steady_clock::duration databaseWriteDuration{};
  auto writeStart = std::chrono::steady_clock::now();
  for (uint64_t firstObject = 0; firstObject < objectCount;
       firstObject += options.objectBatchSize) {
    const uint64_t batchCount =
        std::min<uint64_t>(options.objectBatchSize, objectCount - firstObject);
    const auto prepareStart = std::chrono::steady_clock::now();
    std::vector<std::string> keys;
    std::vector<std::vector<std::byte>> payloads;
    std::vector<LumoDB::StructEntry> entries;
    keys.reserve(static_cast<size_t>(batchCount));
    payloads.reserve(static_cast<size_t>(batchCount));
    entries.reserve(static_cast<size_t>(batchCount));
    for (uint64_t offset = 0; offset < batchCount; ++offset) {
      const uint64_t objectIndex = firstObject + offset;
      keys.push_back(KeyForIndex(objectIndex));
      payloads.push_back(MakeFlatBufferPayload(options.payloadSize, objectIndex));
      entries.push_back({.key = keys.back(), .flatBufferBytes = payloads.back()});
    }
    const auto prepareEnd = std::chrono::steady_clock::now();
    prepareDuration += prepareEnd - prepareStart;

    const auto databaseWriteStart = std::chrono::steady_clock::now();
    status = database.PutStructs("StructA", entries);
    const auto databaseWriteEnd = std::chrono::steady_clock::now();
    databaseWriteDuration += databaseWriteEnd - databaseWriteStart;
    if (!status) {
      std::cerr << "write failed: " << status.Message() << '\n';
      std::exit(EXIT_FAILURE);
    }
  }
  const auto flushStart = std::chrono::steady_clock::now();
  status = database.Flush();
  const auto flushEnd = std::chrono::steady_clock::now();
  databaseWriteDuration += flushEnd - flushStart;
  if (!status) {
    std::cerr << "flush failed: " << status.Message() << '\n';
    std::exit(EXIT_FAILURE);
  }
  auto writeEnd = std::chrono::steady_clock::now();

  std::mt19937_64 random(42);
  std::vector<std::chrono::steady_clock::duration> readDurations;
  readDurations.reserve(static_cast<size_t>(options.readCount));
  std::vector<std::byte> value;

  for (uint64_t index = 0; index < options.readCount; ++index) {
    const uint64_t objectIndex = random() % objectCount;
    auto readStart = std::chrono::steady_clock::now();
    status = database.GetStruct("StructA", KeyForIndex(objectIndex), value);
    auto readEnd = std::chrono::steady_clock::now();
    if (!status) {
      std::cerr << "read failed: " << status.Message() << '\n';
      std::exit(EXIT_FAILURE);
    }
    readDurations.push_back(readEnd - readStart);
  }

  std::cout << "mode=object size_gb=" << sizeGb << " objects=" << objectCount
            << " batch_size=" << options.objectBatchSize
            << " payload=" << options.payloadSize << " bytes"
            << " write=" << FormatSeconds(writeEnd - writeStart)
            << " throughput=" << FormatMbPerSecond(writtenBytes, writeEnd - writeStart)
            << " prepare=" << FormatSeconds(prepareDuration)
            << " db_write=" << FormatSeconds(databaseWriteDuration)
            << " db_throughput=" << FormatMbPerSecond(writtenBytes, databaseWriteDuration)
            << " read_count=" << options.readCount;
  PrintReadPercentiles(readDurations);

  status = database.Close();
  if (!status) {
    std::cerr << "close failed: " << status.Message() << '\n';
    std::exit(EXIT_FAILURE);
  }
}

void RunRowParallelMode(const BenchmarkOptions& options, double sizeGb) {
  const uint64_t targetBytes =
      static_cast<uint64_t>(std::ceil(sizeGb * 1024.0 * 1024.0 * 1024.0));
  const uint64_t objectCount =
      std::max<uint64_t>(1, (targetBytes + options.payloadSize - 1) / options.payloadSize);
  const uint64_t rowCount =
      (objectCount + options.rowEntries - 1) / options.rowEntries;
  const uint64_t writtenBytes = objectCount * options.payloadSize;

  std::filesystem::path runDirectory =
      options.directory / ("row-size-" +
                           std::to_string(static_cast<uint64_t>(sizeGb * 1000)));
  if (!options.keepData) {
    std::filesystem::remove_all(runDirectory);
  }
  std::filesystem::create_directories(runDirectory);

  LumoDB::DatabaseOptions databaseOptions;
  databaseOptions.initialBucketCount = 16;
  databaseOptions.initialRowBucketCount =
      RoundUpPowerOfTwo(static_cast<uint64_t>(rowCount / 0.70) + 1);
  databaseOptions.rowShardCount = options.shards;

  LumoDB::Database database;
  LumoDB::Status status = database.Open(runDirectory, databaseOptions);
  if (!status) {
    std::cerr << "open failed: " << status.Message() << '\n';
    std::exit(EXIT_FAILURE);
  }

  std::atomic<uint64_t> nextRow{0};
  std::atomic<uint64_t> prepareNanos{0};
  std::atomic<uint64_t> databaseWriteNanos{0};
  std::mutex errorMutex;
  std::string errorMessage;

  auto writeStart = std::chrono::steady_clock::now();
  std::vector<std::thread> threads;
  threads.reserve(options.threads);
  for (uint32_t threadId = 0; threadId < options.threads; ++threadId) {
    threads.emplace_back([&]() {
      while (true) {
        const uint64_t rowId = nextRow.fetch_add(1, std::memory_order_relaxed);
        if (rowId >= rowCount) {
          return;
        }

        const uint64_t firstObject = rowId * options.rowEntries;
        const uint64_t rowObjectCount =
            std::min<uint64_t>(options.rowEntries, objectCount - firstObject);

        std::vector<std::string> keys;
        std::vector<std::vector<std::byte>> payloads;
        std::vector<LumoDB::RowStructEntry> entries;
        const auto prepareStart = std::chrono::steady_clock::now();
        keys.reserve(static_cast<size_t>(rowObjectCount));
        payloads.reserve(static_cast<size_t>(rowObjectCount));
        entries.reserve(static_cast<size_t>(rowObjectCount));

        for (uint64_t entryIndex = 0; entryIndex < rowObjectCount; ++entryIndex) {
          const uint64_t objectIndex = firstObject + entryIndex;
          keys.push_back(KeyForIndex(objectIndex));
          payloads.push_back(MakeFlatBufferPayload(options.payloadSize, objectIndex));
          entries.push_back({.key = keys.back(), .flatBufferBytes = payloads.back()});
        }
        const auto prepareEnd = std::chrono::steady_clock::now();
        prepareNanos.fetch_add(
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(prepareEnd - prepareStart)
                    .count()),
            std::memory_order_relaxed);

        const auto databaseWriteStart = std::chrono::steady_clock::now();
        LumoDB::Status rowStatus = database.PutRowStructs("StructA", rowId, entries);
        const auto databaseWriteEnd = std::chrono::steady_clock::now();
        databaseWriteNanos.fetch_add(
            static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                      databaseWriteEnd - databaseWriteStart)
                                      .count()),
            std::memory_order_relaxed);
        if (!rowStatus) {
          std::lock_guard<std::mutex> lock(errorMutex);
          if (errorMessage.empty()) {
            errorMessage = rowStatus.Message();
          }
          return;
        }
      }
    });
  }

  for (std::thread& thread : threads) {
    thread.join();
  }

  if (!errorMessage.empty()) {
    std::cerr << "write failed: " << errorMessage << '\n';
    std::exit(EXIT_FAILURE);
  }

  const auto flushStart = std::chrono::steady_clock::now();
  status = database.Flush();
  const auto flushEnd = std::chrono::steady_clock::now();
  if (!status) {
    std::cerr << "flush failed: " << status.Message() << '\n';
    std::exit(EXIT_FAILURE);
  }
  auto writeEnd = std::chrono::steady_clock::now();
  const auto prepareDuration = std::chrono::nanoseconds(prepareNanos.load(
      std::memory_order_relaxed));
  const auto databaseWriteDuration =
      std::chrono::nanoseconds(databaseWriteNanos.load(std::memory_order_relaxed)) +
      (flushEnd - flushStart);

  std::mt19937_64 random(42);
  std::vector<std::chrono::steady_clock::duration> readDurations;
  readDurations.reserve(static_cast<size_t>(options.readCount));
  std::vector<std::byte> value;

  for (uint64_t index = 0; index < options.readCount; ++index) {
    const uint64_t objectIndex = random() % objectCount;
    const uint64_t rowId = objectIndex / options.rowEntries;
    auto readStart = std::chrono::steady_clock::now();
    status = database.GetRowStruct("StructA", rowId, KeyForIndex(objectIndex), value);
    auto readEnd = std::chrono::steady_clock::now();
    if (!status) {
      std::cerr << "read failed: " << status.Message() << '\n';
      std::exit(EXIT_FAILURE);
    }
    readDurations.push_back(readEnd - readStart);
  }

  std::cout << "mode=row-parallel size_gb=" << sizeGb << " objects=" << objectCount
            << " rows=" << rowCount << " row_entries=" << options.rowEntries
            << " threads=" << options.threads << " shards=" << options.shards
            << " payload=" << options.payloadSize << " bytes"
            << " write=" << FormatSeconds(writeEnd - writeStart)
            << " throughput=" << FormatMbPerSecond(writtenBytes, writeEnd - writeStart)
            << " prepare_sum=" << FormatSeconds(prepareDuration)
            << " db_write_sum=" << FormatSeconds(databaseWriteDuration)
            << " read_count=" << options.readCount;
  PrintReadPercentiles(readDurations);

  status = database.Close();
  if (!status) {
    std::cerr << "close failed: " << status.Message() << '\n';
    std::exit(EXIT_FAILURE);
  }
}

}  // namespace

int main(int argc, char** argv) {
  BenchmarkOptions options = ParseArgs(argc, argv);
  for (double sizeGb : options.sizesGb) {
    if (options.mode == BenchmarkMode::kRowParallel) {
      RunRowParallelMode(options, sizeGb);
    } else {
      RunObjectMode(options, sizeGb);
    }
  }
  return EXIT_SUCCESS;
}
