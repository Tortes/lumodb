#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "tdldb/Database.h"

namespace {

struct BenchmarkOptions {
  std::filesystem::path directory = std::filesystem::temp_directory_path() / "tdldb-bench";
  std::vector<double> sizesGb = {1.0, 5.0, 10.0};
  uint64_t payloadSize = 4096;
  uint64_t readCount = 100;
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
    } else if (arg == "--payload-size") {
      options.payloadSize = std::stoull(std::string(nextValue()));
    } else if (arg == "--read-count") {
      options.readCount = std::stoull(std::string(nextValue()));
    } else if (arg == "--keep") {
      options.keepData = true;
    } else {
      std::cerr << "unknown argument: " << arg << '\n';
      std::exit(EXIT_FAILURE);
    }
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
  constexpr std::string_view marker = "TDLB";
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

void RunOne(const BenchmarkOptions& options, double sizeGb) {
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

  tdldb::DatabaseOptions databaseOptions;
  databaseOptions.initialBucketCount =
      RoundUpPowerOfTwo(static_cast<uint64_t>(objectCount / 0.70) + 1);

  tdldb::Database database;
  tdldb::Status status = database.Open(runDirectory, databaseOptions);
  if (!status) {
    std::cerr << "open failed: " << status.Message() << '\n';
    std::exit(EXIT_FAILURE);
  }

  std::vector<std::byte> payload = MakeFlatBufferPayload(options.payloadSize, 1);
  auto writeStart = std::chrono::steady_clock::now();
  for (uint64_t index = 0; index < objectCount; ++index) {
    payload = MakeFlatBufferPayload(options.payloadSize, index);
    status = database.PutStruct("StructA", KeyForIndex(index), payload);
    if (!status) {
      std::cerr << "write failed: " << status.Message() << '\n';
      std::exit(EXIT_FAILURE);
    }
  }
  status = database.Flush();
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

  std::sort(readDurations.begin(), readDurations.end());
  auto percentile = [&](double p) {
    const size_t offset = static_cast<size_t>(
        std::min<double>(readDurations.size() - 1, std::floor(readDurations.size() * p)));
    return readDurations[offset];
  };

  std::cout << "size_gb=" << sizeGb << " objects=" << objectCount
            << " payload=" << options.payloadSize << " bytes"
            << " write=" << FormatSeconds(writeEnd - writeStart)
            << " throughput=" << FormatMbPerSecond(writtenBytes, writeEnd - writeStart)
            << " read_count=" << options.readCount
            << " p50=" << FormatSeconds(percentile(0.50))
            << " p95=" << FormatSeconds(percentile(0.95))
            << " p99=" << FormatSeconds(percentile(0.99)) << '\n';

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
    RunOne(options, sizeGb);
  }
  return EXIT_SUCCESS;
}
