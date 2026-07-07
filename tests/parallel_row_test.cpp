#include "test_utils.h"

#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace {

TEST(ParallelRowStorageTest, WritesSameColumnDifferentRowsConcurrently) {
  const std::filesystem::path directory = LumoDB::test::MakeTestDirectory("row-parallel");

  LumoDB::DatabaseOptions options;
  options.initialRowBucketCount = 128;
  options.rowShardCount = 4;

  LumoDB::Database database;
  ASSERT_OK(database.Open(directory, options));

  std::mutex failureMutex;
  bool failed = false;
  auto worker = [&](uint64_t beginRow, uint64_t endRow) {
    for (uint64_t rowId = beginRow; rowId < endRow; ++rowId) {
      std::vector<std::byte> first =
          LumoDB::test::MakeBytes("first-" + std::to_string(rowId));
      std::vector<std::byte> second =
          LumoDB::test::MakeBytes("second-" + std::to_string(rowId));
      std::vector<LumoDB::RowStructEntry> entries = {
          {.key = "first", .flatBufferBytes = first},
          {.key = "second", .flatBufferBytes = second},
      };
      LumoDB::Status status = database.PutRowStructs("StructA", rowId, entries);
      if (!status) {
        std::lock_guard<std::mutex> lock(failureMutex);
        failed = true;
        return;
      }
    }
  };

  std::thread t1(worker, 0, 16);
  std::thread t2(worker, 16, 32);
  std::thread t3(worker, 32, 48);
  std::thread t4(worker, 48, 64);
  t1.join();
  t2.join();
  t3.join();
  t4.join();

  EXPECT_FALSE(failed);
  EXPECT_EQ(database.RowCount(), 64);

  for (uint64_t rowId = 0; rowId < 64; ++rowId) {
    std::vector<std::byte> value;
    ASSERT_OK(database.GetRowStruct("StructA", rowId, "first", value));
    EXPECT_EQ(LumoDB::test::BytesToString(value), "first-" + std::to_string(rowId));
    ASSERT_OK(database.GetRowStruct("StructA", rowId, "second", value));
    EXPECT_EQ(LumoDB::test::BytesToString(value), "second-" + std::to_string(rowId));
  }
  ASSERT_OK(database.Close());
}

}  // namespace
