#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include "tdldb/Database.h"

namespace {

int gFailures = 0;

#define CHECK(condition)                                                        \
  do {                                                                          \
    if (!(condition)) {                                                         \
      std::cerr << "CHECK failed at " << __FILE__ << ":" << __LINE__ << ": " \
                << #condition << '\n';                                          \
      ++gFailures;                                                              \
      return;                                                                   \
    }                                                                           \
  } while (false)

#define CHECK_STATUS(status) CHECK((status).IsOk())

std::filesystem::path MakeTestDirectory(std::string_view name) {
  std::filesystem::path path =
      std::filesystem::temp_directory_path() / ("tdldb-test-" + std::string(name));
  std::filesystem::remove_all(path);
  std::filesystem::create_directories(path);
  return path;
}

std::vector<std::byte> MakeBytes(std::string_view value) {
  std::vector<std::byte> bytes(value.size());
  for (size_t index = 0; index < value.size(); ++index) {
    bytes[index] = static_cast<std::byte>(value[index]);
  }
  return bytes;
}

std::string BytesToString(const std::vector<std::byte>& bytes) {
  std::string value(bytes.size(), '\0');
  for (size_t index = 0; index < bytes.size(); ++index) {
    value[index] = static_cast<char>(bytes[index]);
  }
  return value;
}

void TestStringPutGet() {
  const std::filesystem::path directory = MakeTestDirectory("string");

  tdldb::DatabaseOptions options;
  options.initialBucketCount = 4;
  options.maxLoadFactor = 0.5;

  tdldb::Database database;
  CHECK_STATUS(database.Open(directory, options));
  CHECK_STATUS(database.Put("alpha", "one"));
  CHECK_STATUS(database.Put("beta", "two"));
  CHECK_STATUS(database.Put("alpha", "updated"));

  std::string value;
  CHECK_STATUS(database.Get("alpha", value));
  CHECK(value == "updated");
  CHECK_STATUS(database.Get("beta", value));
  CHECK(value == "two");
  CHECK(database.Get("missing", value).Code() == tdldb::StatusCode::kNotFound);
  CHECK(database.EntryCount() == 2);
  CHECK_STATUS(database.Close());

  CHECK_STATUS(database.Open(directory, options));
  CHECK_STATUS(database.Get("alpha", value));
  CHECK(value == "updated");
  CHECK_STATUS(database.Close());
}

void TestStructPutGet() {
  const std::filesystem::path directory = MakeTestDirectory("struct");

  tdldb::DatabaseOptions options;
  options.initialBucketCount = 4;
  options.maxLoadFactor = 0.5;

  tdldb::Database database;
  CHECK_STATUS(database.Open(directory, options));

  CHECK_STATUS(database.PutStruct("StructA", "a-1", MakeBytes("flatbuffer-a1")));
  CHECK_STATUS(database.PutStruct("StructA", "a-2", MakeBytes("flatbuffer-a2")));
  CHECK_STATUS(database.PutStruct("StructB", "a-1", MakeBytes("flatbuffer-b1")));
  CHECK_STATUS(database.PutStruct("StructA", "a-1", MakeBytes("flatbuffer-a1-new")));

  std::vector<std::byte> value;
  CHECK_STATUS(database.GetStruct("StructA", "a-1", value));
  CHECK(BytesToString(value) == "flatbuffer-a1-new");
  CHECK_STATUS(database.GetStruct("StructB", "a-1", value));
  CHECK(BytesToString(value) == "flatbuffer-b1");

  std::vector<std::vector<std::byte>> values;
  CHECK_STATUS(database.GetMany("StructA", {"a-1", "a-2"}, values));
  CHECK(values.size() == 2);
  CHECK(BytesToString(values[0]) == "flatbuffer-a1-new");
  CHECK(BytesToString(values[1]) == "flatbuffer-a2");
  CHECK(database.EntryCount() == 3);
  CHECK(database.BucketCount() >= 8);
  CHECK_STATUS(database.Close());
}

void TestIndexRebuild() {
  const std::filesystem::path directory = MakeTestDirectory("rebuild");

  tdldb::Database database;
  CHECK_STATUS(database.Open(directory));
  CHECK_STATUS(database.PutStruct("StructA", "key", MakeBytes("payload")));
  CHECK_STATUS(database.Close());

  std::filesystem::remove(directory / "index.tdli");

  CHECK_STATUS(database.Open(directory));
  std::vector<std::byte> value;
  CHECK_STATUS(database.GetStruct("StructA", "key", value));
  CHECK(BytesToString(value) == "payload");
  CHECK(database.EntryCount() == 1);
  CHECK_STATUS(database.Close());
}

}  // namespace

int main() {
  TestStringPutGet();
  TestStructPutGet();
  TestIndexRebuild();

  if (gFailures != 0) {
    std::cerr << gFailures << " test failure(s)\n";
    return EXIT_FAILURE;
  }

  std::cout << "tdldb tests passed\n";
  return EXIT_SUCCESS;
}
