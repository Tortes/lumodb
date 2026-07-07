#include "test_utils.h"

#include <string>

namespace {

TEST(KvStorageTest, PutGetOverwriteAndReopen) {
  const std::filesystem::path directory = LumoDB::test::MakeTestDirectory("kv");

  LumoDB::DatabaseOptions options;
  options.initialBucketCount = 4;
  options.maxLoadFactor = 0.5;

  LumoDB::Database database;
  ASSERT_OK(database.Open(directory, options));
  ASSERT_OK(database.Put("alpha", "one"));
  ASSERT_OK(database.Put("beta", "two"));
  ASSERT_OK(database.Put("alpha", "updated"));

  std::string value;
  ASSERT_OK(database.Get("alpha", value));
  EXPECT_EQ(value, "updated");
  ASSERT_OK(database.Get("beta", value));
  EXPECT_EQ(value, "two");
  EXPECT_EQ(database.Get("missing", value).Code(), LumoDB::StatusCode::kNotFound);
  EXPECT_EQ(database.EntryCount(), 2);
  ASSERT_OK(database.Close());

  ASSERT_OK(database.Open(directory, options));
  ASSERT_OK(database.Get("alpha", value));
  EXPECT_EQ(value, "updated");
  ASSERT_OK(database.Close());
}

TEST(KvStorageTest, ReportsNotOpen) {
  LumoDB::Database database;
  std::string value;
  EXPECT_EQ(database.Get("key", value).Code(), LumoDB::StatusCode::kNotOpen);
  EXPECT_EQ(database.Put("key", "value").Code(), LumoDB::StatusCode::kNotOpen);
}

}  // namespace
