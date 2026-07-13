#include "test_utils.h"

#include <sstream>
#include <string>
#include <vector>

namespace {

TEST(DiagnosticsTest, ReportsCurrentObjectsForEachColumn) {
  const std::filesystem::path directory = LumoDB::test::MakeTestDirectory("diagnostics");

  LumoDB::DatabaseOptions options;
  options.initialBucketCount = 16;
  options.initialRowBucketCount = 16;
  options.rowShardCount = 2;

  LumoDB::Database database;
  ASSERT_OK(database.Open(directory, options));
  ASSERT_OK(database.PutStruct("StructA", "object-a",
                               LumoDB::test::MakeBytes("first-value")));
  ASSERT_OK(database.PutStruct("StructA", "object-a",
                               LumoDB::test::MakeBytes("updated-value")));
  ASSERT_OK(database.PutStruct("StructB", "object-b",
                               LumoDB::test::MakeBytes("second-value")));

  const std::vector<std::byte> rowA = LumoDB::test::MakeBytes("row-a");
  const std::vector<std::byte> rowB = LumoDB::test::MakeBytes("row-b");
  const std::vector<LumoDB::RowStructEntry> entries = {
      {.key = "row-key-a", .flatBufferBytes = rowA},
      {.key = "row-key-b", .flatBufferBytes = rowB},
  };
  ASSERT_OK(database.PutRowStructs("StructA", 7, entries));
  ASSERT_OK(database.PutRowStruct("StructB", 9, "row-key-c",
                                  LumoDB::test::MakeBytes("row-c")));
  ASSERT_OK(database.Close());

  ASSERT_OK(database.Open(directory, options));
  std::vector<LumoDB::ColumnStats> stats;
  ASSERT_OK(database.GetColumnStats(stats));
  ASSERT_EQ(stats.size(), 2);
  EXPECT_EQ(stats[0].column, "StructA");
  EXPECT_EQ(stats[0].objectCount, 3);
  EXPECT_EQ(stats[0].rowCount, 1);
  EXPECT_EQ(stats[1].column, "StructB");
  EXPECT_EQ(stats[1].objectCount, 2);
  EXPECT_EQ(stats[1].rowCount, 1);

  std::ostringstream statistics;
  ASSERT_OK(database.DumpColumnStats(statistics));
  EXPECT_NE(statistics.str().find("LumoDB column statistics"), std::string::npos);
  EXPECT_NE(statistics.str().find("column=\"StructA\" objects=3 rows=1"),
            std::string::npos);
  EXPECT_NE(statistics.str().find("column=\"StructB\" objects=2 rows=1"),
            std::string::npos);

  std::ostringstream dump;
  ASSERT_OK(database.Dump(dump));
  const std::string text = dump.str();
  EXPECT_NE(text.find("LumoDB dump"), std::string::npos);
  EXPECT_NE(text.find("object_index: entries=2"), std::string::npos);
  EXPECT_NE(text.find("column=\"StructA\" key=\"object-a\""),
            std::string::npos);
  EXPECT_NE(text.find("value_hex=0x757064617465642d76616c7565"), std::string::npos);
  EXPECT_NE(text.find("row_id=7"), std::string::npos);
  EXPECT_NE(text.find("key=\"row-key-a\" value_size=5 value_hex=0x726f772d61"),
            std::string::npos);
  EXPECT_NE(text.find("column_statistics:"), std::string::npos);
  EXPECT_NE(text.find("column=\"StructB\" objects=2 rows=1"),
            std::string::npos);
  ASSERT_OK(database.Close());
}

TEST(DiagnosticsTest, ReportsNotOpen) {
  LumoDB::Database database;
  std::vector<LumoDB::ColumnStats> stats;
  std::ostringstream output;

  EXPECT_EQ(database.GetColumnStats(stats).Code(), LumoDB::StatusCode::kNotOpen);
  EXPECT_EQ(database.DumpColumnStats(output).Code(), LumoDB::StatusCode::kNotOpen);
  EXPECT_EQ(database.Dump(output).Code(), LumoDB::StatusCode::kNotOpen);
}

}  // namespace
