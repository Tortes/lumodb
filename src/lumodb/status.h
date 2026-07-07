#pragma once

#include <string>
#include <utility>

namespace LumoDB {

enum class StatusCode {
  kOk = 0,
  kNotFound,
  kInvalidArgument,
  kIoError,
  kCorruption,
  kNotOpen,
};

class Status {
 public:
  Status() = default;

  static Status Ok() { return Status(); }
  static Status NotFound(std::string message) {
    return Status(StatusCode::kNotFound, std::move(message));
  }
  static Status InvalidArgument(std::string message) {
    return Status(StatusCode::kInvalidArgument, std::move(message));
  }
  static Status IoError(std::string message) {
    return Status(StatusCode::kIoError, std::move(message));
  }
  static Status Corruption(std::string message) {
    return Status(StatusCode::kCorruption, std::move(message));
  }
  static Status NotOpen(std::string message) {
    return Status(StatusCode::kNotOpen, std::move(message));
  }

  [[nodiscard]] bool IsOk() const { return code_ == StatusCode::kOk; }
  [[nodiscard]] StatusCode Code() const { return code_; }
  [[nodiscard]] const std::string& Message() const { return message_; }

  explicit operator bool() const { return IsOk(); }

 private:
  Status(StatusCode code, std::string message) : code_(code), message_(std::move(message)) {}

  StatusCode code_ = StatusCode::kOk;
  std::string message_;
};

}  // namespace LumoDB
