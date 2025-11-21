#pragma once

#include <string>
#include <utility>

namespace simpledb {

enum class StatusCode {
  kOk = 0,
  kInvalidArgument,
  kIoError,
  kNotFound,
  kUnimplemented,
  kInternal,
};

class Status {
 public:
  Status() : code_(StatusCode::kOk) {}
  explicit Status(StatusCode code, std::string message = {})
      : code_(code), message_(std::move(message)) {}

  static Status OK() { return Status(); }
  static Status InvalidArgument(std::string message) {
    return Status(StatusCode::kInvalidArgument, std::move(message));
  }
  static Status IoError(std::string message) {
    return Status(StatusCode::kIoError, std::move(message));
  }
  static Status NotFound(std::string message) {
    return Status(StatusCode::kNotFound, std::move(message));
  }
  static Status Unimplemented(std::string message) {
    return Status(StatusCode::kUnimplemented, std::move(message));
  }
  static Status Internal(std::string message) {
    return Status(StatusCode::kInternal, std::move(message));
  }

  bool ok() const { return code_ == StatusCode::kOk; }
  StatusCode code() const { return code_; }
  const std::string& message() const { return message_; }

 private:
  StatusCode code_;
  std::string message_;
};

template <typename T>
class Result {
 public:
  Result(const T& value) : value_(value), status_(Status::OK()) {}
  Result(T&& value) : value_(std::move(value)), status_(Status::OK()) {}
  Result(Status status) : status_(std::move(status)) {}

  bool ok() const { return status_.ok(); }
  const Status& status() const { return status_; }

  const T& value() const& { return value_; }
  T& value() & { return value_; }
  T&& value() && { return std::move(value_); }

 private:
  T value_{};
  Status status_{StatusCode::kInternal, "unset"};
};

}  // namespace simpledb
