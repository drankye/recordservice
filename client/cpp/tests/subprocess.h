// Copyright (c) 2014, Cloudera, inc.

#ifndef RECORD_SERVICE_SUBPROCESS_H
#define RECORD_SERVICE_SUBPROCESS_H

#include <string>
#include <vector>

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

namespace recordservice {

// Wrapper around a spawned subprocess.
//
// program will be treated as an absolute path unless it begins with a dot or a
// slash.
//
// This takes care of creating pipes to/from the subprocess and offers
// basic functionality to wait on it or send signals.
// By default, child process only has stdin captured and separate from the parent.
// The stdout/stderr streams are shared with the parent by default.
//
// The process may only be started and waited on/killed once.
//
// Optionally, user may change parent/child stream sharing. Also, a user may disable
// a subprocess stream. A user cannot do both.
//
// Note that, when the Subprocess object is destructed, the child process
// will be forcibly SIGKILLed to avoid orphaning processes.
class Subprocess {
 public:
  Subprocess(const std::string& program,
             const std::vector<std::string>& argv);
  ~Subprocess();

  // Disable subprocess stream output.  Must be called before subprocess starts.
  void DisableStderr();
  void DisableStdout();

  // Share a stream with parent. Must be called before subprocess starts.
  // Cannot set sharing at all if stream is disabled
  void ShareParentStdin(bool  share = true) { SetFdShared(STDIN_FILENO,  share); }
  void ShareParentStdout(bool share = true) { SetFdShared(STDOUT_FILENO, share); }
  void ShareParentStderr(bool share = true) { SetFdShared(STDERR_FILENO, share); }

  // Start the subprocess. Can only be called once.
  //
  // This returns false if the fork() fails. However,
  // note that if the executable path was incorrect such that
  // exec() fails, this will still return true. You must
  // use Wait() to check for failure.
  bool Start();

  // Wait for the subprocess to exit. The return value is the same as
  // that of the waitpid() syscall. Only call after starting.
  //
  // NOTE: unlike the standard wait(2) call, this may be called multiple
  // times. If the process has exited, it will repeatedly return the same
  // exit code.
  bool Wait(int* ret) { return DoWait(ret, 0); }

  // Like the above, but does not block. This returns Status::TimedOut
  // immediately if the child has not exited. Otherwise returns Status::OK
  // and sets *ret. Only call after starting.
  //
  // NOTE: unlike the standard wait(2) call, this may be called multiple
  // times. If the process has exited, it will repeatedly return the same
  // exit code.
  bool WaitNoBlock(int* ret) { return DoWait(ret, WNOHANG); }

  // Send a signal to the subprocess.
  // Note that this does not reap the process -- you must still Wait()
  // in order to reap it. Only call after starting.
  bool Kill(int signal);

  // Return the pipe fd to the child's standard stream.
  // Stream should not be disabled or shared.
  int to_child_stdin_fd()    const { return CheckAndOffer(STDIN_FILENO); }
  int from_child_stdout_fd() const { return CheckAndOffer(STDOUT_FILENO); }
  int from_child_stderr_fd() const { return CheckAndOffer(STDERR_FILENO); }

  // Release control of the file descriptor for the child's stream, only if piped.
  // Writes to this FD show up on stdin in the subprocess
  int ReleaseChildStdinFd()  { return ReleaseChildFd(STDIN_FILENO ); }
  // Reads from this FD come from stdout of the subprocess
  int ReleaseChildStdoutFd() { return ReleaseChildFd(STDOUT_FILENO); }
  // Reads from this FD come from stderr of the subprocess
  int ReleaseChildStderrFd() { return ReleaseChildFd(STDERR_FILENO); }

  pid_t pid() const;

  // Return that args used on process startup
  const std::vector<std::string>& GetArgs() const { return argv_; }

 private:
  void SetFdShared(int stdfd, bool share);
  int CheckAndOffer(int stdfd) const;
  int ReleaseChildFd(int stdfd);
  bool DoWait(int* ret, int options);

  enum StreamMode {SHARED, DISABLED, PIPED};

  std::string program_;
  std::vector<std::string> argv_;

  enum State {
    kNotStarted,
    kRunning,
    kExited
  };
  State state_;
  int child_pid_;
  enum StreamMode fd_state_[3];
  int child_fds_[3];

  // The cached exit result code if Wait() has been called.
  // Only valid if state_ == kExited.
  int cached_rc_;
};

}
#endif
