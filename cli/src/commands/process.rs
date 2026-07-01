use crate::error::{ActrCliError, Result};
use std::time::{Duration, Instant};

#[cfg(unix)]
pub(crate) fn terminate_process(pid: u32) -> Result<bool> {
    use nix::errno::Errno;
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;

    let pid = i32::try_from(pid)
        .map_err(|_| ActrCliError::command_error(format!("Invalid PID {}", pid)))?;
    match kill(Pid::from_raw(pid), Signal::SIGTERM) {
        Ok(()) => Ok(true),
        Err(Errno::ESRCH) => Ok(false),
        Err(error) => Err(ActrCliError::command_error(format!(
            "Failed to send SIGTERM to {}: {}",
            pid, error
        ))),
    }
}

#[cfg(unix)]
pub(crate) fn kill_process(pid: u32) -> Result<bool> {
    use nix::errno::Errno;
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;

    let pid = i32::try_from(pid)
        .map_err(|_| ActrCliError::command_error(format!("Invalid PID {}", pid)))?;
    match kill(Pid::from_raw(pid), Signal::SIGKILL) {
        Ok(()) => Ok(true),
        Err(Errno::ESRCH) => Ok(false),
        Err(error) => Err(ActrCliError::command_error(format!(
            "Failed to send SIGKILL to {}: {}",
            pid, error
        ))),
    }
}

#[cfg(not(unix))]
pub(crate) fn terminate_process(_pid: u32) -> Result<bool> {
    Err(ActrCliError::command_error(
        "stop is only supported on Unix systems".to_string(),
    ))
}

#[cfg(not(unix))]
pub(crate) fn kill_process(_pid: u32) -> Result<bool> {
    Err(ActrCliError::command_error(
        "process control is only supported on Unix systems".to_string(),
    ))
}

pub(crate) async fn wait_for_exit(pid: u32, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if !is_process_alive(pid) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    !is_process_alive(pid)
}

#[cfg(unix)]
pub(crate) fn is_process_alive(pid: u32) -> bool {
    use nix::errno::Errno;
    use nix::sys::signal::kill;
    use nix::unistd::Pid;

    let Ok(pid) = i32::try_from(pid) else {
        return false;
    };

    match kill(Pid::from_raw(pid), None) {
        Ok(()) => true,
        Err(Errno::EPERM) => true,
        Err(Errno::ESRCH) => false,
        Err(_) => false,
    }
}

#[cfg(not(unix))]
pub(crate) fn is_process_alive(_pid: u32) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn is_process_alive_returns_false_for_nonexistent_pid() {
        assert!(!is_process_alive(9_999_999));
    }

    #[cfg(unix)]
    #[test]
    fn terminate_process_returns_false_for_nonexistent_pid() {
        assert!(!terminate_process(9_999_999).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn kill_process_returns_false_for_nonexistent_pid() {
        assert!(!kill_process(9_999_999).unwrap());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn wait_for_exit_returns_true_for_nonexistent_pid() {
        assert!(wait_for_exit(9_999_999, std::time::Duration::from_secs(1)).await);
    }
}
