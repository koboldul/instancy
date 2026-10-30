use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const CHILD_CASE: &str = "INSTANCY_TIMELY_CHILD_CASE";
const TIMEOUT: Duration = Duration::from_secs(120);

pub fn isolated(name: &str, body: impl FnOnce()) {
    if std::env::var(CHILD_CASE).as_deref() == Ok(name) {
        body();
        return;
    }
    run_child(name, &[]);
}

pub fn run_child(name: &str, environment: &[(&str, &str)]) {
    let mut child = Command::new(std::env::current_exe().expect("locate test executable"))
        .args([
            "--exact",
            name,
            "--include-ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD_CASE, name)
        .envs(environment.iter().copied())
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("start isolated parity test");
    let deadline = Instant::now() + TIMEOUT;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                assert!(status.success(), "{name}: child failed with {status}");
                return;
            }
            Ok(None) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            result => {
                let killed = child.kill();
                let reaped = child.wait();
                panic!(
                    "{name}: timed out or could not inspect child after {TIMEOUT:?}: \
                     {result:?}; kill={killed:?}; wait={reaped:?}"
                );
            }
        }
    }
}
