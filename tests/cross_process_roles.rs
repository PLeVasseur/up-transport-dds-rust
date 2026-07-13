// SPDX-License-Identifier: Apache-2.0
//! Cross-process role evidence over real DDS discovery and RTPS.

use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

const WAIT: Duration = Duration::from_secs(15);

#[test]
fn classic_pubsub_owned_notification_and_arrow_rpc_cross_process() {
    run_pair(
        env!("CARGO_BIN_EXE_dds_subscriber"),
        env!("CARGO_BIN_EXE_dds_publisher"),
        61,
        "classic",
        "protobuf",
    );
    run_pair(
        env!("CARGO_BIN_EXE_dds_notifyee"),
        env!("CARGO_BIN_EXE_dds_notifier"),
        62,
        "owned-frame",
        "xcdrv2",
    );
    run_pair(
        env!("CARGO_BIN_EXE_dds_server"),
        env!("CARGO_BIN_EXE_dds_client"),
        63,
        "copy-minimized",
        "arrow",
    );
    for iteration in 0..100 {
        run_pair(
            env!("CARGO_BIN_EXE_dds_server"),
            env!("CARGO_BIN_EXE_dds_client"),
            64 + iteration,
            "owned-frame",
            "arrow",
        );
    }
}

fn run_pair(passive_bin: &str, active_bin: &str, domain: i32, family: &str, encoding: &str) {
    let mut passive = role_command(passive_bin, domain, family, encoding, "dds-b", "dds-a")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn passive DDS role");
    let lines = forward_stdout(&mut passive);
    wait_for(&lines, "READY listener_registered", "passive readiness");

    let active = role_command(active_bin, domain, family, encoding, "dds-a", "dds-b")
        .output()
        .expect("run active DDS role");
    assert!(
        active.status.success(),
        "active role failed: stdout={} stderr={}",
        String::from_utf8_lossy(&active.stdout),
        String::from_utf8_lossy(&active.stderr)
    );
    assert!(String::from_utf8_lossy(&active.stdout).contains("FLOW"));

    wait_for(
        &lines,
        "FLOW observed_payload_bytes=",
        "passive flow evidence",
    );
    let status = passive.wait().expect("wait for passive DDS role");
    assert!(status.success(), "passive role exited unsuccessfully");
}

fn role_command(
    binary: &str,
    domain: i32,
    family: &str,
    encoding: &str,
    local_authority: &str,
    peer_authority: &str,
) -> Command {
    let mut command = Command::new(binary);
    command.args([
        "--domain-id",
        &domain.to_string(),
        "--origin-id",
        &format!("{local_authority}-{domain}"),
        "--route-family",
        family,
        "--encoding",
        encoding,
        "--local-authority",
        local_authority,
        "--peer-authority",
        peer_authority,
        "--timeout-ms",
        "12000",
    ]);
    command
}

fn forward_stdout(child: &mut Child) -> mpsc::Receiver<String> {
    let stdout = child.stdout.take().expect("passive stdout");
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            match line {
                Ok(line) => {
                    if sender.send(line).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
    receiver
}

fn wait_for(lines: &mpsc::Receiver<String>, expected: &str, context: &str) {
    loop {
        let line = lines
            .recv_timeout(WAIT)
            .unwrap_or_else(|error| panic!("{context} timed out: {error}"));
        if line.contains(expected) {
            return;
        }
    }
}
