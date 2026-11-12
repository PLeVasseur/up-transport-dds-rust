// SPDX-License-Identifier: Apache-2.0
//! Role configuration errors are diagnosed before transport startup.

#[test]
fn encoded_owned_native_roles_require_an_explicit_agreement() {
    for executable in [
        env!("CARGO_BIN_EXE_dds_publisher"),
        env!("CARGO_BIN_EXE_dds_subscriber"),
        env!("CARGO_BIN_EXE_dds_notifier"),
        env!("CARGO_BIN_EXE_dds_notifyee"),
        env!("CARGO_BIN_EXE_dds_client"),
        env!("CARGO_BIN_EXE_dds_server"),
    ] {
        let output = std::process::Command::new(executable)
            .args([
                "--route-family",
                "encoded-owned",
                "--encoding",
                "native",
                "--timeout-ms",
                "1",
            ])
            .output()
            .expect("run the actual role binary");
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("explicit native-profile agreement")
        );
        assert!(!String::from_utf8_lossy(&output.stdout).contains("READY"));
    }
}
