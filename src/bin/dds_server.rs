// SPDX-License-Identifier: Apache-2.0
//! DDS RPC server role binary.

mod common;

#[tokio::main]
async fn main() -> Result<(), up_rust::UStatus> {
    common::run(common::Role::Server).await
}
