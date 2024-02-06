# Presto Oxidized

(_Prest-Ox_ and _Presto X_ are both valid pronunciations)


This module implements the Presto worker process using the Rust programming
language.

### Setup and Pre-Requisites

- Rust nightly

```bash
# install rustup
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh && \
    # install nightly toolchain
    rustup install nightly && \
    # set nightly as default toolchain
    rustup default nightly
```

- Execute the presto rust protocol generation script (run these from the root of the repository)
    
```bash
# protocol generation pre-requisites
pip3 install -U chevron pyyaml && \
    brew install jq && \
    export PRESTO_HOME="$(pwd)" && \
    # head to the protocol directory
    pushd presto-native-execution/presto_cpp/presto_protocol && \
    # generate the protocol. Writes output to ./presto-oxidized/prestox/src/protocol/resources.rs
    make presto_protocol.rs && \
    popd
```


### Compilation

Cargo is used for compilation. Run the following commands to build all parts of the prestox module:

```bash

cd ./presto-oxidixed/prestox
cargo build
```


### Running the server

At minumum, the discovery URI to the coordinator needs to be populated. The easiest way to
pass this information is through an environment variable

```bash
PRESTOX_DISCOVERY_URI="http://localhost:1234" cargo run --bin server
```