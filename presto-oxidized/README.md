# Presto Oxidized (Prestox)

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

- Install python's `virtualenv` and `jq`
    
```bash
# protocol generation pre-requisites
pip3 install -U virtualenv && \
    brew install jq && \
```


### Compilation

Cargo is used for compilation. However, a command to generate the presto protocol structs needs
to be executed before the project can be compiled. It is recommended to use maven to compile the
first time before building the rust code directly.

```bash
./mvnw install -pl presto-oxidized -am -DskipTests
```

Under the hood, we're running `make presto_protocol.rs` with a few other dependencies. The script
and logic for this part can be found under `./presto-native-execution/presto_cpp/presto_protocol`

This should generate rust implementations of Presto's JSON serializable objects. You can build the
rust code directly under the `prestox` directory.

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