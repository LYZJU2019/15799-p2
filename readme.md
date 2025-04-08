# optdbg
> query optimizer debugger

## usage 
First, run `git submodule update --init --recursive` if you didn't clone with the submodules already!

To build and run the main CLI tool, run
```sh
cd optdbg
cargo build 
./target/debug/optdbg -o dolomite -q my_query.sql
```

To check integration tests, run
```sh
cd integration-tests
cargo run --bin runner
```

