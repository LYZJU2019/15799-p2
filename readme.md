# optdbg
> query optimizer debugger

## usage 
To build and run the main CLI tool, run
```sh
cd optdbg
cargo build 
./target/debug/optdbg -q my_query.sql
```

To check integration tests, run
```sh
cd integration-tests
cargo run --bin runner
```

