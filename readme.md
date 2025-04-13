# optdbg
> query optimizer debugger

## usage 
First, run `git submodule update --init --recursive` if you didn't clone with the submodules already!

To build and run the main CLI tool, run
```sh
cd optdbg
cargo build --release # important!
./target/debug/optdbg -o dolomite -q my_query.sql
```

# integration tests
Make sure you ran `cargo build --release` in the `optdbg` folder first! There's a hardcoded path to its target folder right now :(. 

First, `cd integration-tests`. To get set up, first run `bash ./trim.sh`. This will unzip and fix up the TPC-H dataset.

Then, you can run the following:
```sh
cargo run --release --bin test
```

