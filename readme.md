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
First, `cd integration-tests`.  Make sure you ran `cargo build --release` in the `optdbg` folder first! There's a hardcoded path to its target folder right now :(. 

To initially set up data, Then, run
```sh
cargo install tpchgen-cli
mkdir tpch-data && cd tpch-data
tpchgen-cli -f parquet -s [scale]
cd -
```
where `[scale]` is the scale factor you want to use for the data (pick 0.2 for something quick).

Then, you can run the following:
```sh
cargo run --release --bin test
```

