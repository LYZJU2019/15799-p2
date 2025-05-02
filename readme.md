# optdbg
> query optimizer debugger

optdbg is a tool for detecting errors in cardinality and cost estimation in query optimizers. As with existing methods like TAQO and OptMark, it 
1) samples alternate query plans for a given query 
2) benchmarks each plan and its subplans
3) does simple static analysis to find common errors with cardinality / cost estimation
These are then aggregated across a query workload to report common errors and optimizer metrics.

## usage 
First, run `git submodule update --init --recursive` if you didn't clone with the submodules already!

# integration tests
First, `cd integration-tests`.  Make sure you ran `cargo build --release` in the `optdbg` folder first! We rely on one of the binary targets in that crate.

To initially set up sample TPC-H data and queries, run
```sh
bash ./data.sh
```
You can then do things like `cargo test` or 
```sh
git 
cd ../optdbg/optd-original
git apply ../../integration-tests/join_underestimate.patch
cd -
cargo run --release --bin test -- --expect-err "HashJoinExec" --expect-kind "cardinality" --expect-by "under" 
```

