# optdbg
> query optimizer debugger

## usage 
First, run `git submodule update --init --recursive` if you didn't clone with the submodules already!

# integration tests
First, `cd integration-tests`.  Make sure you ran `cargo build --release` in the `optdbg` folder first! We rely on one of the binary targets in that crate.

To initially set up data, run
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

