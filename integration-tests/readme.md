# integration tests
To get set up, first run `bash ./trim.sh`. This will unzip and fix up the TPC-H dataset.

Then, you can use the following commands
```sh
cargo run --bin runner # for applying patches + rerunning
cargo run --bin test # for running test directly
```
