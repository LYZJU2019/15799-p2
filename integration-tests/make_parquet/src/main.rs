use test_utils::tpch::tpch_schemas;
use std::sync::Arc;
use std::collections::HashMap;

fn main() {
	let args: Vec<String> = std::env::args().collect();
	let mut tables = HashMap::new();
	for tableref in tpch_schemas() {
		tables.insert(
			tableref.name,
			tableref.schema,
		);
	}
	for entry in glob::glob(&format!("{}/*.tbl", args[1])).unwrap() {
		if let Ok(path) = entry {
			let path2 = path.clone();
			let name = path2.as_path().file_name().unwrap().to_str().unwrap();
			let pathstr = path.clone().into_os_string().into_string().unwrap();
			let out_path = format!("{}.parquet", &pathstr[..pathstr.len()-4]);
			let mut opts = csv2parquet::Opts::new(path, out_path.into());
			opts.delimiter = '|';
			opts.schema = Some(tables[&name[0..name.len()-4]].clone());
			csv2parquet::convert(opts).unwrap();
		}
	}
}
