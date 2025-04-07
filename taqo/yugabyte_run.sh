python src/runner.py \
collect \
--optimizations \
--db=yugabyte \
--ddl-prefix=postgres \
--model=complex \
--ddls database,drop,create,analyze \
--config=config/default.conf \
--output=yugabyte_complex_yb \
--database=taqo