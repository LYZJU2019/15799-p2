python src/runner.py \
collect \
--optimizations \
--db=postgres \
--model=complex \
--ddls drop,create,analyze \
--config=config/default.conf \
--output=postgres_complex_pg \
--database=taqo 