SELECT 1 FROM lineitem, orders, customer
WHERE l_orderkey = o_orderkey
AND o_custkey = c_custkey
AND l_shipdate > date '1995-01-01'
AND l_receiptdate < date '1997-02-01'
AND l_discount < 0.05
AND o_orderpriority = '2-HIGH'
AND c_mktsegment = 'AUTOMOBILE';
