SELECT 1 FROM Lineitem, Orders, Customer
WHERE l_orderkey = o_orderkey
AND o_custkey = c_custkey
AND l_shipdate > date '2008-01-01'
AND l_receiptdate < date '2008-02-01'
AND l_discount < 0.05
AND o_orderpriority = 'HIGH'
AND c_mktsegment = 'AUTOMOBILE';
