cd tpch-data
for f in *.tbl; do
	sed -i 's/|$//g' $f
	# mv -- "$f" "${f%.tbl}.csv"
done
cd - 
cd tpch-queries
grep -rlZ first . | xargs -0 sed -i 's/first/limit/g'
cd - 
