cd tpch-data
gunzip *.gz
for f in *.tbl; do
	sed -i 's/|$//g' $f
	mv -- "$f" "${f%.tbl}.csv"
done

cd - 
