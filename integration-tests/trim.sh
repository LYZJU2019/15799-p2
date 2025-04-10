cd tpch-data
gunzip *.gz
for f in *.tbl; do
	sed -i 's/|$//g' $f
done
cd - 
