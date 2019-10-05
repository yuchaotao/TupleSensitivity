cd tpch-dbgen
echo 'Generating TPC-H data with scale '$1
./dbgen -f -s $1
mkdir ../tpch-data/scale-$1
mv *.tbl ../tpch-data/scale-$1
echo 'Moving data to ./tpc-data/scale-'$1
cd ../tpch-data/scale-$1
chmod a+wr *.tbl
chmod a-x *.tbl
echo 'Truncating extra |'
sed -i 's/|$//' *.tbl
