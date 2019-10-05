pwd=`pwd`
scale=$1
sqlf=tpch-createtables.sql
cp $sqlf.template $sqlf
sed -i 's|path-to-replace|'$pwd'/tpch-data/scale-'$1'|' $sqlf
PGPASSWORD=duke createdb arch-tpch-scale-$1 -h localhost -U duke
PGPASSWORD=duke psql arch-tpch-scale-$1 -h localhost -U duke -f $sqlf
