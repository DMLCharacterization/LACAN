for i in 0 1 2 3 master
do
	echo $1/$i
	python deployment/merge_low_level.py $1/$i/meminfo $1/$i/stat $1/$i/diskstats $1/$i/netstat $1/$i/low_level_metrics.csv
done
