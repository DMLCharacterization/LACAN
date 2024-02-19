
uniq $OAR_NODE_FILE > machineFiles.txt

rm -f leaders.txt

n=$(uniq $OAR_NODE_FILE | wc -l)
n=$(($n/5))

for i in $(seq 1 $n); do
	echo $i
	uniq $OAR_NODE_FILE | head -n $(($i*5)) | tail -n 5 > machineFile.txt
	./deploy.sh
	head -n 1 machineFile.txt >> leaders.txt
	rm machineFile.txt
done

rm machineFiles.txt
