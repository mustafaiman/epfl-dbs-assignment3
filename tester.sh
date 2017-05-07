#!/bin/bash

BASEDIR=$(dirname $0)

MVTOJAVA=src/main/java/MVTO.java
MVTOTESTJAVA=src/test/java/MVTOTest.java

cd $BASEDIR
rm -rf ./classes
mkdir -p ./classes
javac -d ./classes $MVTOJAVA $MVTOTEST

for TEST in {1..5}
do
	`java -cp ./classes MVTOTest $TEST > /dev/null 2>&1`
	rc=$?
	if [[ $rc != 0 ]] ; then
	    echo "TEST $TEST: FAILED"
	fi
done