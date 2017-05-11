#!/bin/sh

cp src/main/java/MVTO.java tatest/MVTO.java
cp src/test/java/MVTOTest.java tatest/MVTOTest.java

cd tatest
bash tester.sh
