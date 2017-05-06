#!/usr/bin/env bash
GASPAR="iman"
SUBMISSION_DIR="submission/$GASPAR/exercise3"

javac $SUBMISSION_DIR/task1/*.java
if [ $? -ne 0 ]
then
    exit 1
fi

javac $SUBMISSION_DIR/task2/*.java
if [ $? -ne 0 ]
then
    exit 1
fi
