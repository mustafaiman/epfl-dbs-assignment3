#!/usr/bin/env bash
GASPAR="iman"
SUBMISSION_DIR="submission/$GASPAR/exercise3"
SOURCE_BASE="src/main/java"

rm -f MVTO.pdf MVTO.log MVTO.dvi MVTO.aux
pdflatex MVTO.tex

rm -rf submission
mkdir -p $SUBMISSION_DIR/task1
mkdir -p $SUBMISSION_DIR/task2

FILES_T1=($SOURCE_BASE/MVTO.java MVTO.pdf)
FILES_T2=($SOURCE_BASE/BloomJoin.java)

copy_files() {
    par_name=$1[@]
    task_f=$2
    FILES=("${!par_name}")
    for file in "${FILES[@]}"; do
        cp $file $SUBMISSION_DIR/$task_f/.
        if [ $? -ne 0 ]
        then
            exit 1
        fi
    done;
}

copy_files FILES_T1 task1
copy_files FILES_T2 task2

zip -r submission.zip submission/$GASPAR/
