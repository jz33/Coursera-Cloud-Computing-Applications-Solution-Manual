#!/bin/bash
$SPARK/bin/spark-submit \
--class WordCount \
--master local[8] \
target/scala-2.11/simple-project_2.11-1.0.jar \
data/titles.txt \
output
