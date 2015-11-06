#!/bin/bash

HADOOP_HOME=/usr/local/hadoop
JAR=contrib/streaming/hadoop-streaming-0.20.2+737.jar

HSTREAMING="$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/$JAR"

echo "now running word count via Ruby and Hadoop Streaming"

$HSTREAMING \
 -mapper  'ruby word_count_map.rb' \
 -reducer 'ruby word_count_reduce.rb' \
 -file word_count_map.rb \
 -file word_count_reduce.rb \
 -input '/user/hduser/input/*' \
 -output /user/hduser/output