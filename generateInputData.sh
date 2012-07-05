#!/usr/bin/env bash
 
WORK_DIR=`dirname "$0"`
WORK_DIR=`cd "$GRID_DIR"; pwd`
source env_variables

# Smaller data set is used by default.
COMPRESSED_DATA_BYTES=102400
UNCOMPRESSED_DATA_BYTES=102400

# Number of partitions for output data
NUM_MAPS=20

## Data sources
export INPUT_DATA=/gridmix/data

export GREP_DATA=${INPUT_DATA}/grep_data

#for grep
${HADOOP_HOME}/bin/hadoop jar \
  ${EXAMPLE_JAR} randomtextwriter \
  -D test.randomtextwrite.total_bytes=${UNCOMPRESSED_DATA_BYTES} \
  -D test.randomtextwrite.bytes_per_map=$((${UNCOMPRESSED_DATA_BYTES} / ${NUM_MAPS})) \
  -D test.randomtextwrite.min_words_key=1 \
  -D test.randomtextwrite.max_words_key=10 \
  -D test.randomtextwrite.min_words_value=0 \
  -D test.randomtextwrite.max_words_value=200 \
  -D mapred.output.compress=false \
  -outFormat org.apache.hadoop.mapred.TextOutputFormat \
  ${GREP_DATA} &


