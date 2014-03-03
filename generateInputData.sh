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
export INPUT_DATA=/workloadgen/data

export GREP_DATA=${INPUT_DATA}/grep_data
export VARINFLTEXT=${INPUT_DATA}/sort_data
export VARCOMPSEQ=${INPUT_DATA}/web_data

#clear existed data
${HADOOP_HOME}/bin/hadoop fs -rmr /workloadgen/data

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

#for sort
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
  ${VARINFLTEXT} &

#for web simulated data blocks
${HADOOP_HOME}/bin/hadoop jar \
  ${EXAMPLE_JAR} randomtextwriter \
  -D test.randomtextwrite.total_bytes=${COMPRESSED_DATA_BYTES} \
  -D test.randomtextwrite.bytes_per_map=$((${COMPRESSED_DATA_BYTES} / ${NUM_MAPS})) \
  -D test.randomtextwrite.min_words_key=5 \
  -D test.randomtextwrite.max_words_key=10 \
  -D test.randomtextwrite.min_words_value=100 \
  -D test.randomtextwrite.max_words_value=10000 \
  -D mapred.output.compress=true \
  -D mapred.map.output.compression.type=BLOCK \
  -outFormat org.apache.hadoop.mapred.TextOutputFormat \
  ${VARCOMPSEQ} &

