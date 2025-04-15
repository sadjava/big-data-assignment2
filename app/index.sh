#!/bin/bash
echo "This script includes commands to run MapReduce jobs using Hadoop Streaming to index documents"

# Check if input path is provided
if [ -z "$1" ]; then
    echo "Usage: ./index.sh <hdfs_input_path>"
    echo "Warning: No input path specified"
    echo "Default input path: /index/data"
    INPUT_PATH="/index/data"
else
    INPUT_PATH=$1
fi

echo "Input file is: $INPUT_PATH"

# Check if input exists in HDFS
echo "Checking if input exists in HDFS..."
hdfs dfs -test -e $INPUT_PATH
if [ $? -ne 0 ]; then
    echo "Input not found in HDFS, attempting to copy from local..."
    
    # Check if local file exists
    if [ -f "$INPUT_PATH" ]; then
        hdfs dfs -mkdir -p $(dirname $INPUT_PATH)
        hdfs dfs -put $INPUT_PATH $INPUT_PATH
    else
        echo "Error: Input file not found locally either"
        exit 1
    fi
fi

# Create output directories in HDFS
OUTPUT_BASE="/tmp/index"
hdfs dfs -mkdir -p $OUTPUT_BASE

# Function to run MapReduce job
run_mapreduce() {
    job_name=$1
    mapper=$2
    reducer=$3
    input=$4
    output="${OUTPUT_BASE}/${job_name}"
    
    echo "Running $job_name job..."
    echo "Input: $input"
    echo "Output: $output"
    
    # Delete output directory if it exists
    hdfs dfs -test -e $output && hdfs dfs -rm -r $output
    
    # Run MapReduce job
    mapred streaming \
        -files /app/mapreduce/$mapper,/app/mapreduce/$reducer,/app/cassandra.zip \
        -mapper "python3 $mapper" \
        -reducer "python3 $reducer" \
        -input $input \
        -output $output
    
    if [ $? -ne 0 ]; then
        echo "Error: $job_name job failed"
        exit 1
    fi
    
    echo "$job_name job completed successfully"
}

# Run all three MapReduce jobs
run_mapreduce "doc_frequencies" "mapper1.py" "reducer1.py" $INPUT_PATH
run_mapreduce "term_frequencies" "mapper2.py" "reducer2.py" $INPUT_PATH
run_mapreduce "doc_lengths" "mapper3.py" "reducer3.py" $INPUT_PATH

echo "All indexing jobs completed successfully"
