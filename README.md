# Word_Prediction
BIG DATA: The Hadoop Word Prediction Project is a distributed system that analyzes large-scale text data to predict the next likely word based on n-grams. Using Hadoop's MapReduce framework, the project processes massive datasets efficiently, enabling scalable and fault-tolerant word prediction with seamless integration into Hadoop's ecosystem.

## Overview
The Hadoop Word Prediction Project is a distributed system designed to process large-scale text data and provide word predictions based on n-grams. It leverages Hadoop's MapReduce framework for efficient data processing and is scalable to handle massive datasets.

## Features
- **N-gram Generation**: Processes text data to generate n-grams of configurable sizes.
- **Word Prediction**: Predicts the most probable word(s) given a context.
- **Distributed Processing**: Utilizes Hadoop MapReduce for efficient computation.
- **Scalable Architecture**: Handles large-scale datasets across multiple nodes.
- **HDFS Integration**: Stores and retrieves input and output data using Hadoop Distributed File System (HDFS).

## Prerequisites
- **Hadoop**: Version 3.x or higher.
- **Java**: Java Development Kit (JDK) 8 or later.
- **Dataset**: A large text dataset for analysis (e.g., books, articles, or web data).

## System Architecture
1. **Mapper**: 
   - Tokenizes the input text.
   - Generates n-grams and emits them as key-value pairs (n-gram, count).
2. **Reducer**:
   - Aggregates n-gram counts.
   - Calculates probabilities for word prediction.
3. **Driver**:
   - Manages the execution of MapReduce jobs.
   - Interfaces with HDFS for data input and output.

## Usage

### Step 1: Set Up Hadoop
1. Install and configure Hadoop on your system.
2. Ensure the Hadoop cluster is running (single-node or multi-node setup).

### Step 2: Compile the Code
1. Use `javac` to compile the Java files.
2. Create a JAR file containing the compiled classes.

### Step 3: Upload Data to HDFS
Upload your text dataset to HDFS using:
```bash
hdfs dfs -put <local-path-to-dataset> <hdfs-destination-path>
