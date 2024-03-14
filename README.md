# Data Engineering Zoomcamp 2024 - Homework 6

## Instructions

### Install Spark Environment

#### Install / Check java version

```console
// installed previously with sdkman
java --version
openjdk 17.0.5 2022-10-18 LTS
OpenJDK Runtime Environment Corretto-17.0.5.8.1 (build 17.0.5+8-LTS)
OpenJDK 64-Bit Server VM Corretto-17.0.5.8.1 (build 17.0.5+8-LTS, mixed mode, sharing)
```

#### Install / Check python version

```console
// installed previously with pyenv  
// pyenv virtualenv 3.10 pyspark-kafka
// pyenv activate pyspark-kafka 

python --version
Python 3.10.3

// pyenv deactivate  
```

#### Download Spark

```console
wget https://dlcdn.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
```

#### Decompress Spark

```console
// Unzip spark .tar.gz
tar -xzvf spark-3.5.0-bin-hadoop3.tgz
```

#### Config Spark

```console
// My local Spark is installed on my project's parent folder

// script that sets the spark environment (setenv.sh)
#!/bin/bash

export spark_version=3.5.0
export hadoop_version=3

current_dir=$(pwd)
parent_dir=$(dirname "$current_dir")

export SPARK_HOME=$parent_dir/spark-$spark_version-bin-hadoop$hadoop_version
echo "The SPARK_HOME is: $SPARK_HOME"
export PATH="${SPARK_HOME}/bin:${PATH}"

export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"
```

##### Config the setenv.sh script file permissions

```console
chmod u+x ./setenv.sh
```

##### Run the setenv.sh script

```console
. ./setenv.sh
// or just excute the export commands above
```
#### Install PySpark

```console
pip install pyspark==3.5.0
```

### Install Python libraries

```console
pip install kafka-python pyarrow pandas numpy
```

### Start Redpanda
```console
docker compose -f docker/docker-compose.yaml up -d
```

#### Stop Redpanda
```console
docker compose -f docker/docker-compose.yaml down -v
```

#### Access Redpanda terminal
```console
docker compose -f docker/docker-compose.yaml exec -it redpanda-1 bash
```

### Install Jupyter notebooks

```console
pip install jupyterlab
```
#### Run Jupyter notebooks

```console
cd notebooks
jupyter lab
```

Go to the browser on http://localhost:8888/lab page

#### Stop Jupyter notebooks

Execute Control-C twice from the console where jupyter lab is running


### Questions
Please check the [Homework Execution](./homework_execution.md) document.

