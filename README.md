#DGST

DGST(**D**istributed **G**eneralized **S**uffix **T**ree) is an efficient and scalable generalized suffix tree construction algorithm on distributed data-Parallel platforms. DGST supports indexing all suffixes for a variety of string, ranging from a single long string to multiple string of varied lengths. In addition, DGST is built on the widely-used distributed data-parallel computing Apache Spark.  

#Prerequisites

- **Apache Spark:** As DGST is built on top of Spark, you need to get the Spark installed first. If you are not clear how to setup Spark, please refer to the guidelines [here](http://spark.apache.org/docs/latest/). Currently, DGST is developed on the APIs of Spark 1.6.3 version.
- **Apache HDFS:** DGST uses HDFS as the distributed file system. If you are not clear how to setup HDFS, please refer to the guidelines [here](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html). The HDFS version is 2.6.5. 
- **Java:** The JDK version is 1.7.  

## Compile
 
DGST is built using [Apache Maven](https://maven.apache.org/). To build DGST, Run `mvn scala:compile compile package` in the root directory.

The compiled jar is available at `target/DGST-1.0-SNAPSHOT.jar`

> Note: A precompiled jar is provided in the `lib` directory.

## Run

The entry of DGST is defined in the scala class `GST.Main`.

Run DGST with the following command:

	spark-submit --master [SPARK_MASTER_ADDRESS] \
	--executor-memory 2G \
	--driver-memory 20G \
	--total-executor-cores [Total cores in the Spark cluster] \ 
	--conf spark.memory.fraction=0.9 \
	--conf spark.memory.storageFraction=0.1 \
    --driver-class-path [Configuration directory] \
	--class GST.Main \
    DGST-1.0-SNAPSHOT.jar \
    <Input path> <Output path> <Temporary path> \
    <Range size> <Maximum sub-tree size> <Parallelism>

The run command contains the following arguments:

- `<Input path>`: The input data path on HDFS or local file system.
- `<Output path>`: The output data path on HDFS or local file system.
- `<Temporary path>`: The tempoaray data path on HDFS or local file system.
- `<Range size>`: The size of *range* in the LCP-Range structure
- `<Maximum sub-tree size>`: The Maximum sub-tree size (i.e., the maximum S-prefix frequency)
- `<Range size>`: The size of *range* in the LCP-Range structure
- `<Parallelism>`: The computation parallelism on Spark

**Note:** `--driver-class-path` is the directory where the configuration file is. The arguments set in the run command have higher priority than those set in the configuration file. To find more information about the configuration file, please refer to the [Configuration Guide](https://github.com/PasaLab/DGST/wiki/DGST-Configuration).

**Note:** We also port the [ERa](http://www.vldb.org/pvldb/vol5/p049_essammansour_vldb2012.pdf) algorithm to Spark. Please replace the main class with `GST.EraMain` to run the ERa algorithm.

## Demo

The demo dataset which contais two strings can be found in `/demo` directory. Each file represents a string. 

Refer to the result generalized suffix tree [here](https://github.com/PasaLab/DGST/wiki/DGST-Demo).

## Licence

Please contact the authors for the licence info of the source code.