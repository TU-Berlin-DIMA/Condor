# Condor: Efficient Approximate Stream Processing based on Synopses

This repository provides Condor, a framework that allows for the specification of synopsis-based streaming jobs on top of general dataflow processing systems.

### Features:
- Synopsis generalization as aggregate window operators.
- High performance synopsis maintenance and evaluation. 
- Linear scalability to the number of available cores in the system.
- Allows parallel computation for Samples, Histograms, Wavelets, and Sketches.
- Provide a collection of 12 synopsis representatives. 
- Simple API to implement user-defined synopses.
- Support for Bucketing and general stream slicing as window processing strategies.
- Stable performance in scenarios with a high number of concurrent windows.
- Connector for [Apache Flink](https://flink.apache.org/).
- Connector for [Scotty Window Processor](https://github.com/TU-Berlin-DIMA/scotty-window-processor/).

### Flink/Scotty Integration Example:

```java
// Set up other configuration parameters
Class<CountMinSketch> synopsisClass = CountMinSketch.class;
Window[] windows = {new TumblingWindow(WindowMeasure.Time, 10000)};
Object[] synopsisParameters = new Object[]{633, 5, 7L};

BuildConfiguration config = new BuildConfiguration(inputStream, synopsisClass, windows, synopsisParameters, parallelism);

// Build the synopses
SingleOutputStreamOperator<WindowedSynopsis<CountMinSketch>> synopsesStream = SynopsisBuilder.build(env, config);

// Evaluate the synopsis stream based on the query stream
SingleOutputStreamOperator<QueryResult<Integer, Integer>> resultStream = ApproximateDataAnalytics.queryLatest(synopsesStream, queryStream, new QueryCountMin());
```

### Setup:
Condor has dependencies to Scotty Window Processor, which is not publically available in the maven repository. Please make sure to install it first by building it from source.
You can find Scotty's install guide [here](https://github.com/TU-Berlin-DIMA/scotty-window-processor/). 

Condor's maven package is currently not publicly available.
Therefore we have to build it from source:

`
git clone git@github.com:TU-Berlin-DIMA/Condor.git
`

`
mvn clean install
`

Then you can use the library in your maven project.

```xml
<dependency> 
  <groupId>de.tub.dima.condor</groupId>
  <artifactId>flinkScottyConnector</artifactId>
  <version>0.1</version>
</dependency>
```

### Writing your first Condor Job:
You can use the `de.tub.dima.condor.demo` package to write and compile your first Condor Synopsis-based Streaming Job. 
Just set up the SynopsisBasedStreamingJob main class and run it on your favorite IDE.

Alternatively is possible to compile the job to a jar file with maven and submit it as a streaming job to a Flink cluster:

`
cd demo/
`

`
mvn clean package
`

After initializing a Flink cluster:

`
<FLINK-HOME>/bin/flink run <CONDOR-HOME>/demo/target/demo-0.1.jar
`

### Running Benchmarking Examples:
Condor provides multiple examples that we used to benchmark Condor's efficiency and performance. It is possible to run all the experiments locally on your favorite IDE.
However, to run any of the benchmarking jobs in a cluster, set up the `dataFilePath` from all the data sources (`de.tub.dima.condor.benchmark.sources`) to a directory that is reachable for all nodes in the cluster. Then you need to compile the job to a jar file with maven and submit it as a streaming job to a Flink cluster:

`
cd benchmark/
`

`
mvn clean package
`

After initializing a Flink cluster:

`
<FLINK-HOME>/bin/flink run <CONDOR-HOME>/benchmark/target/benchmark-0.1.jar <RUN-PARAMETERS>
`

Please check the javadoc on `de.tub.dima.condor.benchmark.Runner` to see a description of all run-parameters.

Quick example, run `de.tub.dima.condor.benchmark.reliablility.CountMinAccuracy` test:

`
<FLINK-HOME>/bin/flink run <CONDOR-HOME>/benchmark/target/benchmark-0.1.jar cma -p <DESIRED-PARALLELISM> -o <OUTPUT-DIRECTORY>
`
