# Condor: Efficient Approximate Stream Processing based on Synopses

This repository provides Condor, a framework that allows for the specification of synopsis-based streaming jobs on top of general dataflow processing systems.

### Features:
- Synopsis generalization as aggregate window operators.
- High performance synopsis maintanence and evaluation. 
- Linear scalelability to the number of available cores in the system.
- Allows parallel computation for Samples, Histograms, Wavelets, and Sketches.
- Provide a collection of 12 synopsis representatives. 
- Simple API to implement own user-defined synopses.
- Support for Bucketing and general stream slicing as window processing strategies.
- Stable performance in scenarios with high number of concurrent windows.
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
Condor has dependencies to Scotty Window Processor, which is not publically available in the maven repository. Please make sure to intall it first by building it from source.
Scotty's install guide can be found [here](https://github.com/TU-Berlin-DIMA/scotty-window-processor/). 

Condor's maven package is currently not publically available.
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

You can use the demo package to write and compile your first Condor Synopsis-based Streaming Job. 
Just set up the SynopsisBasedStreamingJob main class and run it on your favorite IDE.

Alternativelly is possible to create a jar with maven and submit the job to a Flink cluster:

`
cd demo/
mvn clean package
`

After inializing a Flink cluster:

`
<FLINK-HOME>/bin/flink run <CONDOR-HOME>/demo/target/demo-0.1.jar
`

### Runnning Benchmarking Examples
TODO...
