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

### Benchmark

We now evaluate the efficiency of our summarization libraries and compare it with the [Yahoo! DataSketches libraries](https://datasketches.apache.org/). We observe that our processing strategies significantly outperform Yahoo. Yahoo’s implementation only outperforms Condor in scenarios with very low parallelism (below eight cores) because of Flink’s system overhead. However, we observe that already with two quadcore computers, our system achieves better performance. More interestingly, we observe that Condor’s HLL implementation scales linearly with the number of available cores and outperforms Yahoo by more than one order of magnitude (46× faster). It can achieve this due to its divide and conquer design, making better use of the system’s parallelism.

<img src="https://user-images.githubusercontent.com/38589698/133761967-53afd74c-7a0b-438e-8713-fee28d006da0.png" width="550">

An essential feature of Condor is that it allows users to implement their synopses via a simple API: Users can focus on the application logic instead of intricate internal details. We tested this feature by adapting Yahoo’s HLL sketch implementation to our API, showing that Condor enables any mergeable synopsis to scale linearly with the parallelism. The next figure illustrates the results of this experiment. We see that the improvements in Yahoo’s HLL sketch’s scalability and performance when using Condor are remarkable. Now Yahoo’s HLL sketch performance is very similar to Condor’s original implementation. More importantly, Yahoo’s HLL sketch now scales linearly with the system’s parallelism (see log scale plot). Note that achieving this was easy as Condor’s API is simple to use. 

<img src="https://user-images.githubusercontent.com/38589698/133761970-bc7cf271-c92f-4fa2-bb8d-77738575d59c.png" width="600">

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

### Publications
#### In the Land of Data Streams where Synopses are Missing, One Framework to Bring Them All

**Abstract:**  
In pursuit of real-time data analysis, approximate summarization structures, i.e., synopses, have gained importance over the years. However, existing stream processing systems, such as Flink, Spark, and Storm, do not support synopses as first class citizens, i.e., as pipeline operators. Synopses’ implementation is upon users. This is mainly because of the diversity of synopses, which makes a unified implementation difficult. We present Condor, a framework that supports synopses as first class citizens. Condor facilitates the specification and processing of synopsis-based streaming jobs while hiding all internal processing details. Condor’s key component is its model that represents synopses as a particular case of windowed aggregate functions. An inherent divide and conquer strategy allows Condor to efficiently distribute the computation, allowing for high-performance and linear scalability. Our evaluation shows that Condor outperforms existing approaches by up to a factor of 75x and that it scales linearly with the number of cores.

- Paper: [In the Land of Data Streams where Synopses are Missing, One Framework to Bring Them All](http://vldb.org/pvldb/vol14/p1818-poepsel-lemaitre.pdf)

- BibTeX citation:
```
@article{poepsel2021land,
  title={In the Land of Data Streams where Synopses are Missing, One Framework to Bring Them All},
  author={Poepsel-Lemaitre, Rudi and Kiefer, Martin and von Hein, Joscha and Quian{\'e}-Ruiz, Jorge-Arnulfo and Markl, Volker},
  journal={Proceedings of the VLDB Endowment},
  volume={14},
  number={10},
  pages={1818 -- 1831},
  year={2021},
  publisher={VLDB Endowment}
}
```

Acknowledgements: This work has received funding from the German Ministry for Education and Research as BIFOLD - Berlin Institute for the Foundations of Learning and Data (01IS18025A and 01IS18037A) and Software Campus (01|S17052).

