package de.tub.dima.condor.benchmark.throughputUtils;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParallelThroughputLogger<T> extends RichFlatMapFunction<T, T> {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelThroughputLogger.class);

    private long totalReceived;
    private long lastTotalReceived;
    private long lastLogTimeMs;
    //	private int elementSize;
    private long logfreq;
    private boolean parallelismSet = false;
//    private String outputPath;
    private String configuration;
    private ParallelThroughputStatistics throughputStatistics;

    public ParallelThroughputLogger(long logfreq, String configuration) {
//		this.elementSize = elementSize;
        this.logfreq = logfreq;
        this.totalReceived = 0;
        this.lastTotalReceived = 0;
        this.lastLogTimeMs = -1;
        this.configuration = configuration;
//        this.outputPath = outputPath;

        this.throughputStatistics = new ParallelThroughputStatistics();
        
    }

    @Override
    public void close() throws Exception {
//        BufferedWriter resultWriter;
//        try {
////            resultWriter = new PrintWriter(new FileOutputStream(new File(outputPath), true));
//            resultWriter = new BufferedWriter(new PrintWriter(new FileOutputStream(new File(outputPath), true)));
//        } catch (FileNotFoundException e) {
//            throw new IllegalArgumentException("File not found: "+outputPath);
//        }
        LOG.info(throughputStatistics.toString());
//        resultWriter.append(configuration +
//                throughputStatistics.mean() + "\t");

        System.out.println(System.currentTimeMillis()+" : "+configuration+" : "+throughputStatistics.printHistory());
//        Environment.out.println(throughputStatistics.history.size());
//        Environment.out.println();

//        InetAddress ip = null;
//        String hostname="";
//        try {
//            ip = InetAddress.getLocalHost();
//            hostname = ip.getHostName();
//
//        } catch (UnknownHostException e) {
//
//            e.printStackTrace();
//        }
//        String res = "current Hostname:"+"\t"+ hostname+"\t"+throughputStatistics.mean() + "\t"+"\n";
////        ThroughputWriter.getThroughputWriter().append(res);
//        Environment.out.println(res);

//        resultWriter.append("current Hostname:"+"\t"+ hostname+"\t");
////        String.format("%s = %d", "joe", 35);
//        resultWriter.append(throughputStatistics.mean() + "\t");
//        resultWriter.append("\n");
//        resultWriter.flush();
//        resultWriter.close();
//        Environment.out.println(ParallelThroughputStatistics.getInstance().toString());
    }

    @Override
    public void flatMap(T element, Collector<T> collector) throws Exception {
        collector.collect(element);
//        if (!parallelismSet) {
//            ParallelThroughputStatistics.setParallelism(this.getRuntimeContext().getNumberOfParallelSubtasks());
//            parallelismSet = true;
//        }
        totalReceived = totalReceived + 1;
        long now = System.currentTimeMillis();
        if (lastLogTimeMs == -1) {
            // init (the first)
            lastLogTimeMs = now;
        }
        long timeDiff = now - lastLogTimeMs;
        if (timeDiff > logfreq) {
            long elementDiff = totalReceived - lastTotalReceived;
            double ex = (1000 / (double) timeDiff);
            LOG.info("During the last {} ms, we received {} elements. That's {} elements/second/core. ",
                    timeDiff, elementDiff, elementDiff * ex);

            throughputStatistics.addThrouputResult(elementDiff * ex);
            //Environment.out.println(ThroughputStatistics.getInstance().toString());
            // reinit
            lastLogTimeMs = now;
            lastTotalReceived = totalReceived;
        }
    }

}
