package de.tub.dima.condor.benchmark.throughputUtils;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.ArrayList;

public class ParallelThroughputStatistics implements Serializable {

//	static final AtomicInteger nextId = new AtomicInteger();

	//private static ParallelThroughputStatistics statistics;
	private boolean pause;
	public ArrayList<Double> history;
//	private int parallelism = 1;

	public ParallelThroughputStatistics() {
		this.history = new ArrayList<>();
//		id = nextId.incrementAndGet();
	}

	private double counter = 0;
	private double sum = 0;
//	public int id;
//	public static ParallelThroughputStatistics getInstance() {
//		if (statistics == null)
//			statistics = new ParallelThroughputStatistics();
//		return statistics;
//	}

//	public static void setParallelism(int parallelism) {
//		if (statistics == null)
//			statistics = new ParallelThroughputStatistics();
//		statistics.parallelism = parallelism;
//	}


	public void addThrouputResult(double throuputPerS) {
		if (this.pause)
			return;
		history.add(throuputPerS);
		counter += ((double) 1);
//		counter += ((double) 1)/parallelism;
		sum += throuputPerS;
	}

	public void clean() {
		counter = 0;
		sum = 0;
	}

	public double mean() {
		return sum / counter;
	}

	public String printHistory(){
		String res = "";
		for (int i = 0; i < history.size(); i++) {
			res += history.get(i).toString()+",";
		}
		return res;
	}

	@Override
	public String toString() {
//		return "Task ID: "+id+" Throughput Mean: " + mean();
		return "Throughput Mean: " + mean();
	}

//	public void pause(final boolean pause) {
//		this.pause = pause;
//	}


	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
		out.writeDouble(counter);
		out.writeDouble(sum);
		out.writeBoolean(pause);
		out.writeObject(history);
//		out.writeInt(id);

//		out.writeObject(nextId);
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
		counter = in.readDouble();
		sum = in.readDouble();
		pause = in.readBoolean();
		history = (ArrayList<Double>) in.readObject();
//		id = in.readInt();

//		nextId = (AtomicInteger) in.readObject();
	}

	private void readObjectNoData() throws ObjectStreamException {
		System.out.println("readObjectNoData() called - should give an exception");
	}
}
