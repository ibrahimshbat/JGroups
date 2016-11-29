package org.jgroups.protocols.jzookeeper.zabclient;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.jgroups.Message;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;

/*
 * It uses to gathering protocol stats like throughput, latency and load.
 */
public class ProtocolStats {
	private List<Long> writeLatencies;
	private List<Long> readLatencies;
	//private List<Integer> throughputInRate;
	private int read_percentage;
	private List<Double> throughputs;
	private int throughput;
	private long startThroughputTime;
	private long endThroughputTime;
	private long lastThroughputTime;
	private String protocolName;
	private int numberOfSenderInEachClient;
	private int numberOfClients;
	private AtomicInteger numRequest;
	private AtomicInteger numReqDelivered;
	private AtomicInteger lastNumReqDeliveredBefore;
	private AtomicInteger countMessageLeader;
	private AtomicInteger countTotalMessagesFollowers;
	private AtomicInteger countMessageFollower;
	private static PrintWriter outFile;
	private static PrintWriter readRequest;
	private static PrintWriter outFileToWork;
	private static PrintWriter outFileToWorkLatency;

	private String outDir;
	private String outDirWork;

	private AtomicInteger countDummyCall;
	private boolean is_warmup = true;
	public boolean isEnabled = false;

	protected final Log log = LogFactory.getLog(this.getClass());

	public ProtocolStats() {

	}

	public ProtocolStats(String protocolName, int numberOfClients,
			int numberOfSenderInEachClient, String outDir, double read_percentage,
			boolean stopWarmup, boolean isEnabled) {

		this.writeLatencies = new ArrayList<Long>();
		this.readLatencies = new ArrayList<Long>();
		//this.throughputInRate = new ArrayList<Integer>();
		this.throughputs = new ArrayList<Double>();
		this.throughput = 0;
		this.protocolName = protocolName;
		this.numberOfClients = numberOfClients;
		this.numberOfSenderInEachClient = numberOfSenderInEachClient;
		this.numRequest = new AtomicInteger(0);
		this.numReqDelivered = new AtomicInteger(0);
		this.lastNumReqDeliveredBefore = new AtomicInteger(0);
		countMessageLeader = new AtomicInteger(0);
		countMessageFollower = new AtomicInteger(0);
		countTotalMessagesFollowers = new AtomicInteger(0);
		this.countDummyCall = new AtomicInteger(0);
		this.is_warmup = stopWarmup;
		this.isEnabled = isEnabled;
		this.outDir = outDir;
		this.outDirWork = outDirWork;
		this.read_percentage = (int) (100*read_percentage);
		try {
			this.outFile = new PrintWriter(new BufferedWriter(new FileWriter(
					outDir + InetAddress.getLocalHost().getHostName()
					+ protocolName + ".log", true)));
			this.readRequest = new PrintWriter(new BufferedWriter(new FileWriter(
					outDir + InetAddress.getLocalHost().getHostName()
					+ protocolName + "read.log", true)));
			this.outFileToWork = new PrintWriter(new BufferedWriter(
					new FileWriter(outDir +InetAddress.getLocalHost().getHostName()+ protocolName + ".csv", true)));
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public int getLastNumReqDeliveredBefore() {
		return lastNumReqDeliveredBefore.get();
	}

	public void setLastNumReqDeliveredBefore(int lastNumReqDeliveredBefore) {
		this.lastNumReqDeliveredBefore.set(lastNumReqDeliveredBefore);
	}

	public int getThroughput() {
		return throughput;
	}

	public void setThroughput(int throughput) {
		this.throughput = throughput;
	}

	public long getStartThroughputTime() {
		return startThroughputTime;
	}

	public void setStartThroughputTime(long startThroughputTime) {
		this.startThroughputTime = startThroughputTime;
	}

	public long getEndThroughputTime() {
		return endThroughputTime;
	}

	public void setEndThroughputTime(long endThroughputTime) {
		this.endThroughputTime = endThroughputTime;
	}

	public String getProtocolName() {
		return protocolName;
	}

	public void setProtocolName(String protocolName) {
		this.protocolName = protocolName;
	}

	public int getNumberOfSenderInEachClient() {
		return numberOfSenderInEachClient;
	}

	public void setNumberOfSenderInEachClient(int numberOfSenderInEachClient) {
		this.numberOfSenderInEachClient = numberOfSenderInEachClient;
	}

	public int getNumberOfClients() {
		return numberOfClients;
	}

	public void setNumberOfClients(int numberOfClients) {
		this.numberOfClients = numberOfClients;
	}

	public AtomicInteger getNumRequest() {
		return numRequest;
	}

	public void setNumRequest(int numRequest) {
		this.numRequest.set(numRequest);
	}

	public int getnumReqDelivered() {
		return numReqDelivered.get();
	}

	public AtomicInteger getcountDummyCall() {
		return countDummyCall;
	}

	public void setnumReqDelivered(int numReqDelivered) {
		this.numReqDelivered.set(numReqDelivered);
	}

	public AtomicInteger getCountMessageLeader() {
		return countMessageLeader;
	}

	public void setCountMessageLeader(AtomicInteger countMessageLeader) {
		this.countMessageLeader = countMessageLeader;
	}

	public AtomicInteger getCountTotalMessagesFollowers() {
		return countTotalMessagesFollowers;
	}

	public void setCountTotalMessagesFollowers(int countTotalMessagesFollowers) {
		this.countTotalMessagesFollowers.set(countTotalMessagesFollowers);
	}

	public String getOutdir() {
		return outDir;
	}

	public AtomicInteger getCountMessageFollower() {
		return countMessageFollower;
	}

	public void setCountMessageFollower(int countMessageFollower) {
		this.countMessageFollower.addAndGet(countMessageFollower);
	}

	public void incNumRequest() {
		numRequest.incrementAndGet();
	}

	public void incnumReqDelivered() {
		numReqDelivered.incrementAndGet();
	}

	public void addCountTotalMessagesFollowers(int countTotalMessages) {
		this.countTotalMessagesFollowers.addAndGet(countTotalMessages);
	}

	public void InCountDummyCall() {
		countDummyCall.incrementAndGet();
	}

	public void addWriteLatency(long latency) {
		writeLatencies.add(latency);
	}
	public void addReadLatency(long latency) {
		readLatencies.add(latency);
	}

	//Add Throughput using rate
	public void addThroughput(double thr){
		throughputs.add(thr);
	}

	public long getLastThroughputTime() {
		return lastThroughputTime;
	}

	public void setLastThroughputTime(long lastThroughputTime) {
		this.lastThroughputTime = lastThroughputTime;
	}
	public void incCountMessageFollower() {
		this.countMessageFollower.incrementAndGet();
	}

	public void incCountMessageLeader() {
		this.countMessageLeader.incrementAndGet();
	}

	public boolean isWarmup() {
		return is_warmup;
	}

	public void setWarmup(boolean is_warmup) {
		this.is_warmup = is_warmup;
	}

	public void printProtocolStats() {

		// print Min, Avg, and Max latency
		List<Long> latAvg = new ArrayList<Long>();
		int count = 0;
		long avgTemp = 0;
		long min = Long.MAX_VALUE, avg = 0, max = Long.MIN_VALUE, FToLFAvg = 0, 
				LToFPAvg = 0, avgAll = 0, latProp = 0, latLeader = 0, minR = 0,
				maxR = 0;
		double avgAllD = 0, avgR = 0, latLeaderD = 0, throughputRate =0, 
				tempSumThr=0, CILower=0, CIUpper=0, per50th=0,per90th=0, per99th=0;

		//printLatencyToFile(latencies);
		for (long lat : writeLatencies) {
			if (lat < min) {
				min = lat;
			}
			if (lat > max) {
				max = lat;
			}
			avg += lat;
			avgTemp += lat;
			count++;
			if (count > 10000) {
				latAvg.add(avgTemp / count);
				count = 0;
				avgTemp = 0;
			}

		}

		for (long lat : readLatencies) {
			if (lat < minR) {
				minR = lat;
			}
			if (lat > maxR) {
				maxR = lat;
			}

		}
		outFile.println("Info: "+protocolName + "/" + numberOfClients + "/"
				+ numberOfSenderInEachClient);
		int read=(int)(read_percentage*100);
		int write = 100-read;
		outFile.println("Workload: Read("+read+") Write("+write+")");
		outFile.println("Number of Request Recieved: " + (numRequest));
		outFile.println("Number of Request Deliever: " + numReqDelivered);
		outFile.println("Throughput From Start and End Time: "
				+ (numReqDelivered.get() / (TimeUnit.MILLISECONDS
						.toSeconds(endThroughputTime - startThroughputTime)))+"/sec");

		for (double th:throughputs){
			tempSumThr+=th;
		}
		throughputRate = tempSumThr/throughputs.size();
		outFile.println("Throughput Rates = " + throughputs);
		outFile.println("Throughput Rate average = " + throughputRate);
		//Write Latencies
		if(writeLatencies.size() !=0){
			outFile.println("Write Latencies Size: " + writeLatencies.size());
			avgAllD = average(writeLatencies);
			outFile.println("Write Latency: [Min= " + ((double) (min)/1000000.0) + " Avg= " +
					avgAllD  + " Max= " +((double) (max)/1000000.0)+"]");
			String CI95= computeCI(avgAllD);
			outFile.println("95% confidence interval: "+CI95);		
			per50th = percentile50th(writeLatencies);
			per90th = percentile90th(writeLatencies);
			outFile.println("Write Median: "+per50th);
			outFile.println("Write 90th percentile: "+per90th);
			per99th = percentile99th(writeLatencies);
			outFile.println("Write 99th percentile: "+per99th);
		}
		else
			outFile.println("No write Request");

		//Write Latencies
		if(readLatencies.size() !=0){
			outFile.println("Read Latencies Size: " + readLatencies.size());		 
			avgR = average(readLatencies);
			avgAllD = (avgAllD) / 1000000.0;
			avgR = (avgR) / 1000000.0;	
			outFile.println("Read Latency: [Min= " + ((double) (minR)/1000000.0) + " Avg= " +
					avgR  + " Max= " +((double) (maxR)/1000000.0)+"]");		
			per50th = percentile50th(readLatencies);
			per90th = percentile90th(readLatencies);
			outFile.println("Read Median: "+per50th);
			outFile.println("Read 90th percentile: "+per90th);
			per99th = percentile99th(readLatencies);
			outFile.println("Read 99th percentile: "+per99th);
		}
		else
			outFile.println("No Read Request");
		
		if(writeLatencies.size() !=0)
			findDist(writeLatencies, outFileToWork);
		if(readLatencies.size() !=0)
			findDist(readLatencies, readRequest);

		if(writeLatencies.size() !=0){
			outFileToWork.println();
			for (long lat:writeLatencies){
				outFileToWork.println((double) (((double) lat)/1000000));
			}
		}

		if(readLatencies.size() !=0){
			readRequest.println();
			for (long lat:readLatencies){
				readRequest.println((double) (((double) lat)/1000000));
			}
		}
		//latnecyDistribution(latencies);

		outFile.println("Test Generated at "
				+ new Date()
				+ " /Lasted for = "
				+ TimeUnit.MILLISECONDS
				.toSeconds((endThroughputTime - startThroughputTime))+" Sec");
		outFile.println();
		outFile.close();
		readRequest.close();
		outFileToWork.println();
		outFileToWork.close();
		System.out.println("Finished");

	}

	public void findDist(List<Long> data, PrintWriter out){
		List<Double> latencies = new ArrayList<Double>();
		int x0T0D5=0, x3D5=0, x4=0, x4D5=0, x5=0, x10=0, x100=0,x300=0, x500=0, x800=0,
				x1000=0, x600=0, x650=0, x700=0, xLager1000=0, x1=0, x1D5=0,x2=0,x2D5=0,x3=0;
		for (int i = 0; i < data.size(); i++) {
			latencies.add((double) (((double) data.get(i))/1000000));
		}
		Collections.sort(latencies);


		for (double l:latencies){
			if (l<=0.500000)
				x0T0D5++;
			else if(l>0.500000 && l<=1.00000)
				x1++;
			else if(l>1.00000 && l<=1.500000)
				x1D5++;
			else if(l>1.500000 && l<=2.000000)
				x2++;
			else if(l>2.000000 && l<=2.500000)
				x2D5++;
			else if(l>2.500000 && l<=3.000000)
				x3++;
			else if(l>3.000000 && l<=3.500000)
				x3D5++;
			else if(l>3.500000 && l<=4.000000)
				x4++;
			else if(l>4.000000 && l<=4.500000)
				x4D5++;
			else if(l>4.500000 && l<=10.000000)
				x10++;
			else if(l>10.000000 && l<=100.000000)
				x100++;
			else if(l>100.000000 && l<=300.000000)
				x300++;
			else if(l>300.000000 && l<=500.000000)
				x500++;
			else if(l>500.000000 && l<=800.000000)
				x800++;
			else if(l>800.000000 && l<=1000.0000000)
				x1000++;
			else
				xLager1000++;
		}
		out.println("(0-0.5), " + x0T0D5);
		out.println("(0.5-1), " + x1);
		out.println("(1-1.5), " + x1D5);
		out.println("(1.5-2), " + x2);
		out.println("(2-2.5), " + x2D5);
		out.println("(2.5-3), " + x3);
		out.println("(3-3.5), " + x3D5);
		out.println("(3.5-4), " + x4);
		out.println("(4-4.5), " + x4D5);
		out.println("(4.5-10), " + x10);
		out.println("(10-100), " + x100);
		out.println("(100-300), " + x300);
		out.println("(300-500), " + x500);
		out.println("(500-800), " + x800);
		out.println("(800-1000), " + x1000);
		out.println("(>1000), " + xLager1000);    

	}

	public double average(List<Long> data) {
		long sum = 0;
		double avg = 0;
		for (int i = 0; i < data.size(); i++) {
			sum += data.get(i);
		}
		avg = (double) sum / data.size();
		return avg;

	}

	public void printLatencyToFile(List<Long> data) {
		for (int i = 0; i < data.size(); i++) {
			outFileToWorkLatency.println(data.get(i));
		}
		outFileToWorkLatency.println("End");
		outFileToWorkLatency.println();
		outFileToWorkLatency.close();
	}

	public void latnecyDistribution(List<Long> data) {
		List<Double> latencies = new ArrayList<Double>();
		int partitionSize = 20;

		for (int i = 0; i < data.size(); i++) {
			latencies.add((double) (((double) data.get(i))/1000000));
		}
		final Multiset<Double> multiset = TreeMultiset.create(latencies);

		final Iterable<List<Double>> partitions = Iterables.partition(
				multiset.elementSet(),
				multiset.elementSet().size() / partitionSize);

		int partitionIndex = 0;
		List<Double> larageLatencies = new ArrayList<Double>();
		for (final List<Double> partition : partitions) {

			// count the items in this partition
			int count = 0;
			if(partitionIndex==partitionSize-1){
				int index = 0;
				while(index<partition.size()){
					larageLatencies.add(partition.get(index));
					index = index+(partition.size()/30);
				}
				for (int i=partition.size()-10;i<partition.size();i++){
					larageLatencies.add(partition.get(i));
				}
			}
			if(partitionIndex==partitionSize){
				for (int i=0;i<partition.size();i++){
					larageLatencies.add(partition.get(i));
				}
			}

			for (final Double item : partition) {
				count += multiset.count(item);
			}

			outFile.println("Partition " + ++partitionIndex + " contains "
					+ count + " latencies from "
					+ partition.get(0) + " to "
					+ partition.get(partition.size() - 1));
		}
		outFile.println("Check Large Latencies "+ larageLatencies);

	}

	public String computeCI(double mean){
		double marginError =0.0, ci95 = 1.96, CILower=0.0, CIUpper=0.0;
		double stdev = stdev(writeLatencies, mean);
		marginError = ci95 * (stdev/(Math.sqrt(writeLatencies.size())));
		CILower = mean - marginError;
		CIUpper = mean + marginError;
		return "("+CILower+","+CIUpper+")";
	}

	public double percentile50th(List<Long> latInLong){
		List<Long> latenciesInLong = new ArrayList<Long>(latInLong);
		List<Double> latenciesInDouble = new ArrayList<Double>();
		for (int i = 0; i < latInLong.size(); i++) {
			latenciesInDouble.add((double) (((double) latenciesInLong.get(i))/1000000));
		}
		Collections.sort(latenciesInDouble);
		return latenciesInDouble.get((int)(latenciesInDouble.size()*0.50)-1);

	}

	public double percentile90th(List<Long> latInLong){
		List<Long> latenciesInLong = new ArrayList<Long>(latInLong);
		List<Double> latenciesInDouble = new ArrayList<Double>();
		for (int i = 0; i < latenciesInLong.size(); i++) {
			latenciesInDouble.add((double) (((double) latenciesInLong.get(i))/1000000));
		}
		Collections.sort(latenciesInDouble);
		return latenciesInDouble.get((int)(latenciesInDouble.size()*0.90)-1);

	}

	public double percentile99th(List<Long> latInLong){
		List<Long> latenciesInLong = new ArrayList<Long>(latInLong);
		List<Double> latenciesInDouble = new ArrayList<Double>();
		for (int i = 0; i < latenciesInLong.size(); i++) {
			latenciesInDouble.add((double) (((double) latenciesInLong.get(i))/1000000));
		}
		Collections.sort(latenciesInDouble);
		return latenciesInDouble.get((int)(latenciesInDouble.size()*0.99)-1);

	}


	public double stdev(List<Long> latInLong, double mean){
		List<Double> latenciesInDouble = new ArrayList<Double>();
		List<Double> distFromMean = new ArrayList<Double>();
		for (int i = 0; i < latInLong.size(); i++) {
			latenciesInDouble.add((double) (((double) latInLong.get(i))/1000000));
		}
		double sumDistFromMean = 0.0, stdev=0.0;
		for(int i = 0; i < latenciesInDouble.size(); i++){
			distFromMean.add(Math.pow((latenciesInDouble.get(i)-mean),2));
		}

		for(int i = 0; i < distFromMean.size(); i++){
			sumDistFromMean += distFromMean.get(i);
		}

		stdev = Math.sqrt((sumDistFromMean/latenciesInDouble.size()));

		return stdev;
	}

}