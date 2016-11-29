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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;

public class ServerStats {


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
	private AtomicInteger numReqDelivered = new AtomicInteger(0);
	private AtomicInteger lastNumReqDeliveredBefore;
	private static PrintWriter outFile;
	private String outDir;

	protected final Log log = LogFactory.getLog(this.getClass());

	public ServerStats() {

	}

	public ServerStats(String protocolName, int numberOfClients,
			int numberOfSenderInEachClient, String outDir, int workload,
			boolean stopWarmup, boolean isEnabled) {

		this.throughputs = new ArrayList<Double>();
		this.throughput = 0;
		this.protocolName = protocolName;
		this.numberOfClients = numberOfClients;
		this.numberOfSenderInEachClient = numberOfSenderInEachClient;
		this.numRequest = new AtomicInteger(0);
		this.numReqDelivered = new AtomicInteger(0);
		this.lastNumReqDeliveredBefore = new AtomicInteger(0);
		this.outDir = outDir;
		this.read_percentage = workload;
		try {
			this.outFile = new PrintWriter(new BufferedWriter(new FileWriter(
					outDir + InetAddress.getLocalHost().getHostName()
					+ protocolName + ".log", true)));
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

	

	public void setnumReqDelivered(int numReqDelivered) {
		this.numReqDelivered.set(numReqDelivered);
	}
	
	public String getOutdir() {
		return outDir;
	}


	public void incNumRequest() {
		numRequest.incrementAndGet();
	}

	public void incnumReqDelivered() {
		numReqDelivered.incrementAndGet();
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
	
	public void printProtocolStats() {

		double throughputRate =0, tempSumThr=0;
		outFile.println("Info: "+protocolName + "/" + numberOfClients + "/"
				+ numberOfSenderInEachClient);
		outFile.println("Workload: Read("+read_percentage+") Write("+(100 - read_percentage)+")");
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

		outFile.println("Test Generated at "
				+ new Date()
				+ " /Lasted for = "
				+ TimeUnit.MILLISECONDS
				.toSeconds((endThroughputTime - startThroughputTime))+" Sec");
		outFile.println();
		outFile.close();
		System.out.println("Finished");

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




}
