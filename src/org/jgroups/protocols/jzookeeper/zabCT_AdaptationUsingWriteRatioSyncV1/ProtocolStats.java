package org.jgroups.protocols.jzookeeper.zabCT_AdaptationUsingWriteRatioSyncV1;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;

/*
 * It uses to gathering protocol stats like throughput, latency and load.
 */
public class ProtocolStats {
	private List<Long> latencies;
	private List<Long> readLatencies;
	private List<Double> throughputs;
	private List<Long> recievedRequestTime;
	private List<String> infoForp;
	private int throughput;
	private long startThroughputTime;
	private long endThroughputTime;
	private long lastThroughputTime;
	private String protocolName;
	private int numberOfSenderInEachClient;
	private int numberOfClients;
	private long lastRecievedTime;
	private AtomicInteger numRequest;
	private AtomicInteger numReqDelivered;
	private AtomicInteger lastNumReqDeliveredBefore;
	public AtomicInteger countACKPerBroadcast;
	private static PrintWriter outFile;
	private static PrintWriter outFileToWork;
	private static PrintWriter outFileAllLat;
	private static PrintWriter writeLat;
	private static PrintWriter readLat;
	private static PrintWriter allLat;
	private static PrintWriter allInfoZanCT;
	private static PrintWriter outThroughput;

	public String dirTestType=null;
	public String info=null;
	public AtomicInteger numProposal;
	public AtomicInteger lastNumProposal;
	private String outDir;
	private AtomicInteger countDummyCall;
	private boolean is_warmup = true;
	private List<Integer> numAckPerBroadcast;
	private List<String> results;
	private AtomicInteger countTotalMessagesFollowers;

	//For compare number of Acks between Zab and ZabCT Adaptation
	public AtomicInteger countAckMessage;

	private long startTimeRatio=System.currentTimeMillis();

	private TreeMap<Double, Double> extrapW; //For store more ps if the defult ps table does not work
	protected final Log log = LogFactory.getLog(this.getClass());
	public ProtocolStats() {

	}

	public ProtocolStats(String protocolName, int numberOfClients,
			int numberOfSenderInEachClient, String outDir,
			boolean stopWarmup, String info) {
		this.infoForp = new ArrayList<String>();
		this.latencies = new ArrayList<Long>();
		this.readLatencies = new ArrayList<Long>();
		this.throughputs = new ArrayList<Double>();
		this.extrapW  = new TreeMap<Double, Double>();
		this.recievedRequestTime = new ArrayList<Long>();
		this.throughput = 0;
		this.protocolName = protocolName;
		this.numberOfClients = numberOfClients;
		this.numberOfSenderInEachClient = numberOfSenderInEachClient;
		this.lastRecievedTime = 0;
		this.numRequest = new AtomicInteger(0);
		this.numReqDelivered = new AtomicInteger(0);
		this.lastNumReqDeliveredBefore = new AtomicInteger(0);
		this.countDummyCall = new AtomicInteger(0);
		countTotalMessagesFollowers = new AtomicInteger(0);
		this.numProposal = new AtomicInteger(0);
		this.lastNumProposal =  new AtomicInteger(0);
		this.is_warmup = stopWarmup;
		this.outDir = outDir;
		this.info = info;
		System.out.println("Info="+info);
		this.countACKPerBroadcast =  new AtomicInteger(0);
		this.numAckPerBroadcast = new ArrayList<Integer>();
		this.results = new ArrayList<String>();
		this.countAckMessage = new AtomicInteger(0);
		System.out.println("Dir="+outDir);
		if (info.length()>1){
			this.dirTestType = "R-"+ (Double.parseDouble(info.split(":")[1]));
			System.out.println("Dir="+outDir+dirTestType);
			try {
				this.outFile = new PrintWriter(new BufferedWriter(new FileWriter(
						outDir + InetAddress.getLocalHost().getHostName()
						+ protocolName + "RW.log", true)));
				this.allLat = new PrintWriter(new BufferedWriter(
						new FileWriter(outDir+"/Adaptation/" +dirTestType + "/" + InetAddress.getLocalHost().getHostName()+ protocolName +  "AllLat.csv", true)));
				this.writeLat = new PrintWriter(new BufferedWriter(
						new FileWriter(outDir+"/Adaptation/" +dirTestType + "/" + InetAddress.getLocalHost().getHostName()+ protocolName +  "WriteLat.csv", true)));
				this.readLat = new PrintWriter(new BufferedWriter(
						new FileWriter(outDir+"/Adaptation/" +dirTestType + "/" + InetAddress.getLocalHost().getHostName()+ protocolName +  "ReadLat.csv", true)));
				this.allInfoZanCT = new PrintWriter(new BufferedWriter(
						new FileWriter(outDir+"/Adaptation/" + dirTestType + "/" + InetAddress.getLocalHost().getHostName()+ protocolName +  "AllInfoLat.csv", true)));
				this.outThroughput = new PrintWriter(new BufferedWriter(
						new FileWriter(outDir+"/Adaptation/" + dirTestType + "/" + InetAddress.getLocalHost().getHostName()+ protocolName +  "Throughput.csv", true)));
				this.outFileToWork = new PrintWriter(new BufferedWriter(
						new FileWriter(outDir +InetAddress.getLocalHost().getHostName()+ protocolName + ".csv", true)));
				//this.outFileAllLat = new PrintWriter(new BufferedWriter(
				//new FileWriter(outDir+ dirTestType+"/" + InetAddress.getLocalHost().getHostName()+ protocolName +  "All.csv", true)));
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void addInforForp(String infop) {
		this.infoForp.add(infop);
	}

	public int getLastNumReqDeliveredBefore() {
		return lastNumReqDeliveredBefore.get();
	}

	public void setLastNumReqDeliveredBefore(int lastNumReqDeliveredBefore) {
		this.lastNumReqDeliveredBefore.set(lastNumReqDeliveredBefore);
	}

	public List<Long> getLatencies() {
		return latencies;
	}

	public void setLatencies(List<Long> latencies) {
		this.latencies = latencies;
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

	public void setNumRequest(AtomicInteger numRequest) {
		this.numRequest = numRequest;
	}

	public int getnumReqDelivered() {
		return numReqDelivered.get();
	}

	public void addToNumReqDelivered(int value) {
		numReqDelivered.addAndGet(value);
	}

	public AtomicInteger getcountDummyCall() {
		return countDummyCall;
	}

	public void setnumReqDelivered(AtomicInteger numReqDelivered) {
		this.numReqDelivered = numReqDelivered;
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

	public void InCountDummyCall() {
		countDummyCall.incrementAndGet();
	}

	public void addLatency(long latency) {
		latencies.add(latency);
	}
	public void addReadLatency(long latency) {
		readLatencies.add(latency);
	}

	public void addRecievedTime(long timeDelay) {
		recievedRequestTime.add(timeDelay);
	}

	public void setLastRecievedTime(long lastTime){
		lastRecievedTime = lastTime;
	}

	public long getLastRecievedTime(){
		return lastRecievedTime;
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

	public boolean isWarmup() {
		return is_warmup;
	}

	public void setWarmup(boolean is_warmup) {
		this.is_warmup = is_warmup;
	}

	// This method uses for count ACK Per broadcast for each W% case
	public void addNumAckPerBroadcast(int numACK) {
		numAckPerBroadcast.add(numACK);
	}

	// This method uses for store result (matrix) for ZabCT Adaptation
	public void addResult(String result) {
		results.add(result);
	}

	// This method find last current latency index
	public int getLatencyIndex() {
		return latencies.size()-1;
	}

	public long getStartTimeRatio() {
		return startTimeRatio;
	}
	public void setStartTimeRatio(long start) {
		this.startTimeRatio=start;
	}
	public AtomicInteger getCountTotalMessagesFollowers() {
		return countTotalMessagesFollowers;
	}

	public void setCountTotalMessagesFollowers(int countTotalMessagesFollowers) {
		this.countTotalMessagesFollowers.set(countTotalMessagesFollowers);
	}
	public void addCountTotalMessagesFollowers(int countTotalMessages) {
		this.countTotalMessagesFollowers.addAndGet(countTotalMessages);
	}

	public TreeMap<Double,Double> findpW(int N, int currentServers){
		TreeMap<Double, Double> pW = new TreeMap<Double, Double>();
		SortedSet<Double> ps = new TreeSet<Double>();
		int c=N-currentServers;
		N=N-c;
		int n = N-1;
		List<RangeHead> rangeHeads = findRageHead(N, c, n);
		ps= findpForRangeHead(rangeHeads, c, n);
		for(double p:ps){
			pW.put(p, findWForRangep(p, N, c));
		}
		return pW;

	}

	public List<RangeHead> findRageHead(int N, int c, int n){
		List<RangeHead> rHeads = new ArrayList<RangeHead>();
		RangeHead rh;
		for (int i = 1; i < n; i++) {
			for (int j = i; j <n; j++) {
				rh = new RangeHead(i,j);
				rHeads.add(rh);
			}
		}
		return rHeads;
	}

	public static SortedSet<Double> findpForRangeHead(List<RangeHead> rangeHeads, int c, int n){
		DecimalFormat roundValue = new DecimalFormat("#.000");
		double term1=1,p=0;
		SortedSet<Double> ps = new TreeSet<Double>();
		for (RangeHead rangeHead : rangeHeads) {
			term1=1;
			for (int j = 0; j <= rangeHead.getB(); j++) {
				term1*= ((double)((n-rangeHead.getAlpha())-j)) / ((double)(rangeHead.getAlpha()+j));				
			}
			term1=1+Math.pow(term1, ((double)1/((double) rangeHead.getB()+1)));
			p = ((double)1) / ((double) term1);
			p= Double.parseDouble(roundValue.format(p));
			ps.add(p);
		}
		//Get more p values
		double newp=0.0;
		ArrayList<Double> copyps = new ArrayList<Double>(ps);
		if(n==2 && c==0){
			ps.add(0.125);
			ps.add(0.25);
			ps.add(0.75);
			ps.add(0.800);
		}
		double fristp=((double)copyps.get(0))/2;
		ps.add(fristp);
		for(int i=0;i<copyps.size()-1;i++){
			newp=((double) copyps.get(i)+copyps.get(i+1))/2;
			ps.add((Double.parseDouble(roundValue.format(newp))));
		}
		if (n==6){
			ps.add(0.148);
			ps.add(0.130);
		}

		return ps;
	}

	public double findExtrap(int c, int n, double c1, double c2, double lastP){
		boolean found = false;
		DecimalFormat roundValue = new DecimalFormat("#.000");
		double p=0.0, w=0.0;
		p=lastP;
		while(p<=0.995){
			p= p+ 0.005;
			p=Double.parseDouble(roundValue.format(p));
			w = findWForRangep(p, n+1, c);
			if (p>=c2 || w>=c1){	
				extrapW.put(p, w);
			}
			else{
				extrapW.put(p, w);
				found = true;
				break;
			}
		}
		if (found)
			return p;
		else
			return 0.0;
	}
	// This method used by old computing p algorithm
	public double findp(int c, int n, double c1, double c2, double P1, double P2){
		final double ABS = 0.005;
		DecimalFormat roundValue = new DecimalFormat("#.000");
		double newp=P1-ABS, newW=0.0, p=0.000;
		while(newp>=P2){
			newp=Double.parseDouble(roundValue.format(newp));
			newW = findWForRangep(newp, n+1, c);
			log.info("W(P)="+newW+" P="+newp);

			if (newp<=c2 && newW<=c1){	
				p=newp;
				break;
			}
			newp= newp- 0.005;
			//System.out.println("check newp="+newp);
			//log.info("check newp="+newp);
		}
		return p;
	}

	public double findWForRangep(double p, int N, int c){
		DecimalFormat roundValue = new DecimalFormat("#.000");
		TreeMap<Double, Double> pW = new TreeMap<Double, Double>();
		double q00,q01,q02,q03,q11,q12,q13,q22,q23,q33, W=0.0;
		switch(N){
		case 3:
		{
			switch(c){
			case 0:

				q00=Math.pow((1-p), 2);
				W = ( (double) q00/(double) (1-q00) );
				W= Double.parseDouble(roundValue.format(W));
				break;
			case 1:
				q00=Math.pow((1-p), 1);
				W = ( (double) q00/(double) (1-q00) );
				W= Double.parseDouble(roundValue.format(W));
				break;
			}
		}
		break;
		case 5:
		{
			switch(c){
			case 0:
				q00=Math.pow((1-p), 4);
				q01=(4*p)* (Math.pow((1-p), 3));
				q11=Math.pow((1-p), 3);
				W = (((double) q00)/(double) (1-q00))+  (((double) q01)/((double) (1-q00) * (1-q11)));
				W= Double.parseDouble(roundValue.format(W));
				break;
			case 1:

				q00=Math.pow((1-p), 3);
				q01=(3*p)* (Math.pow((1-p), 2));
				q11=Math.pow((1-p), 2);
				W = (((double) q00)/(double) (1-q00))+  (((double) q01)/((double) (1-q00) * (1-q11)));
				W= Double.parseDouble(roundValue.format(W));
				break;
			case 2:
				q00=Math.pow((1-p), 2);
				q01=(2*p)* (1-p);
				q11=1-p;
				W = (((double) q00)/(double) (1-q00))+  (((double) q01)/((double) (1-q00) * (1-q11)));
				W= Double.parseDouble(roundValue.format(W));

				break;

			}
		}
		break;
		case 7:
		{
			switch(c){
			case 0:
				q00=Math.pow((1-p), 6);
				q01=(6*p)* (Math.pow((1-p), 5));
				q02=(15* (Math.pow(p, 2))) * (Math.pow((1-p), 4));
				q11=Math.pow((1-p), 5);
				q12=(5*p) * (Math.pow((1-p), 4));
				q22=Math.pow((1-p), 4);
				W = (((double) q00)/(double) (1-q00)) + (((double) q01)/((double) (1-q00) * (1-q11)))
						+ (((double) q01*q12)/((double) (1-q00) * (1-q11) * (1-q22)))
						+ (((double) q02)/(double) (1-q00) * (1-q22)) ;
				W= Double.parseDouble(roundValue.format(W));
				break;
			case 1:
				q00=Math.pow((1-p), 5);
				q01=(5*p) * (Math.pow((1-p), 4));
				q02=(10* (Math.pow(p, 2))) * (Math.pow((1-p), 3));
				q11=Math.pow((1-p), 4);
				q12=(4*p) * (Math.pow((1-p), 3));
				q22=Math.pow((1-p), 3);
				W = (((double) q00)/(double) (1-q00)) + (((double) q01)/((double) (1-q00) * (1-q11))) 
						+ (((double) q01*q12)/((double) (1-q00) * (1-q11) * (1-q22))) 
						+ (((double) q02)/((double) (1-q00) * (1-q22))) ;
				W= Double.parseDouble(roundValue.format(W));
				break;
			case 2:
				q00=Math.pow((1-p), 4);
				q01=(4*p) * (Math.pow((1-p), 3));
				q02=(6* (Math.pow(p, 2))) * (Math.pow((1-p), 2));
				q11=Math.pow((1-p), 3);
				q12=(3*p) * (Math.pow((1-p), 2));
				q22=Math.pow((1-p), 2);
				W = (((double) q00)/(double) (1-q00)) + (((double) q01)/((double) (1-q00) * (1-q11))) 
						+ (((double) q01*q12)/((double) (1-q00) * (1-q11) * (1-q22))) 
						+ (((double) q02)/((double) (1-q00) * (1-q22))) ;
				W= Double.parseDouble(roundValue.format(W));
				break;
			case 3:
				q00=Math.pow((1-p), 3);
				q01=(3*p) * (Math.pow((1-p), 2));
				q02=(3* (Math.pow(p, 2))) * (1-p);
				q11=Math.pow((1-p), 2);
				q12=(2*p) * (1-p);
				q22=(1-p);
				W = (((double) q00)/(double) (1-q00)) + (((double) q01)/(double) (1-q00) * (1-q11)) 
						+ (((double) q01*q12)/(double) (1-q00) * (1-q11) * (1-q22)) 
						+ (((double) q02)/(double) (1-q00) * (1-q22)) ;
				W= Double.parseDouble(roundValue.format(W));
				break;
			}
		}
		break;
		case 9:
		{
			switch(c){
			case 0:
				q00=Math.pow((1-p), 8);
				q01=(8*p) * (Math.pow((1-p), 7));
				q02=(28* (Math.pow(p, 2))) * (Math.pow((1-p), 6));
				q03=(56* (Math.pow(p, 3))) * (Math.pow((1-p), 5));
				q11=Math.pow((1-p), 7);
				q12=(7*p) * (Math.pow((1-p), 6));
				q13=(21* (Math.pow(p, 2))) * (Math.pow((1-p), 5));
				q22=Math.pow((1-p), 6);
				q23=(6*p) * (Math.pow((1-p), 5));
				q33=Math.pow((1-p), 5);
				W = (((double) q00)/(double) (1-q00)) + (((double) q01)/(double) (1-q00) * (1-q11)) 
						+ (((double) q01*q12)/((double) ((1-q00) * (1-q11) * (1-q22))))
						+ (((double) q01*q12*q13)/((double) ((1-q00) * (1-q11) * (1-q22) * (1-q33))))
						+ (((double) q02)/(double) (1-q00) * (1-q22)) + (((double) q02*q23)/((double) ((1-q00) * (1-q22) * (1-q33))))
						+ (((double) q03)/((double) ((1-q00) * (1-q33))));
				W= Double.parseDouble(roundValue.format(W));
				break;
			case 1:
				q00=Math.pow((1-p), 7);
				q01=(7*p) * (Math.pow((1-p), 6));
				q02=(21* (Math.pow(p, 2))) * (Math.pow((1-p), 5));
				q03=(35* (Math.pow(p, 3))) * (Math.pow((1-p), 4));
				q11=Math.pow((1-p), 6);
				q12=(6*p) * (Math.pow((1-p), 5));
				q13=(15* (Math.pow(p, 2))) * (Math.pow((1-p), 4));
				q22=Math.pow((1-p), 5);
				q23=(5*p) * (Math.pow((1-p), 4));
				q33=Math.pow((1-p), 4);
				W = (((double) q00)/(double) (1-q00)) + (((double) q01)/(double) (1-q00) * (1-q11)) 
						+ (((double) q01*q12)/((double) ((1-q00) * (1-q11) * (1-q22))))
						+ (((double) q01*q12*q13)/((double) ((1-q00) * (1-q11) * (1-q22) * (1-q33))))
						+ (((double) q02)/(double) (1-q00) * (1-q22)) + (((double) q02*q23)/((double) ((1-q00) * (1-q22) * (1-q33))))
						+ (((double) q03)/((double) ((1-q00) * (1-q33))));
				W= Double.parseDouble(roundValue.format(W));
				break;
			case 2:
				q00=Math.pow((1-p), 6);
				q01=(6*p) * (Math.pow((1-p), 5));
				q02=(15* (Math.pow(p, 2))) * (Math.pow((1-p), 4));
				q03=(4* (Math.pow(p, 3))) * (Math.pow((1-p), 3));
				q11=Math.pow((1-p), 5);
				q12=(5*p) * (Math.pow((1-p), 4));
				q13=(10* (Math.pow(p, 2))) * (Math.pow((1-p), 3));
				q22=Math.pow((1-p), 4);
				q23=(4*p) * (Math.pow((1-p), 3));
				q33=Math.pow((1-p), 3);
				W = (((double) q00)/(double) (1-q00)) + (((double) q01)/(double) (1-q00) * (1-q11)) 
						+ (((double) q01*q12)/((double) ((1-q00) * (1-q11) * (1-q22))))
						+ (((double) q01*q12*q13)/((double) ((1-q00) * (1-q11) * (1-q22) * (1-q33))))
						+ (((double) q02)/(double) (1-q00) * (1-q22)) + (((double) q02*q23)/((double) ((1-q00) * (1-q22) * (1-q33))))
						+ (((double) q03)/((double) ((1-q00) * (1-q33))));
				W= Double.parseDouble(roundValue.format(W));
				break;
			case 3:
				q00=Math.pow((1-p), 5);
				q01=(5*p) * (Math.pow((1-p), 4));
				q02=(10* (Math.pow(p, 2))) * (Math.pow((1-p), 3));
				q03=(10* (Math.pow(p, 3))) * (Math.pow((1-p), 2));
				q11=Math.pow((1-p), 4);
				q12=(4*p) * (Math.pow((1-p), 3));
				q13=(6* (Math.pow(p, 2))) * (Math.pow((1-p), 2));
				q22=Math.pow((1-p), 3);
				q23=(3*p) * (Math.pow((1-p), 2));
				q33=Math.pow((1-p), 2);
				W = (((double) q00)/(double) (1-q00)) + (((double) q01)/(double) (1-q00) * (1-q11)) 
						+ (((double) q01*q12)/((double) ((1-q00) * (1-q11) * (1-q22))))
						+ (((double) q01*q12*q13)/((double) ((1-q00) * (1-q11) * (1-q22) * (1-q33))))
						+ (((double) q02)/(double) (1-q00) * (1-q22)) + (((double) q02*q23)/((double) ((1-q00) * (1-q22) * (1-q33))))
						+ (((double) q03)/((double) ((1-q00) * (1-q33))));
				W= Double.parseDouble(roundValue.format(W));
				break;
			case 4:
				q00=Math.pow((1-p), 4);
				q01=(4*p) * (Math.pow((1-p), 3));
				q02=(6* (Math.pow(p, 2))) * (Math.pow((1-p), 2));
				q03=(4* (Math.pow(p, 3))) * (1-p);
				q11=Math.pow((1-p), 3);
				q12=(3*p) * (Math.pow((1-p), 2));
				q13=(3* (Math.pow(p, 2))) * (1-p);
				q22=Math.pow((1-p), 2);
				q23=(2*p) * (1-p);
				q33=1-p;
				W = (((double) q00)/(double) (1-q00)) + (((double) q01)/(double) (1-q00) * (1-q11)) 
						+ (((double) q01*q12)/((double) ((1-q00) * (1-q11) * (1-q22))))
						+ (((double) q01*q12*q13)/((double) ((1-q00) * (1-q11) * (1-q22) * (1-q33))))
						+ (((double) q02)/(double) (1-q00) * (1-q22)) + (((double) q02*q23)/((double) ((1-q00) * (1-q22) * (1-q33))))
						+ (((double) q03)/((double) ((1-q00) * (1-q33))));
				W= Double.parseDouble(roundValue.format(W));
				break;
			}
		}
		break;
		}
		return W;
	}


	public TreeMap<Double, Double> getExtrapW() {
		return extrapW;
	}

	public void setExtrapW(TreeMap<Double, Double> extrapW) {
		this.extrapW = extrapW;
	}

	public void printProtocolStats(int deliveredRequest, int cliuterSize, int perRW, long waitSentTime, boolean is_leader) {
		//System.out.println("info: " + deliveredRequest+" cliuterSize="+cliuterSize+" perRW"+perRW+" waitSentTime="+waitSentTime);
		double avgAllD = 0, avgRead=0, allRWAvg=0, throughputRate =0, tempSumThr=0, per50th=0, per90th=0, 
				per99th=0, per95th=0;
		String CI95=null;
		outFile.println("Info: "+protocolName + "/" + numberOfClients + "/"
				+ numberOfSenderInEachClient+"/Cluster Size=:"+cliuterSize+"/R:W="+perRW+":"+(100-perRW)+"/waitSentTime=:"+waitSentTime);
		outFile.println("Number of Request Deliever (Counter): " + numReqDelivered.get());
		outFile.println("Throughput: "
				+ (numReqDelivered.get() / (TimeUnit.MILLISECONDS
						.toSeconds((endThroughputTime - startThroughputTime))))+" Req/sec");

		if(latencies.size()!=0){
			avgAllD = average(latencies);
			avgAllD = (avgAllD) / 1000000.0;
			outFile.println("Write Latencies Size: " + latencies.size());
			outFile.println("Write Latency: [Min= " + ((double) (min(latencies))/1000000.0) + " Avg= " +
					avgAllD  + " Max= " +((double) (max(latencies))/1000000.0)+"]");
			CI95= computeCI(latencies, avgAllD);
			outFile.println("Write Latency 95% confidence interval: "+CI95);
			per50th = percentile50th(latencies);
			per90th = percentile90th(latencies);
			per95th = percentile95th(latencies);
			per99th = percentile99th(latencies);
			outFile.println("Write Latency Median: "+per50th);
			outFile.println("Write Latency 90th percentile: "+per90th);
			outFile.println("Write Latency 95th percentile: "+per95th);
			outFile.println("Write Latency 99th percentile: "+per99th);
			for (long lat: latencies){
				//outFileToWork.println((double) (((double) lat)/1000000));
				writeLat.println((double) (((double) lat)/1000000));
			}
		}

		if(readLatencies.size()!=0){
			avgRead = average(readLatencies);
			avgRead = (avgRead) / 1000000.0;
			outFile.println("Read Latencies Size: " + readLatencies.size());
			//outFile.println("Read Latency: [Min= " + ((double) (min(readLatencies))/1000000.0) + " Avg= " +
			//	avgRead  + " Max= " +((double) (max(readLatencies))/1000000.0)+"]");
			CI95= computeCI(readLatencies,avgRead);
			outFile.println("Read Latency 95% confidence interval: "+CI95);
			per50th = percentile50th(readLatencies);
			per90th = percentile90th(readLatencies);
			per95th = percentile95th(readLatencies);
			per99th = percentile99th(readLatencies);
			outFile.println("Read Latency Median: "+per50th);
			outFile.println("Read Latency 90th percentile: "+per90th);
			outFile.println("Read Latency 95th percentile: "+per95th);
			outFile.println("Read Latency 99th percentile: "+per99th);
			for (long lat: readLatencies){
				//outFileToWork.println((double) (((double) lat)/1000000));
				readLat.println((double) (((double) lat)/1000000));
			}
		}

		List<Long> RWLatencies = new ArrayList<Long>();
		RWLatencies.addAll(latencies);
		RWLatencies.addAll(readLatencies);
		outFile.println("Read/Write Latencies Size: " + RWLatencies.size());				
		allRWAvg = average(RWLatencies);
		allRWAvg = (allRWAvg) / 1000000.0;
		outFile.println("All Latency: [Min= " + ((double) (min(RWLatencies))/1000000.0) + " Avg= " +
				allRWAvg  + " Max= " +((double) (max(RWLatencies))/1000000.0)+"]");		
		CI95= computeCI(RWLatencies,allRWAvg);
		outFile.println("All Latencies 95% confidence interval: "+CI95);
		per50th = percentile50th(RWLatencies);
		per90th = percentile90th(RWLatencies);
		per95th = percentile95th(RWLatencies);
		per99th = percentile99th(RWLatencies);
		outFile.println("All Latencies Median: "+per50th);
		outFile.println("All Latencies 90th percentile: "+per90th);
		outFile.println("All Latencies 95th percentile: "+per95th);
		outFile.println("All Latencies 99th percentile: "+per99th);
		for (long lat: RWLatencies){
			//outFileToWork.println((double) (((double) lat)/1000000));
			allLat.println((double) (((double) lat)/1000000));
		}

		if(!is_leader){
			outFile.println("Number of ACK Broadcast per W%: "+countACKPerBroadcast.get());
		}
		outFile.println("Number of ACK Recieved per W%: "+countAckMessage.get());

		allInfoZanCT.println("NewResult");
		for(String info:results)
			allInfoZanCT.println((info));

		for(double thr:throughputs){
			outThroughput.println(thr);
		}
		//outFile.println();
		outFile.println("Test Generated at "
				+ new Date()
				+ " /Lasted for = "
				+ TimeUnit.MILLISECONDS
				.toSeconds((endThroughputTime - startThroughputTime))+" Sec");
		outFile.println();		
		outFile.close();
		outFileToWork.println();
		outFileToWork.close();
		outThroughput.close();
		//outFileAllLat.close();
		writeLat.close();
		allLat.close();
		readLat.close();
		allInfoZanCT.close();
		System.out.println("Finished");
	}

	public String averageTwo(int s, int e, List<Long> allLatency){
		String r=null;
		double avg=0.0;
		double sum=0;
		List<Double> latency = convertToDouble(allLatency.subList(s, (e+1)));
		//For Frist half
		int size = latency.size()/2;
		for (int i=0;i<size;i++) {
			sum += latency.get(i);
		}
		avg = (double) sum / size;
		r=String.valueOf(avg)+"-";;
		//For Second half
		sum=0;
		avg=0.0;
		for (int i=size;i<latency.size();i++) {
			sum += latency.get(i);
		}
		avg = (double) sum / size;
		r+=String.valueOf(avg);;

		return r;

	}
	public List<Double> convertToDouble(List<Long> allLatency){
		List<Long> latenciesInLong = new ArrayList<Long>(allLatency);
		List<Double> latenciesInDouble = new ArrayList<Double>();
		for (int i = 0; i < latenciesInLong.size(); i++) {
			latenciesInDouble.add((double) (((double) latenciesInLong.get(i))/1000000));
		}
		return latenciesInDouble;
	}


	public String p90Two(int s, int e, List<Long> allLatency){
		String r = null;
		List<Double> latencies = convertToDouble(allLatency.subList(s, (e+1)));
		int midPoint = (latencies.size()/2);
		List<Double> latency1= latencies.subList(0, midPoint);
		List<Double> latency2= latencies.subList(midPoint, (latencies.size()));
		Collections.sort(latency1);
		Collections.sort(latency2);
		int p90_indexList_1=(int)(latency1.size()*0.90)-1;
		int p90_indexList_2=(int)(latency2.size()*0.90)-1;
		r = String.valueOf(latency1.get(p90_indexList_1))+"-";;
		r+= String.valueOf(latency2.get(p90_indexList_2));
		return r;
	}

	public String p95Two(int s, int e, List<Long> allLatency){

		String r = null;
		List<Double> latencies = convertToDouble(allLatency.subList(s, (e+1)));
		int midPoint = (latencies.size()/2);
		List<Double> latency1= latencies.subList(0, midPoint);
		List<Double> latency2= latencies.subList(midPoint, (latencies.size()));
		Collections.sort(latency1);
		Collections.sort(latency2);
		int p90_indexList_1=(int)(latency1.size()*0.95)-1;
		int p90_indexList_2=(int)(latency2.size()*0.95)-1;
		r = String.valueOf(latency1.get(p90_indexList_1))+"-";;
		r+= String.valueOf(latency2.get(p90_indexList_2));
		return r;

	}
	public String p99Two(int s, int e, List<Long> allLatency){

		String r = null;
		List<Double> latencies = convertToDouble(allLatency.subList(s, (e+1)));
		int midPoint = (latencies.size()/2);
		List<Double> latency1= latencies.subList(0, midPoint);
		List<Double> latency2= latencies.subList(midPoint, (latencies.size()));
		Collections.sort(latency1);
		Collections.sort(latency2);
		int p90_indexList_1=(int)(latency1.size()*0.99)-1;
		int p90_indexList_2=(int)(latency2.size()*0.99)-1;
		r = (String.valueOf(latency1.get(p90_indexList_1)))+"-";
		r+= String.valueOf(latency2.get(p90_indexList_2));
		return r;

	}
	public void findDist(List<Long> data){
		List<Long> latLong = new ArrayList<Long>(data);
		List<Double> latencies = new ArrayList<Double>();
		int x0T0D5=0, x3D5=0, x4=0, x4D5=0, x5=0, x10=0, x100=0,x300=0, x500=0, x800=0,
				x1000=0, x600=0, x650=0, x700=0, xLager1000=0, x1=0, x1D5=0,x2=0,x2D5=0,x3=0;
		for (int i = 0; i < latLong.size(); i++) {
			latencies.add((double) (((double) latLong.get(i))/1000000));
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
		outFileToWork.println("(0-0.5), " + x0T0D5);
		outFileToWork.println("(0.5-1), " + x1);
		outFileToWork.println("(1-1.5), " + x1D5);
		outFileToWork.println("(1.5-2), " + x2);
		outFileToWork.println("(2-2.5), " + x2D5);
		outFileToWork.println("(2.5-3), " + x3);
		outFileToWork.println("(3-3.5), " + x3D5);
		outFileToWork.println("(3.5-4), " + x4);
		outFileToWork.println("(4-4.5), " + x4D5);
		outFileToWork.println("(4.5-10), " + x10);
		outFileToWork.println("(10-100), " + x100);
		outFileToWork.println("(100-300), " + x300);
		outFileToWork.println("(300-500), " + x500);
		outFileToWork.println("(500-800), " + x800);
		outFileToWork.println("(800-1000), " + x1000);
		outFileToWork.println("(>1000), " + xLager1000);    

	}

	public double average(List<Long> data) {
		System.out.println("Test Size"+data.size());
		double sum=0;
		double avg = 0;
		for (Long lat : data) {
			sum += lat;
		}
		avg = (double) sum / data.size();
		return avg;

	}

	public void printLatencyToFile(List<Long> data) {
		List<Long> latenciesInLong = new ArrayList<Long>(data);
		List<Double> latenciesInDouble = new ArrayList<Double>();
		for (int i = 0; i < latenciesInLong.size(); i++) {
			latenciesInDouble.add((double) (((double) latenciesInLong.get(i))/1000000));
		}
		for (int i = 0; i < latenciesInDouble.size(); i++) {
			outFileAllLat.println(latenciesInDouble.get(i));
		}
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

	public String computeCI(List<Long> lats, double mean){
		double marginError =0.0, ci95 = 1.96, CILower=0.0, CIUpper=0.0;
		double stdev = stdev(lats, mean);
		marginError = ci95 * (stdev/(Math.sqrt(lats.size())));
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

	public double percentile95th(List<Long> latInLong){
		List<Long> latenciesInLong = new ArrayList<Long>(latInLong);
		List<Double> latenciesInDouble = new ArrayList<Double>();
		for (int i = 0; i < latenciesInLong.size(); i++) {
			latenciesInDouble.add((double) (((double) latenciesInLong.get(i))/1000000));
		}
		Collections.sort(latenciesInDouble);
		return latenciesInDouble.get((int)(latenciesInDouble.size()*0.95)-1);

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
		List<Long> latLong = new ArrayList<Long>(latInLong);
		List<Double> latenciesInDouble = new ArrayList<Double>();
		List<Double> distFromMean = new ArrayList<Double>();
		for (int i = 0; i < latLong.size(); i++) {
			latenciesInDouble.add((double) (((double) latLong.get(i))/1000000));
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

	public long max(List<Long> data){
		long max = Long.MIN_VALUE;
		for (long lat : data) {
			if (lat > max) {
				max = lat;
			}
		}
		return max;
	}

	public long min(List<Long> data){
		long min = Long.MAX_VALUE;
		for (long lat : data) {
			if (lat < min) {
				min = lat;
			}
		}
		return min;
	}

	public double averageFromTo(int start, int end, List<Long> data) {
		double sum=0;
		double avg = 0;
		for (int i=start;i<=end;i++) {
			sum += data.get(i);
		}
		avg = (double) sum / (end-start);
		return avg;

	}

}