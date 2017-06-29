package org.jgroups.protocols.jzookeeper.zabNRWEDCCAsync;

import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanServer;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.MethodLookup;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.protocols.UNICAST;
import org.jgroups.protocols.UNICAST2;
import org.jgroups.protocols.relay.RELAY2;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Buffer;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

public class ZabInfinispan extends ReceiverAdapter {
	private JChannel               channel;
	private Address                local_addr;
	static final String            groupname="Cluster";
	private String propsFile = "conf/Zab.xml";
	private static String ProtocotName = "";
	protected final List<Address>  members=new ArrayList<Address>();
	protected final List<Address>  site_masters=new ArrayList<Address>();
	private List<String> boxMembers  = new ArrayList<String>();
	private List<Address> box = new ArrayList<Address>();
	private int clusterSize = 3;
	private AtomicLong localSequence = new AtomicLong();
	private String outputDir;
	private View view;
	private String initiator;
	private String channelName;
	private static PrintWriter outFile;
	private AtomicInteger numClientsFinished = new AtomicInteger();
	private boolean warmUp = true;
	private static Random rand = new Random();
	private static int min = 0, max = 0;
	private Timer checkerRatioUpdated = new Timer();
	private int threadFinishedCount=0;
	private boolean isInitiator=false;
	private AtomicInteger num_msgs_sent = new AtomicInteger();
	private int threshold=0;
	private int finishedCount=0;
	private int countClientFinished = 0;
	private AtomicLong local = new AtomicLong(0);
	private int index = 0;
	private AtomicInteger countFinishedInvoker = new AtomicInteger(0);
	private static int LOAD = 200000;



	// ============ configurable properties ==================
	private boolean sync=true, oob=false, anycastRequests=true;
	private int num_threads=25, numOfClients=10, timeout=5000, load = 1;
	private int num_msgs=10000, msg_size=1000, num_msgsPerThreads = 10000;
	private int numsOfWarmUp = 0;
	private int anycast_count=1;
	private boolean use_anycast_addrs = true;
	private boolean random_destinations = false;
	private boolean include_local_address = true; // Include local address in anycast count
	private double read_percentage=0; // 80% reads, 20% writes
	private long waitCC=0;// wait for client before starting
	private long waitII =0;//wait for invoker before starting
	private long waitSS =0;//wait before sending consecutive request
	// =======================================================

	private static final Method[] METHODS=new Method[15];

	private static final short START                 =  0;
	private static final short SET_OOB               =  1;
	private static final short SET_SYNC              =  2;
	private static final short SET_NUM_MSGS          =  3;
	private static final short SET_NUM_THREADS       =  4;
	private static final short SET_MSG_SIZE          =  5;
	private static final short SET_ANYCAST_COUNT     =  6;
	private static final short SET_USE_ANYCAST_ADDRS =  7;
	private static final short SET_READ_PERCENTAGE   =  8;
	private static final short GET                   =  9;
	private static final short PUT                   = 10;
	private static final short GET_CONFIG            = 11;
	private static final short STARTWARM             =  12;
	private static final short FINISHED              =  13;
	private static final short FINISHEDWARM          =  14;



	private final AtomicInteger COUNTER=new AtomicInteger(1);
	private final AtomicInteger receivedCounter=new AtomicInteger(1);
	private ConcurrentMap<Long, MessageOrderInfo> keysForRead = new ConcurrentHashMap<Long, MessageOrderInfo>();
	private boolean testStarted = false;



	private byte[] GET_RSP=new byte[msg_size];

	static NumberFormat f;


	public void init(List<String> members, String protocolName, String props,
			int totalNum_msgs, int totalPerThreads, int num_threads,
			int msg_size, String outputDir, int numOfClients, int load,
			int numsOfWarmUp, int timeout, boolean sync, String 
			channelName, String initiator,int clusterSize, int rf, double read,
			long waitCC, long waitII, long waitSS, int threshold) throws Throwable {
		this.ProtocotName = protocolName;
		this.propsFile = props;
		this.num_msgs = totalNum_msgs;
		this.num_msgsPerThreads = totalPerThreads;
		this.num_threads = num_threads;
		this.msg_size = msg_size;
		this.outputDir = outputDir;
		this.numOfClients = numOfClients;
		this.load = load;
		this.numsOfWarmUp = numsOfWarmUp;
		this.timeout = 0;
		this.sync = sync;
		this.channelName = channelName;
		this.initiator = initiator;
		this.anycast_count = rf;
		this.clusterSize = clusterSize;
		this.read_percentage = read;
		this.waitCC= waitCC;
		this.waitII = waitII;
		this.waitSS = waitSS;
		this.min = (int) waitSS - ((int) (waitSS * 0.25));
		this.max = (int) waitSS + ((int) (waitSS * 0.25));
		this.threshold = threshold;
		channel=new JChannel(propsFile);
		System.out.println("this.ProtocotName := " + this.ProtocotName+ " propsFile :="+propsFile+
				"this.num_threads "+this.num_threads + " outdir := "+outputDir);
		channel.setReceiver(this);
		channel.connect(channelName);
		local_addr=channel.getAddress();
		this.boxMembers = members;
		this.isInitiator = (channel.getAddress().toString().contains(initiator))? true:false;
		try {
			MBeanServer server=Util.getMBeanServer();
			JmxConfigurator.registerChannel(channel, server, "jgroups", channel.getClusterName(), true);
		}
		catch(Throwable ex) {
			System.err.println("registering the channel in JMX failed: " + ex);
		}

		if (isInitiator) {
			System.out.println("I am initiator");
			this.outFile = new PrintWriter(new BufferedWriter(new FileWriter(
					outputDir + protocolName + "Test.log", true)));
			startBenchmark();
		}
		else{
			MessageId messageId = null;
			MessageOrderInfo messageInfo = null;
			for (int i = 1; i < 1001; i++) {
				messageId = new MessageId(local_addr, local.getAndIncrement());
				messageInfo = new MessageOrderInfo(messageId);
				messageInfo.setOrdering(i);
				keysForRead.put((long)i, messageInfo);					
			}
		}

	}

	void stop() {
		Util.close(channel);
	}

	public void viewAccepted(View new_view) {
		List<Address> addresses = new ArrayList<Address>(new_view.getMembers());
		synchronized (members) {
			members.clear();
			members.addAll(addresses);
			removeBoxMembers(members);
		}
		System.out.println("** Members: " + members);


		System.out.println("** view: " + new_view);
		view = new_view;
		List<Address> mbrs = new_view.getMembers();

		if (mbrs.size() == clusterSize+2) {
			for (int i=2;i<mbrs.size();i++){
				box.add(mbrs.get(i));
			}
		}
		if (mbrs.size() > (clusterSize+2) && box.isEmpty()) {
			for (int i = 2; i < mbrs.size(); i++) {
				box.add(mbrs.get(i));
				if(i>=(clusterSize+2))
					break;
			}
		}
		local_addr=channel.getAddress();
	}


	private void removeBoxMembers(Collection<Address> addresses) {
		if (boxMembers == null)
			return;

		Iterator<Address> i = addresses.iterator();
		while (i.hasNext()) {
			Address address = i.next();
			for (String boxName : boxMembers) {
				if (address.toString().contains(boxName)) {
					System.out.println("Remove | " + address);
					i.remove();
				}
			}
		}
	}

	/** Kicks off the benchmark on all cluster nodes */
	void startBenchmark() throws Throwable {
		doWarmup();
		Thread.sleep(50);
		sendStartNotify();
	}


	private void doWarmup() {
		Address destination = null;
		MessageId messageId =null;
		MessageOrderInfo messageOrderInfo =null;
		ZabHeader hdrReq = null;
		Message message =null;
		for (int i = 0; i < 1000; i++) {
			messageId = new MessageId(local_addr, local.getAndIncrement());
			messageOrderInfo = new MessageOrderInfo(messageId);
			hdrReq = new ZabHeader(ZabHeader.REQUESTW, messageOrderInfo);
			++index;
			if (index > (clusterSize-1))
				index = 0;
			destination = box.get(index);
			message = new Message(destination).putHeader((short) 78, hdrReq);
			message.setSrc(local_addr);
			message.setFlag(Message.Flag.DONT_BUNDLE);
			message.setBuffer(new byte[msg_size]);
			try {
				channel.send(message);
				Thread.sleep(50);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void sendStartNotify(){
		ZabHeader hdrReq = new ZabHeader(ZabHeader.STARTWORKLOAD);
		Message startedMessage = new Message().putHeader((short) 78, hdrReq);
		startedMessage.setFlag(Message.Flag.DONT_BUNDLE);
		startedMessage.setObject(read_percentage+":"+num_threads);
		for (Address address : box) {
			Message cpy = startedMessage.copy();
			cpy.setDest(address);
			cpy.setSrc(local_addr);
			try {
				channel.send(cpy);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void receive(Message msg) {
		//System.out.println("keysForRead Size="+keysForRead.size());
		synchronized (this) {
			ZabHeader head = (ZabHeader) msg.getHeader((short) 78);
			if(head.getType()==ZabHeader.RESPONSEW){
				keysForRead.put(head.getMessageOrderInfo().getOrdering(), head.getMessageOrderInfo());	
				//System.out.println("keysForRead Size"+keysForRead.size());
				//System.out.println("head=="+head);
				if(receivedCounter.incrementAndGet()==numsOfWarmUp){
					System.out.println("End Warm Up"+receivedCounter.get());
				}
			}
			else if(head.getType()==ZabHeader.STARTWORKLOAD && !testStarted){
				try {
					System.out.println("Start Real Test");
					startRealTest();
				} catch (Throwable e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}
	}

	public void startRealTest() throws Throwable {
		testStarted=true;
		//Thread.sleep((long)(rand.nextInt((int)waitCC) + 1));
		warmUp = false;
		System.out.println("invoking " + num_msgs + " RPCs of " + Util.printBytes(msg_size) +
				", sync=" + sync + ", oob=" + oob + ", use_anycast_addrs=" + use_anycast_addrs);
		num_msgs_sent=new AtomicInteger(0);
		final AtomicInteger checkWRatioUpdated=new AtomicInteger(0);
		List<Address> nonBoxMembers = new ArrayList<Address>(members);
		removeBoxMembers(nonBoxMembers);

		Random random = new Random();
		Invoker[] invokers=new Invoker[num_threads];
		// create sender (threads) to send writes to /_1/_2
		for(int i=0; i < invokers.length; i++){
			invokers[i]=new Invoker(nonBoxMembers, (LOAD/numOfClients), num_msgs_sent, random, read_percentage
					, warmUp, waitSS, checkWRatioUpdated, threshold, channel, this, countFinishedInvoker);
			System.out.println("Create Invoker --------------->>>> " + i);
		}

		long start=System.currentTimeMillis();
		for(Invoker invoker: invokers){
			//Thread.sleep((long)(rand.nextInt((int)waitII) + 1));
			invoker.start();
		}
		for(Invoker invoker: invokers) {
			invoker.join();
			//total_gets+=invoker.numGets();
			//total_puts+=invoker.numPuts();
		}

		long total_time=System.currentTimeMillis() - start;
		System.out.println("done (in " + total_time + " ms)");
	}


	void printView() {
		System.out.println("\n-- view: " + members + '\n');
		try {
			System.in.skip(System.in.available());
		}
		catch(Exception e) {
		}
	}


	/** Picks the next member in the view */
	private Address getReceiver() {
		try {
			List<Address> mbrs = members;
			int index=mbrs.indexOf(local_addr);
			int new_index=index + 1 % mbrs.size();
			return mbrs.get(new_index);
		}
		catch(Exception e) {
			System.err.println("UPerf.getReceiver(): " + e);
			return null;
		}
	}

	public void sendCompleteNotify(){
		ZabHeader hdrReq = new ZabHeader(ZabHeader.FINISHED);
		Message finishedMessage = new Message().putHeader((short) 78, hdrReq);
		finishedMessage.setFlag(Message.Flag.DONT_BUNDLE);
		System.out.println("Send Finish Message");

		for (Address address : box) {
			Message cpy = finishedMessage.copy();
			cpy.setDest(address);
			cpy.setSrc(local_addr);
			try {
				channel.send(cpy);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public synchronized void finished(){
		threadFinishedCount++;
		System.out.println("threadFinishedCount==------------>"+threadFinishedCount);
		if(threadFinishedCount==num_threads){
			System.out.println("Send Finish Message threadFinishedCount==------------>"+threadFinishedCount);
			sendCompleteNotify();
			System.out.println("Send Finished message");
		}
	}

	public synchronized void finishedMe() throws Exception{
		countClientFinished++;
		if(countClientFinished==numOfClients && isInitiator){
			sendCompleteNotify();
			countClientFinished=0;
		}
	}


	public synchronized void finishedAll() throws Exception{
		finishedCount++;

		if(finishedCount==num_threads){
			List<Address> nonBoxMembers = new ArrayList<Address>(members);
			removeBoxMembers(nonBoxMembers);
			RequestOptions options=new RequestOptions(ResponseMode.GET_NONE, 0, anycastRequests);
			options.setFlags(Message.Flag.OOB, Message.Flag.DONT_BUNDLE, Message.NO_FC);
			//RspList<Object> responses=disp.callRemoteMethods(nonBoxMembers, new MethodCall(FINISHED), options);
			//System.out.println("after sending rpc for WarmUp");
		}
	}
	//Sender to send write RPC, for testing ording protocol 
	private class Invoker extends Thread {
		private final List<Address>  destClients=new ArrayList<Address>();
		private final int            num_msgs_to_send;
		private final AtomicInteger  num_msgs_sent;
		private final AtomicInteger  checkWRatio;
		//private final AtomicInteger  syncSendReceived;

		private int                  num_gets=0;
		private int                  num_puts=0;
		private Random random;
		private double read_per=0;
		private boolean warmStage = false;
		private int lastMsgSent = 0;
		private int minl = 0, maxl = 0;
		private final DecimalFormat roundValue = new DecimalFormat("#.0");
		private int threshold = 10000;
		private final int thresholdFixed = 10000;
		private long waitSSl=0;
		private long sendTime = 0;
		private int thresholdSync =50;
		private int indexServer =0;
		MessageId messageId =null;
		private MessageOrderInfo messageInfoRead =null;
		private ZabHeader hdrReq =null;
		private Message sendMessage=null;
		private JChannel myChannel = null;
		private ZabInfinispan zabClient = null;
		private final AtomicInteger  countFInvokers;

		public Invoker(Collection<Address> dests, int num_msgs_to_send, AtomicInteger num_msgs_sent, 
				Random random, double read_per, boolean isWarm, long waitSSl, AtomicInteger checkWRatioUpdated
				, int threshold, JChannel myChannel, ZabInfinispan zabC, AtomicInteger countF) {
			this.num_msgs_sent=num_msgs_sent;
			this.destClients.addAll(dests);
			System.out.println(" ***********num_msgs_to_send "+num_msgs_to_send);;
			this.random = random;
			this.read_per = read_per;
			this.num_msgs_to_send= (int) ((double) num_msgs_to_send/(1.0 - this.read_per));
			System.out.println("Load is=="+this.num_msgs_to_send);
			this.warmStage = isWarm;
			this.waitSSl=waitSSl;
			System.out.println(this.waitSSl);
			this.minl = (int) this.waitSSl - ((int) (this.waitSSl * 0.25));
			this.maxl = (int) this.waitSSl + ((int) (this.waitSSl * 0.25));
			this.sendTime = minl + this.random.nextInt((maxl - minl) + 1);
			this.checkWRatio = checkWRatioUpdated;
			setName("Invoker-" + COUNTER.getAndIncrement());
			this.thresholdSync = threshold;
			this.myChannel = myChannel;
			this.zabClient = zabC;
			this.countFInvokers = countF;
			System.out.println("this.thresholdSync"+this.thresholdSync);
		}

		public int numGets() {return num_gets;}
		public int numPuts() {return num_puts;}


		public void run() {
			//System.out.println(" Inside run");
			final byte[] buf=new byte[msg_size];
			Object[] put_args={0, buf};
			Object[] get_args={0};

			while(true) {
				long i=num_msgs_sent.getAndIncrement();
				if(i >= (num_msgs_to_send)){
					System.out.println(" *********** "+this.getName() + " Finished=: " + i);
					//zabClient.finished();
					if(countFInvokers.incrementAndGet()==25){
						System.out.println("All Invokers Finished=: ");
						sendFinished();
					}
					//System.out.println(" *********** "+this.getName() + " Finished=: " + i);
					break;
				}
				try {
					Thread.sleep(this.waitSSl);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				boolean get=Util.tossWeightedCoin(read_per);
				++indexServer;
				if (indexServer > (clusterSize-1))
					indexServer = 0;
				try {
					if(get) { // sync GET
						while(true){
							long randomIndex = (long) random.nextInt(((int) keysForRead.size())-1);
							messageInfoRead = keysForRead.get(randomIndex);
							if(messageInfoRead!=null){
								break;
							}
			
						}
						hdrReq = new ZabHeader(ZabHeader.REQUESTR, messageInfoRead);
						sendMessage = new Message(box.get(indexServer)).putHeader((short) 78, hdrReq);
						sendMessage.setSrc(local_addr);
						sendMessage.setFlag(Message.Flag.DONT_BUNDLE);
						myChannel.send(sendMessage);
					}
					else {    
						messageId = new MessageId(local_addr, local.getAndIncrement());
						messageInfoRead = new MessageOrderInfo(messageId);
						hdrReq = new ZabHeader(ZabHeader.REQUESTW, messageInfoRead);
						sendMessage = new Message(box.get(indexServer)).putHeader((short) 78, hdrReq);
						sendMessage.setSrc(local_addr);
						sendMessage.setFlag(Message.Flag.DONT_BUNDLE);
						sendMessage.setBuffer(new byte[1000]);
						myChannel.send(sendMessage);
					}

				}
				catch(Throwable throwable) {
					throwable.printStackTrace();
				}
			}
		}
		
		public void sendFinished(){			
			ZabHeader hdrReq = new ZabHeader(ZabHeader.FINISHED);
			Message finishedMessage = new Message().putHeader((short) 78, hdrReq);
			finishedMessage.setFlag(Message.Flag.DONT_BUNDLE);
			System.out.println("Send Finish Message");
			for (Address address : box) {
				Message cpy = finishedMessage.copy();
				cpy.setDest(address);
				cpy.setSrc(local_addr);
				try {
					myChannel.send(cpy);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
		}

		public void changeWaitTime(long newWaitTime){
			System.out.println("^^^^^^^Call changeWaitTime waitSS="+this.waitSSl);
			this.waitSSl = newWaitTime;
			System.out.println("^^^^^^^Call changeWaitTime waitSS="+this.waitSSl);
			this.minl = (int) this.waitSSl - ((int) (this.waitSSl * 0.25));
			this.maxl = (int) this.waitSSl + ((int) (this.waitSSl * 0.25));
		}

		public void changeReadRatio(double newRatio){
			this.read_per=Double.parseDouble(roundValue.format(newRatio));
		}
	}

	public static class ConfigOptions implements Streamable {
		private boolean sync, oob;
		private int     num_threads;
		private int     num_msgs, msg_size;
		private int     anycast_count;
		private boolean use_anycast_addrs;
		private double  read_percentage;

		public ConfigOptions() {
		}

		public ConfigOptions(boolean oob, boolean sync, int num_threads, int num_msgs, int msg_size,
				int anycast_count, boolean use_anycast_addrs,
				double read_percentage) {
			this.oob=oob;
			this.sync=sync;
			this.num_threads=num_threads;
			this.num_msgs=num_msgs;
			this.msg_size=msg_size;
			this.anycast_count=anycast_count;
			this.use_anycast_addrs=use_anycast_addrs;
			this.read_percentage=read_percentage;
		}


		public void writeTo(DataOutput out) throws Exception {
			out.writeBoolean(oob);
			out.writeBoolean(sync);
			out.writeInt(num_threads);
			out.writeInt(num_msgs);
			out.writeInt(msg_size);
			out.writeInt(anycast_count);
			out.writeBoolean(use_anycast_addrs);
			out.writeDouble(read_percentage);
		}

		public void readFrom(DataInput in) throws Exception {
			oob=in.readBoolean();
			sync=in.readBoolean();
			num_threads=in.readInt();
			num_msgs=in.readInt();
			msg_size=in.readInt();
			anycast_count=in.readInt();
			use_anycast_addrs=in.readBoolean();
			read_percentage=in.readDouble();
		}

		public String toString() {
			return "oob=" + oob + ", sync=" + sync + ", anycast_count=" + anycast_count +
					", use_anycast_addrs=" + use_anycast_addrs +
					", num_threads=" + num_threads + ", num_msgs=" + num_msgs + ", msg_size=" + msg_size +
					", read percentage=" + read_percentage;
		}
	}




	public static void main(String[] args) {
		String propsFile = "conf/.xml";
		String name ="";
		String outputDir= "/home/pg/p13/a6915654/"+name+"/";
		String [] boxInits= new String[3];
		List<String> members = new ArrayList<String>();
		int msgSize = 1000;
		int load = 1;
		int numsThreads= 10;
		int numberOfMessages= 100000; // #Msgs to be executed by this node
		int totalMessages= 1000000; // #Msgs to be sent by the whole cluster
		int numOfClients= 10; 
		int numWarmUp= 10000; 
		int timeout=0;
		boolean sync=true;
		String channelName = "ZabInfinspan";
		String initiator = "";
		int rf=0;
		int cSize = 10;
		double read =0.0;
		long waitcc=0, waitii=0, waitss=0;
		int thresh=50;



		for (int i = 0; i < args.length; i++) {


			if("-warmup".equals(args[i])) {
				numWarmUp = Integer.parseInt(args[++i]);
				System.out.println(numWarmUp);
				continue;
			}

			if("-load".equals(args[i])) {
				load = Integer.parseInt(args[++i]);
				System.out.println(load);
				continue;
			}

			if("-config".equals(args[i])) {
				propsFile = args[++i];
				continue;
			}
			if ("-hosts".equals(args[i])){
				boxInits = args[++i].split(","); 
				members.addAll(Arrays.asList(boxInits));
			}
			if("-name".equals(args[i])) {
				name = args[++i];
				continue;
			}
			if("-tmessages".equals(args[i])){
				totalMessages = Integer.parseInt(args[++i]);
				continue;
			}
			if("-nmessages".equals(args[i])) {
				numberOfMessages = Integer.parseInt(args[++i]);
				continue;
			}
			if("-threads".equals(args[i])) {
				numsThreads = Integer.parseInt(args[++i]);
				continue;
			}
			if("-msgSize".equals(args[i])) {
				msgSize = Integer.parseInt(args[++i]);
				continue;
			}
			if("-outputDir".equals(args[i])) {
				outputDir = args[++i];
				continue;
			}
			if("-numClients".equals(args[i])) {
				numOfClients = Integer.parseInt(args[++i]);
				continue;
			}
			if("-timeout".equals(args[i])) {
				timeout = Integer.parseInt(args[++i]);
				continue;
			}
			if("-syn".equals(args[i])) {
				sync = Boolean.parseBoolean(args[++i]);
				continue;
			}
			if ("-channel".equals(args[i])) {
				channelName = args[++i];
				continue;
			}			
			if ("-init".equals(args[i])) {
				initiator = args[++i];
				continue;
			}
			if("-RF".equals(args[i])) {
				rf = Integer.parseInt(args[++i]);
				continue;
			}
			if ("-clusterSize".equals(args[i])) {
				cSize = Integer.parseInt(args[++i]);
				continue;
			}
			if ("-read".equals(args[i])) {
				read = Double.parseDouble(args[++i]);
				continue;
			}
			if("-waitC".equals(args[i])) {
				waitcc = (long) Integer.parseInt(args[++i]);
				continue;
			}
			if("-waitIn".equals(args[i])) {
				waitii = (long) Integer.parseInt(args[++i]);
				continue;
			}
			if("-waitSS".equals(args[i])) {
				waitss =  (long) Integer.parseInt(args[++i]);
				System.out.println("waitss=***"+waitss);
				continue;
			}
			if("-thresh".equals(args[i])) {
				thresh =  Integer.parseInt(args[++i]);
				System.out.println("thresh=***"+thresh);
				continue;
			}

		}

		ZabInfinispan test=null;
		test=new ZabInfinispan();
		try {
			test.init(members, name, propsFile, totalMessages,
					numberOfMessages, numsThreads, msgSize, 
					outputDir, numOfClients, load, numWarmUp, timeout
					, sync, channelName, initiator, cSize, rf, read,
					waitcc, waitii, waitss, thresh);
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	static void help() {
		System.out.println("UPerf [-props <props>] [-name name] [-xsite <true | false>] [-boxes String[]]");
	}

}