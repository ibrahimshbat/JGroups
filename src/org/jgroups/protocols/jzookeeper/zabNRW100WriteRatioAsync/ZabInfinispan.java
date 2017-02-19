package org.jgroups.protocols.jzookeeper.zabNRW100WriteRatioAsync;

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
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
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
import org.omg.Messaging.SyncScopeHelper;

public class ZabInfinispan extends ReceiverAdapter {
	private JChannel               channel;
	private Address                local_addr;
	private RpcDispatcher          disp;
	static final String            groupname="Cluster";
	private String propsFile = "conf/Zab.xml";
	private static String ProtocotName = "";
	protected final List<Address>  members=new ArrayList<Address>();
	protected final List<Address>  site_masters=new ArrayList<Address>();
	private List<String> boxMembers  = new ArrayList<String>();
	private List<Address> box = new ArrayList<Address>();
	private int clusterSize = 7;
	private AtomicLong localSequence = new AtomicLong();
	private String outputDir;
	private View view;
	private String initiator;
	private String channelName;
	private static PrintWriter outFile;
	private AtomicInteger numClientsFinished = new AtomicInteger();
	private boolean warmUp = true;
	private static Random rand = new Random();
	private Credit credits = new Credit(0);
	private final int thros=150;;
	Invoker[] invokers;
	//private Timer checkerRatioUpdated = new Timer();

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
	private static int min = 0, max = 0;
	private  int c = 0;
	// =======================================================
	private Timer checkCounter = new Timer();


	private final AtomicInteger COUNTER=new AtomicInteger(1);


	public void init(List<String> members, String protocolName, String props,
			int totalNum_msgs, int totalPerThreads, int num_threads,
			int msg_size, String outputDir, int numOfClients, int load,
			int numsOfWarmUp, int timeout, boolean sync, String 
			channelName, String initiator,int clusterSize, int rf, double read,
			long waitCC, long waitII, long waitSS) throws Throwable {
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
		this.waitCC= waitCC;
		this.waitII = waitII;
		this.waitSS = waitSS;
		this.min = (int) waitSS - ((int) (waitSS * 0.25));
		this.max = (int) waitSS + ((int) (waitSS * 0.25));
		this.timeout = 0;
		this.sync = sync;
		this.channelName = channelName;
		this.initiator = initiator;
		this.anycast_count = rf;
		this.clusterSize = clusterSize;
		this.invokers=new Invoker[num_threads];
		this.read_percentage = read;
		channel=new JChannel(propsFile);
		channel.setReceiver(this);
		System.out.println("this.ProtocotName := " + this.ProtocotName+ " propsFile :="+propsFile+
				"this.num_threads "+this.num_threads + " outdir := "+outputDir);

		channel.connect(channelName);
		local_addr=channel.getAddress();
		this.boxMembers = members;

		try {
			MBeanServer server=Util.getMBeanServer();
			JmxConfigurator.registerChannel(channel, server, "jgroups", channel.getClusterName(), true);
		}
		catch(Throwable ex) {
			System.err.println("registering the channel in JMX failed: " + ex);
		}

		//if (box.size()>clusterSize)
		//sendMyAddressToZab();
		removeBoxMembers(this.members);
		System.out.println("this.members==========="+this.members);
		CSInteractionHeader startHeader = new CSInteractionHeader(CSInteractionHeader.STARTREALTEST);
		Message startTest = new Message().putHeader((short) 79, startHeader);
		startTest.setFlag(Message.Flag.DONT_BUNDLE);
		if (channel.getAddress().toString().contains(initiator)) {
			System.out.println("I am initiator");
			//startBenchmark();
			for (int i = 0; i < this.members.size(); i++) {
				startTest.setDest(this.members.get(i));			
				channel.send(startTest);
			}
			//startTest.setDest(channel.getAddress());			
			//channel.send(startTest);
		}

	}


	void stop() {
		if(disp != null)
			disp.stop();
		Util.close(channel);
	}

	private Address pickCoordinator() {
		if (boxMembers == null) {
			System.out.println("Box Members is null");
			return members.get(0);
		}
		for (Address address : members) {
			for (String boxName : boxMembers) {
				if (!address.toString().contains(boxName))
					return address;
			}
		}
		return null;
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




	// =================================== callbacks ======================================

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

	public void startWarm() throws Throwable {

		System.out.println("Start warm up");
		final AtomicInteger num_msgs_sent=new AtomicInteger(0);
		final AtomicInteger checkWRatioUpdatedWarm=new AtomicInteger(0);
		List<Address> nonBoxMembers = new ArrayList<Address>(members);
		removeBoxMembers(nonBoxMembers);

		Random random = new Random();
		Invoker[] invokers=new Invoker[num_threads];
		// create sender (threads) to send writes to /_1/_2
		for(int i=0; i < invokers.length; i++){
			invokers[i]=new Invoker(nonBoxMembers, (numsOfWarmUp/numOfClients), num_msgs_sent, random, 0.0,
					true, waitSS, checkWRatioUpdatedWarm, channel, credits, thros);
			System.out.println("Create Invoker --------------->>>> " + i);

		}

		long start=System.currentTimeMillis();
		for(Invoker invoker: invokers)
			invoker.start();

		for(Invoker invoker: invokers) {
			invoker.join();
		}

		long total_time=System.currentTimeMillis() - start;
		System.out.println("warmUP done (in " + total_time + " ms)");
	}

	public void startTest() throws Throwable {
		warmUp = false;
		System.out.println("invoking " + num_msgs + " RPCs of " + Util.printBytes(msg_size) +
				", sync=" + sync + ", oob=" + oob + ", use_anycast_addrs=" + use_anycast_addrs);
		int total_gets=0, total_puts=0;
		final AtomicInteger num_msgs_sent=new AtomicInteger(0);
		final AtomicInteger checkWRatioUpdated=new AtomicInteger(0);
		//		if (channel.getAddress().toString().contains(initiator)) {
		//			checkerRatioUpdated.schedule(new ChekerWriteRatio(checkWRatioUpdated), 5, 1000);
		//		}
		List<Address> nonBoxMembers = new ArrayList<Address>(members);
		removeBoxMembers(nonBoxMembers);

		Random random = new Random();
		// create sender (threads) to send writes to /_1/_2
		for(int i=0; i < invokers.length; i++){
			invokers[i]=new Invoker(nonBoxMembers, (num_msgs/numOfClients), num_msgs_sent, random, read_percentage,
					false, waitSS, checkWRatioUpdated, channel, credits, thros);
			System.out.println("Create Invoker --------------->>>> " + i);
		}

		long start=System.currentTimeMillis();
		for(Invoker invoker: invokers)
			invoker.start();

		for(Invoker invoker: invokers) //{
			invoker.join();
		//			total_gets+=invoker.numGets();
		//			total_puts+=invoker.numPuts();
		//		}

		long total_time=System.currentTimeMillis() - start;
		System.out.println("done (in " + total_time + " ms)");
	}




	/** Kicks off the benchmark on all cluster nodes */
	void startBenchmark() throws Throwable {
		checkCounter.schedule(new Printer(credits), 5, 500);

		synchronized (members) {
			removeBoxMembers(members);
		}
		System.out.println("Members at start | " + members);

		Collection<Address> dest;
		if (anycastRequests)
			dest = members;
		else
			dest = null;
		//sendStartNotfiy();
		//First, it calls for warmUp 
		//RequestOptions options=new RequestOptions(ResponseMode.GET_ALL, 0, anycastRequests);
		//options.setFlags(Message.Flag.OOB, Message.Flag.DONT_BUNDLE, Message.NO_FC);
		//RspList<Object> responses=disp.callRemoteMethods(dest, new MethodCall(STARTWARM), options);
		//startWarm();
		System.out.println("after WarmUp");
		if (channel.getAddress().toString().contains(initiator)) {
			sendStartNotify();
		}
		//Thread.sleep(30);

		//responses=disp.callRemoteMethods(dest, new MethodCall(START), options);
		startTest();
		System.out.println("after real test");
		//sendCompleteNotify();

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
		CSInteractionHeader startTest = (CSInteractionHeader) msg.getHeader((short) 79);
		if(startTest!=null){
			if(startTest.getType()==CSInteractionHeader.STARTREALTEST){
				try {
					startBenchmark();
					return;
				} catch (Throwable e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		System.out.println("Received="+(c++));
		synchronized (this) {
			synchronized(credits){
				if (credits.decAndGet()<thros){
					credits.notifyAll();
				}
			}
			//}
			//	}
		}
	}
	//Sender to send write RPC, for testing ording protocol 
	private class Invoker extends Thread {
		private final List<Address>  dests=new ArrayList<Address>();
		private final int            num_msgs_to_send;
		private final AtomicInteger  num_msgs_sent;
		private int                  num_gets=0;
		private int                  num_puts=0;
		private Random random;
		private double read_per=0;
		private boolean warmStage = false;
		private int minl = 0, maxl = 0;
		private long waitSSl=0;
		private final DecimalFormat roundValue = new DecimalFormat("#.0");
		private int threshold = 10000;
		private final int thresholdFixed = 10000;
		private long sendTime = 0;
		private final AtomicInteger  checkWRatio;
		private JChannel               channel;
		private boolean wait = false;
		private Credit  credits;
		private int thresh = 0;






		public Invoker(Collection<Address> dests, int num_msgs_to_send, 
				AtomicInteger num_msgs_sent, Random random, double read_per, boolean isWarm, long waitSSl
				, AtomicInteger checkWRatioUpdated, JChannel channel, Credit credits, int thros) {
			this.num_msgs_sent=num_msgs_sent;
			this.dests.addAll(dests);
			this.num_msgs_to_send=num_msgs_to_send;
			this.random = random;
			this.read_per = read_per;
			this.warmStage = isWarm;
			this.waitSSl=waitSSl;
			this.minl = (int) this.waitSSl - ((int) (this.waitSSl * 0.25));
			this.maxl = (int) this.waitSSl + ((int) (this.waitSSl * 0.25));
			this.sendTime = minl + rand.nextInt((maxl - minl) + 1);
			this.checkWRatio = checkWRatioUpdated;
			this.credits = credits;
			this.channel = channel;
			this.thresh = thros;
			setName("Invoker-" + COUNTER.getAndIncrement());
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
				if(i >= num_msgs_to_send){
					System.out.println(" *********** Name "+this.getName() + " Finished=: " + i);;
					break;
				}
				//				if (!warmUp){
				//					try {
				//						Thread.sleep(this.waitSSl);
				//						//System.out.println("this.sendTime--->"+this.sendTime);
				//					} catch (InterruptedException e) {
				//
				//					}
				//				}

				boolean get=Util.tossWeightedCoin(read_per);
				try {
					if(get) { // sync GET
						synchronized (members) {
							removeBoxMembers(members);
						}
						get_args[0]=i;
						ZabHeader hdrReq = new ZabHeader(ZabHeader.REQUESTR);
						Message sendMessage = new Message(null).putHeader((short) 78, hdrReq);
						channel.send(sendMessage);
						num_gets++;
					}
					else {    // sync or async (based on value of 'sync') PUT
						synchronized (credits) {
							if(credits.incAndGet()>=thresh){
								credits.wait();
							}
							//else{
							ZabHeader hdrReq = new ZabHeader(ZabHeader.REQUESTW);
							Message sendMessage = new Message(null).putHeader((short) 78, hdrReq);
							channel.send(sendMessage);
							num_puts++;
							System.out.println("Name--->"+this.getName()+" NumSent="+num_puts);
							//}
						}


					}
				}
				catch(Throwable throwable) {
					throwable.printStackTrace();
				}
			}
		}

		public boolean isWait() {
			return wait;
		}
		public void SetIsWait(boolean wait) {
			this.wait = wait;
		}
		private Address pickTarget() {
			int index=dests.indexOf(local_addr);
			int new_index=(index +1) % dests.size();
			return dests.get(new_index);
		}

		private Collection<Address> pickRandomAnycastTargets() {
			Collection<Address> anycastTargets = new HashSet<Address>(anycast_count);

			if (include_local_address)
				anycastTargets.add(local_addr);

			while (anycastTargets.size() < anycast_count) {
				int randomIndex = random.nextInt(dests.size());
				Address randomAddress = dests.get(randomIndex);

				if (!anycastTargets.contains(randomAddress))
					anycastTargets.add(randomAddress);
			}
			return anycastTargets;
		}

		private Collection<Address> pickAnycastTargets() {
			Collection<Address> anycast_targets=new ArrayList<Address>(anycast_count);

			if (include_local_address)
				anycast_targets.add(local_addr);

			int index=dests.indexOf(local_addr);
			for(int i=index + 1; i < index + 1 + anycast_count; i++) {
				int new_index=i % dests.size();
				Address tmp=dests.get(new_index);
				if(!anycast_targets.contains(tmp))
					anycast_targets.add(tmp);
			}
			return anycast_targets;
		}

		private Collection<Address> pickLocalTarget() {
			Collection<Address> anycast_targets=new ArrayList<Address>(anycast_count);
			anycast_targets.add(local_addr);
			return anycast_targets;
		}
		public void changeReadRatio(double newRatio){
			this.read_per=Double.parseDouble(roundValue.format(newRatio));
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

		}

		ZabInfinispan test=null;
		test=new ZabInfinispan();
		try {
			test.init(members, name, propsFile, totalMessages,
					numberOfMessages, numsThreads, msgSize, 
					outputDir, numOfClients, load, numWarmUp, timeout
					, sync, channelName, initiator, cSize, rf, read, waitcc, waitii, waitss);
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	static void help() {
		System.out.println("UPerf [-props <props>] [-name name] [-xsite <true | false>] [-boxes String[]]");
	}


	class Printer extends TimerTask {
		private Credit c;
		public Printer(Credit c) {
			this.c = c;
		}

		@Override
		public void run() {
			System.out.println("current credit="+c.getCounter());

		}
	}

}