package org.jgroups.protocols.jzookeeper.zabCT_AdaptationUsingWriteRatioSync;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.Message.Flag;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;

import com.google.common.util.concurrent.AtomicDouble;


/* 
 * Zab_3 (main approach ) is the same implementation as Zab_2.
 * Note that all the code and implementation are simaller to Zab_2, just we change probability 
 * parameter in ZUtil class from 1.0 10 0.5.
 * Also it has features of testing throughput, latency (in Nano), ant etc. 
 * When using testing, it provides warm up test before starting real test.
 */
public class ZabCT extends Protocol {
	private final static String ProtocolName = "ZabCT";
	private static int numberOfSenderInEachClient = 25;
	protected final AtomicLong        zxid=new AtomicLong(0);
	private ExecutorService executor1;
	private ExecutorService executor2;
	private ExecutorService executor3ProcessAck;
	//private ExecutorService delayTimeout;
	//private ExecutorService sendACKToF;
	protected Address                           local_addr;
	protected volatile Address                  leader;
	protected volatile View                     view;
	protected volatile boolean                  is_leader=false;
	private List<Address> zabMembers = Collections.synchronizedList(new ArrayList<Address>());
	private long lastZxidCommitted=0;
	private final Set<MessageId> requestQueue =Collections.synchronizedSet(new HashSet<MessageId>());
	private Map<Long, ZabCTHeader> queuedCommitMessage = new HashMap<Long, ZabCTHeader>();
	private final LinkedBlockingQueue<ZabCTHeader> queuedMessages =
			new LinkedBlockingQueue<ZabCTHeader>();
	private final LinkedBlockingQueue<ZabCTHeader> delivery = new LinkedBlockingQueue<ZabCTHeader>();
	private final LinkedBlockingQueue<ACK> ackToProcess = new LinkedBlockingQueue<ACK>();
	private ConcurrentMap<Long, Proposal> outstandingProposals = new ConcurrentHashMap<Long, Proposal>();
	//private Map<Long, Long> tailProposal = Collections.synchronizedMap(new LinkedHashMap<Long, Long>());
	//private LinkedHashMap<Long, Long> copyTailProposals = new LinkedHashMap<Long, Long>();
	private final Map<Long, ZabCTHeader> queuedProposalMessage = Collections.synchronizedMap(new HashMap<Long, ZabCTHeader>());
	private final Map<MessageId, Message> messageStore = Collections.synchronizedMap(new HashMap<MessageId, Message>());
	//private ConcurrentMap<Long, Integer> followerACKs = new ConcurrentHashMap<Long, Integer>();
	private final List<ACK> vector = new ArrayList<ACK>(); 

	private TreeMap<Double, Double> pW = new TreeMap<Double, Double>();
	protected volatile boolean                  running=true;
	private final static String outDir = "/work/ZabCoinTossing/";
	private static double percentRW = 0;
	private static long waitSentTime = 0;
	private ProtocolStats stats = new ProtocolStats();
	private int numABRecieved = 0;
	@Property(name = "ZabCoinTossing_size", description = "It is ZabCoinTossing cluster size")
	private final int N = 3;
	private final int THETA_N3 = 3767; //Using Sync and no wait time
	private final int THETA_N5 = 2251;//Using Sync and no wait time
	private final int THETA_N7 = 1639 ;//Using Sync and no wait time
	private final int THETA_N9 = 1332 ;//Using Sync and no wait time

	private final double D_N3 = 17.911;
	private final double D_N5 = 27.575;
	private final double D_N7 = 38.153;
	private final double D_N9 = 48.323;
	private final double DWAITTIME_N5 = 15.118;//wait time 50 stager to make it like-like
	private final double DWAITTIME_N7 = 25.747;//wait time 50 stager to make it like-like
	private final double THETAWAITTIME_N5 = 2250.333; //It is the same as THETA_N5
	private final double THETAWAITTIME_N7 = 2250;//1610.667; //It is the same as THETA_N7
	private final double SYNCDWAITTIME_N3 = 2.218;//wait time 180 stager using Async Test
	private final double SYNCDWAITTIME_N5 = 2.248;//wait time 180 stager using Async Test
	private final double SYNCDWAITTIME_N7 = 2.545;//wait time 180 stager using Async Test
	private final double SYNCTHETAWAITTIME_N3 = 1322; //wait time 180 stager using Async Test
	private final double SYNCTHETAWAITTIME_N5 = 1290; //wait time 180 stager using Async Test
	private final double SYNCTHETAWAITTIME_N7 = 1300;//wait time 180 stager using Async Test

	private int theta ; //= (int) THETA_N3;
	private double d ; //= (double) D_N3/1000;
	private final double D = 0.5; //timeout in second
	private int n=N-1;
	@Property(name = "tail_timeout", description = "pending Proposal timeout in Millisecond, before deliver")
	private AtomicInteger tailTimeout = new AtomicInteger(2000);
	//private Timer timerForTail = new Timer();	

	private static int numReadCoundRecieved=0;
	private static int warmUp = 0;
	private boolean is_warm = true;

	private Timer timer = new Timer();
	private ZUtil zUnit= new ZUtil(1.0);
	private final SortedSet<Timeout> timeouts= Collections.synchronizedSortedSet(new TreeSet<Timeout>());
	private final LinkedBlockingQueue<ZabCTHeader> processTimeout = new LinkedBlockingQueue<ZabCTHeader>();
	private final List<Long> delays = new ArrayList<Long>();
	//private final LinkedBlockingQueue<Long> sendACKToFollower = new LinkedBlockingQueue<Long>();
	//private long latestZxidSeen=0;
	//private boolean ackedNextProposal=false;
	private static String info = null;
	//private static int thshot = 9950;
	private Timer measureLamda = new Timer();		
	private AtomicDouble propArrivalRate = new AtomicDouble(0.0);// Next arrival proposal time
	private int runingProtocol = 2;
	private final int ZabCT = 2;
	private final int Zab= 1;
	//private double p_warmup= 1;
	Map<Address, SortedSet<Timeout>> mapTimeouts = new HashMap<Address, SortedSet<Timeout>>(); //For Measuring d
	private final LinkedBlockingQueue<Timeout> processTimeoutd = new LinkedBlockingQueue<Timeout>();//For Measuring d
	private ExecutorService delayTimeout; //For Measuring d
	private List<Double> delays_d = new ArrayList<Double>();




	//For count Acks in leader
	//private Timer timerAck = new Timer();

	private long latestZxidProposed=0; // From the leader
	//private int majLargest=N/2; // From the leader
	private int majLargest=((N-1)/2)-1 ; //  ((N-1)/2)-1 follower in ZabCT


	private  AtomicInteger  numClientFinised=new AtomicInteger(0);


	public ZabCT(){

	}

	@ManagedAttribute
	public boolean isleader() {return is_leader;}
	public Address getleader() {return leader;}
	public Address getLocalAddress() {return local_addr;}

	@Override
	public void start() throws Exception {
		super.start();
		log.setLevel("trace");
		running=true;        
		executor1 = Executors.newSingleThreadExecutor();
		executor1.execute(new FollowerMessageHandler(this.id));
		executor2 = Executors.newSingleThreadExecutor();
		executor2.execute(new MessageHandler());
		executor3ProcessAck=Executors.newSingleThreadExecutor();
		executor3ProcessAck.execute(new ProcessorAck());
		this.stats = new ProtocolStats(ProtocolName, 10,
				numberOfSenderInEachClient, outDir, false, "");
	}

	public void reset() {
		//zxid.set(0);
		//lastZxidProposed = 0;
		//lastZxidCommitted = 0;
		requestQueue.clear();
		//queuedCommitMessage.clear();
		queuedProposalMessage.clear();
		queuedMessages.clear();
		outstandingProposals.clear();
		messageStore.clear();
		is_warm = false;
		//latestZxidSeen = 0;
		//requestsInProcess.set(0);
		//ackedNextProposal=false;
		if (N==3){
			d = (double) D_N3/1000;
			theta = (int) THETA_N3;
		} 
		else if (N==5){
			d = (double) D_N5/1000;
			theta = (int) THETA_N5;
		}
		else if(N==7){
			d = (double) D_N7/1000;
			theta = (int) THETA_N7;
		}
		else {
			d = (double) D_N9/1000;
			theta = (int) THETA_N9;
		}
		//zUnit.setP(p_warmup);
		this.pW=this.stats.findpW(N, zabMembers.size());
		//this.pW.remove(0.0835);
		log.info("pW====="+this.pW);

		if(!is_leader){
			//timerForTail.schedule(new TailTimeOutTask(), 5, 2000);//For tail proposal timeout
			measureLamda.schedule(new MeasuePropArrivalRate(), 5, 1000);//For tail proposal timeout
			log.info("I am Follower");
		}
		else{
			log.info("I am Leader");
		}
		warmUp = queuedCommitMessage.size();
		log.info("Vector size=:"+vector.size());
		log.info("Reset done");
	}

	@Override
	public void stop() {
		running=false;
		executor1.shutdown();
		executor2.shutdown();
		executor3ProcessAck.shutdown();
		//delayTimeout.shutdown();
		delayTimeout.shutdown();

		super.stop();
	}

	public int getn() {
		return n;
	}

	public void setn(int n){
		this.n=n;
	}


	public int getRuningProtocol() {
		return runingProtocol;
	}

	public void setRuningProtocol(int runingProtocol) {
		this.runingProtocol = runingProtocol;
	}

	public Object down(Event evt) {
		switch(evt.getType()) {
		case Event.MSG:
			return null;
		case Event.SET_LOCAL_ADDRESS:
			local_addr=(Address)evt.getArg();
			break;
		}
		return down_prot.down(evt);
	}

	public Object up(Event evt) {
		Message msg = null;
		ZabCTHeader hdr;

		switch(evt.getType()) {
		case Event.MSG:            	
			msg=(Message)evt.getArg();
			hdr=(ZabCTHeader)msg.getHeader(this.id);
			if(hdr == null){
				break; // pass up
			}
			switch(hdr.getType()) {                
			case ZabCTHeader.REQUESTW:
				forwardToLeader(msg);
				break;
			case ZabCTHeader.REQUESTR:
				hdr.getMessageOrderInfo().getId().setStartTime(System.nanoTime());
				readData(hdr.getMessageOrderInfo());
				break;
			case ZabCTHeader.FORWARD:
				queuedMessages.add(hdr);
				Timeout tReceived= new Timeout();
				//log.info("hdr.getTimeout()="+"NOT NULL_____");
				tReceived = hdr.getTimeout();
				//log.info("tReceived="+tReceived);
				tReceived.setArriveTime(System.currentTimeMillis());
				Address folAddress = msg.getSrc();
				tReceived.setId(hdr.getMessageOrderInfo().getId());
				SortedSet<Timeout> timeouts = mapTimeouts.get(folAddress);
				timeouts.add(tReceived);
				mapTimeouts.put(folAddress, timeouts); //End
				break;
			case ZabCTHeader.PROPOSAL:
				stats.numProposal.incrementAndGet();
				sendACK(msg, hdr);
				break;          		
			case ZabCTHeader.ACK:
				ackToProcess.add(new ACK(msg.getSrc(), hdr.getZxid()));
				break;
			case ZabCTHeader.ACKZAB:
				stats.countAckMessage.incrementAndGet();
				//stats.leaderCountACK.incrementAndGet();
				ackToProcess.add(new ACK(msg.getSrc(), hdr.getZxid()));
				break;
				//			case ZabCTHeader.WARMUP:
				//				stats.numProposal.set(0);
				//				this.pW=this.stats.findpW(N, zabMembers.size());
				//				if(!is_leader){
				//					measureLamda.schedule(new MeasuePropArrivalRate(), 5, 1000);//For tail proposal timeout
				//				}
				//				log.info("Running warmup1");
				//				break;
			case ZabCTHeader.STARTWORKLOAD:
				info = (String) msg.getObject();
				log.info("info=====----> "+info);
				//String waitSTString = info.split(":")[0];
				//waitSentTime = Long.parseLong(waitSTString);
				//measureLamda.cancel();
				String r = info.split(":")[0];
				percentRW = Double.parseDouble(r);
				numberOfSenderInEachClient = Integer.parseInt(info.split(":")[1]);
				this.stats = new ProtocolStats(ProtocolName, 10,
						numberOfSenderInEachClient, outDir, false, info);
				stats.setStartThroughputTime(System.currentTimeMillis());
				stats.setLastNumReqDeliveredBefore(0);
				stats.setLastThroughputTime(System.currentTimeMillis());
				//stats.setStartTimeRatio(System.currentTimeMillis());
				timer.schedule(new Throughput(), 1030, 1000);
				//if(is_leader)
				//timerAck.schedule(new Ack(), 1000, 5000);
				reset();
				break;
			case ZabCTHeader.FINISHED:
				log.info("I Have notfied from Client----> "+msg.getSrc());
				//if (clientFinished.incrementAndGet() == 10) {
				running = false;
				timer.cancel();
				//delayTimeout.shutdown();
				measureLamda.cancel();
				sendCountRead();
				log.info("Printing stats");
				//}
				break;
			case ZabCTHeader.COUNTMESSAGE:
				addCountReadToTotal(hdr);
				break;
			} 
			return null;

		case Event.VIEW_CHANGE:
			handleViewChange((View)evt.getArg());
			break;

		}

		return up_prot.up(evt);
	}


	/* --------------------------------- Private Methods ----------------------------------- */


	private void handleViewChange(View v) {
		this.view = v;
		List<Address> mbrs=v.getMembers();
		if (mbrs.size() == (N+2)) {
			for (int i=2;i<mbrs.size();i++){
				zabMembers.add(mbrs.get(i));
				vector.add(new ACK(mbrs.get(i),0));
			}

			leader = zabMembers.get(0);
			if (leader.equals(local_addr)) {
				is_leader = true;
				//For Measuring d
				for (int j=1;j<zabMembers.size();j++){
					SortedSet<Timeout> timeouts= Collections.synchronizedSortedSet(new TreeSet<Timeout>());
					mapTimeouts.put(zabMembers.get(j), timeouts);
				}//End
				majLargest = (N-1)/2 ;//For Leader  in ZabCT/ZabAA
			}
			else{
				vector.remove(new ACK(leader, 0));
				//For Measuring d
				delayTimeout = Executors.newSingleThreadExecutor();
				delayTimeout.execute(new ProcessACKDelay());
			}

		}

		if (mbrs.size() > (N+2) && zabMembers.isEmpty()) {
			for (int i = 2; i < mbrs.size(); i++) {
				zabMembers.add(mbrs.get(i));
				if ((zabMembers.size()>=N))
					break;
			}
			leader = zabMembers.get(0);
			if (leader.equals(local_addr)) {
				is_leader = true;
			}
		}

		if(mbrs.isEmpty()) return;

		if(view == null || view.compareTo(v) < 0)
			view=v;
		else
			return; 
	}

	private long getNewZxid(){
		return zxid.incrementAndGet();
	}

	private void forwardToLeader(Message msg) {
		ZabCTHeader hdrReq = (ZabCTHeader) msg.getHeader(this.id);
		requestQueue.add(hdrReq.getMessageOrderInfo().getId());
		if (is_leader){
			long stp = System.nanoTime();
			hdrReq.getMessageOrderInfo().getId().setStartTime(stp);
			queuedMessages.add(hdrReq);
		}	   
		else{
			long stf = System.nanoTime();
			hdrReq.getMessageOrderInfo().getId().setStartTime(stf);
			forward(msg);
		}


	}

	private synchronized void forward(Message msg) {
		Address target=leader;
		ZabCTHeader hdrReq = (ZabCTHeader) msg.getHeader(this.id);
		ZabCTHeader hdr = new ZabCTHeader(ZabCTHeader.FORWARD, hdrReq.getMessageOrderInfo());
		Timeout timeout = new Timeout(System.currentTimeMillis());
		hdr.setTimeout(timeout);
		Message forward_msg = new Message(target).putHeader(this.id, hdr);
		forward_msg.setBuffer(new byte[1000]);
		//forward_msg.setFlag(Message.Flag.DONT_BUNDLE);
		//forward_msg.setFlag(Message.Flag.OOB);
		try {			
			down_prot.down(new Event(Event.MSG, forward_msg));
		} catch (Exception ex) {
			log.error("failed forwarding message to " + msg, ex);
		}

	}

	private synchronized void sendACK(Message msg, ZabCTHeader hrdAck){
		Proposal p=null;
		MessageOrderInfo msgInfo = hrdAck.getMessageOrderInfo();
		long zxidACK = msgInfo.getOrdering();

		//latestZxidSeen=zxidACK;
		//For ZabCT only
		//p.setMessageOrderInfo(msgInfo);
		p = new Proposal();
		p.setMessageOrderInfo(msgInfo);
		outstandingProposals.put(zxidACK, p);
		queuedProposalMessage.put(zxidACK, hrdAck);
		ackToProcess.add(new ACK(local_addr, zxidACK)); //Follower log then ack to himself
		switch(runingProtocol){
		case Zab:
			log.info("Descsion is--->"+"ZABBBBBB");
			ZabCTHeader hdrACK = new ZabCTHeader(ZabCTHeader.ACKZAB, zxidACK);
			Message ACKMessage = new Message(leader).putHeader(this.id, hdrACK);
			//.setFlag(Message.Flag.DONT_BUNDLE);
			try {
				down_prot.down(new Event(Event.MSG, ACKMessage));
			} catch (Exception ex) {
				log.error("failed sending ACK message to Leader");
			}
			//ackToProcess.add(zxidACK);
			break;
		case ZabCT:
			//log.info("Descsion is--->"+"ZABCTTTTTTTTTT");
			//log.info("p=="+zUnit.getP());
			if (zUnit.SendAckOrNoSend()){// || ackedNextProposal) {
				stats.countACKPerBroadcast.incrementAndGet(); //For counting ACK broadcast per W%
				ZabCTHeader hdrACKCT = new ZabCTHeader(ZabCTHeader.ACKZAB, zxidACK);
				Message ackMessage = new Message().putHeader(this.id, hdrACKCT);
				//ackMessage.setFlag(Message.Flag.DONT_BUNDLE);
				//ackMessage.setFlag(Message.Flag.OOB);
				try{
					for(Address addr:zabMembers){
						if (local_addr.equals(addr)){
							continue;
						}
						Message cpy = ackMessage.copy();
						cpy.setDest(addr);
						down_prot.down(new Event(Event.MSG, cpy));  
						//stats.countAck.incrementAndGet();
					}	
				}catch(Exception ex) {
					log.error("failed proposing message to members");
				}    
			}
			//else {
			//log.info("get Tail--->"+zxidACK);
			//synchronized(tailProposal){
			//tailProposal.put(zxidACK, (System.currentTimeMillis()+tailTimeout.get()));
			//}
			//}
			break;
		}
		//For Measuring d
		long roundTrip = System.currentTimeMillis();
		Timeout fullInfo=hrdAck.getTimeout();
		if(fullInfo !=null){
			fullInfo.setArriveTimeForRoundTrip(roundTrip);
			processTimeoutd.add(fullInfo);
		}
		//else{
		//log.info("**********fullInfo=null*************"+fullInfo);
		//}//End
	}

	private synchronized void processACK(ACK ack){
		ACK fourthL = null;
		if (lastZxidCommitted >= ack.getZxid()) {
			return;
		}
		vector.remove(ack);
		vector.add(ack);
		Collections.sort(vector);
		fourthL = vector.get(majLargest);
		if (outstandingProposals.containsKey(fourthL.getZxid()) &&
				lastZxidCommitted < fourthL.getZxid()){
			long zxidCommiting = lastZxidCommitted+1;
			lastZxidCommitted = fourthL.getZxid();
			for (long z = zxidCommiting; z < (fourthL.getZxid()+1); z++){
				outstandingProposals.remove(z);
				delivery.add(new ZabCTHeader(ZabCTHeader.DELIVER, z));
			}
		}
	}

	private void deliver(long committedZxid){
		MessageOrderInfo messageOrderInfo = null;
		ZabCTHeader hdrOrginal = queuedProposalMessage.get(committedZxid);
		if (hdrOrginal == null){
			log.info("****hdrOrginal is null ****");
			return;
		}
		messageOrderInfo = hdrOrginal.getMessageOrderInfo();
		queuedProposalMessage.remove(committedZxid);
		synchronized(queuedCommitMessage){
			queuedCommitMessage.put(committedZxid, hdrOrginal);
		}
		stats.incnumReqDelivered();
		stats.setEndThroughputTime(System.currentTimeMillis());
		//log.info("Zxid=:"+committedZxid);
		if (requestQueue.contains(messageOrderInfo.getId())){
			long startTime = hdrOrginal.getMessageOrderInfo().getId().getStartTime();
			long endTime = System.nanoTime();
			stats.addLatency((long) (endTime - startTime));
			sendOrderResponse(messageOrderInfo);
			requestQueue.remove(messageOrderInfo.getId());
		}
		//requestsInProcess.decrementAndGet();

	}

	private void sendOrderResponse(MessageOrderInfo messageOrderInfo){

		CSInteractionHeader hdrResponse = new CSInteractionHeader(CSInteractionHeader.RESPONSEW, messageOrderInfo);
		Message msgResponse = new Message(messageOrderInfo.getId()
				.getOriginator()).putHeader((short) 79, hdrResponse);
		//log.info("Inside sendOrderResponse**************:");
		//msgResponse.setFlag(Message.Flag.DONT_BUNDLE);
		down_prot.down(new Event(Event.MSG, msgResponse));
	}

	private synchronized void readData(MessageOrderInfo messageInfo){
		Message readReplay = null;
		CSInteractionHeader hdrResponse = null;
		ZabCTHeader hdrOrginal = null;
		synchronized(queuedCommitMessage){
			hdrOrginal = queuedCommitMessage.get(messageInfo.getOrdering());
		}

		if (hdrOrginal != null){
			hdrResponse = new CSInteractionHeader(CSInteractionHeader.RESPONSER, 
					hdrOrginal.getMessageOrderInfo());
		}
		else{
			hdrResponse = new CSInteractionHeader(CSInteractionHeader.RESPONSER, messageInfo);
		}
		readReplay = new Message(messageInfo.getId().getOriginator()).putHeader((short) 79, hdrResponse);

		//long startTime = messageInfo.getId().getStartTime();
		//long endTime = System.nanoTime();
		//stats.addReadLatency((endTime - startTime));
		messageInfo.getId().setStartTime(0);
		readReplay.setBuffer(new byte[1000]);
		//stats.incnumReqDelivered();
		stats.setEndThroughputTime(System.currentTimeMillis());
		readReplay.setFlag(Message.Flag.DONT_BUNDLE);
		//readReplay.setFlag(Message.Flag.OOB);
		down_prot.down(new Event(Event.MSG, readReplay));
		//requestsInProcess.decrementAndGet();
	}

	private boolean isQuorum(int majority){
		return majority >= ((N/2) + 1)? true : false;
	}

	private void sendCountRead(){
		int writeOnly= queuedCommitMessage.size()-warmUp;
		int readOnly = stats.getnumReqDelivered() - writeOnly;
		//		System.out.println("writeOnly="+writeOnly);
		//		System.out.println("readOnly="+readOnly);
		//		System.out.println("queuedCommitMessage.size()="+queuedCommitMessage.size());
		//		System.out.println("stats.getnumReqDelivered()="+stats.getnumReqDelivered());

		ZabCTHeader readCount = new ZabCTHeader(ZabCTHeader.COUNTMESSAGE, readOnly);
		Message countRead = new Message(leader).putHeader(this.id,
				readCount);
		countRead.setFlag(Flag.DONT_BUNDLE);
		for (Address address : zabMembers) {
			if (address.equals(local_addr))
				continue;
			Message cpy = countRead.copy();
			cpy.setDest(address);
			down_prot.down(new Event(Event.MSG, cpy));
		}
	}

	//	private void sendCountAckSent(){
	//		ZabCoinTossingHeader ackCount = new ZabCoinTossingHeader(ZabCoinTossingHeader.COUNTACK, stats.countAck.get());
	//		Message countAck = new Message(leader).putHeader(this.id,
	//				ackCount);
	//		countAck.setFlag(Flag.DONT_BUNDLE);
	//		Message cpy = countAck.copy();
	//		down_prot.down(new Event(Event.MSG, cpy));
	//	}
	//
	//	private synchronized void addCountAckToTotal(ZabCoinTossingHeader countAckHeader) {
	//		long ackCount = countAckHeader.getZxid();
	//		stats.countAck.addAndGet((int) ackCount);
	//	}

	private synchronized void addCountReadToTotal(ZabCTHeader countReadHeader) {
		long readCount = countReadHeader.getZxid();
		System.out.println("readCount="+readCount);
		stats.addToNumReqDelivered((int) readCount);
		numReadCoundRecieved++;
		if(numReadCoundRecieved==(zabMembers.size()-1)){
			System.out.println("Print Total request (R and W)="+stats.getnumReqDelivered());
			stats.printProtocolStats(queuedCommitMessage.size(), N, (int) (percentRW*100), waitSentTime, is_leader);	

		}

	}
	private synchronized void addTotalABMssages(ZabCTHeader carryCountMessageLeader) {
		long followerMsg = carryCountMessageLeader.getZxid();
		stats.addCountTotalMessagesFollowers((int) followerMsg);
		numABRecieved++;
		if(numABRecieved==zabMembers.size()-1){
			ZabCTHeader headertStats = new ZabCTHeader(ZabCTHeader.STATS);
			for (Address zabServer:zabMembers){
				Message messageStats = new Message(zabServer).putHeader(this.id,
						headertStats);
				messageStats.setFlag(Message.Flag.DONT_BUNDLE);
				down_prot.down(new Event(Event.MSG, messageStats));
			}
		}
	}


	/* ----------------------------- End of Private Methods -------------------------------- */


	final class FollowerMessageHandler implements Runnable {

		private short id;
		public FollowerMessageHandler(short id){
			this.id = id;
		}

		@Override
		public void run() {
			handleRequests();         
		}

		/**
		 * create a proposal and send it out to all the members
		 * 
		 * @param message
		 */

		private void handleRequests() {
			ZabCTHeader hdrReq = null;
			while (running) {
				try {
					hdrReq=queuedMessages.take();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				long new_zxid = getNewZxid();
				MessageOrderInfo messageOrderInfo = hdrReq.getMessageOrderInfo();
				messageOrderInfo.setOrdering(new_zxid);

				ZabCTHeader hdrProposal = new ZabCTHeader(ZabCTHeader.PROPOSAL, messageOrderInfo); 
				Message proposalMessage=new Message().putHeader(this.id, hdrProposal);

				proposalMessage.setSrc(local_addr);
				proposalMessage.setBuffer(new byte[1000]);
				//proposalMessage.setFlag(Message.Flag.DONT_BUNDLE);
				Proposal p = new Proposal();
				p.setMessageOrderInfo(hdrReq.getMessageOrderInfo());
				p.addAddressesACK(local_addr);
				outstandingProposals.put(new_zxid, p);
				queuedProposalMessage.put(new_zxid, hdrProposal);
				ackToProcess.add(new ACK(local_addr, new_zxid)); //For ZabCt/ZabCT/ZabAA
				Timeout infoToFollower;

				try{
					for (Address address : zabMembers) {
						if(address.equals(leader))
							continue; 
						//For Measuring d
						if (!(mapTimeouts.get(address).isEmpty())){
							infoToFollower=mapTimeouts.get(address).first();
							mapTimeouts.get(address).remove(infoToFollower);
							infoToFollower.setSendBackTimeout(System.currentTimeMillis());
							hdrProposal.setTimeout(infoToFollower);
						}
						//else{
						//log.info("In else $$$$$$$$$$$");
						//}// End
						Message cpy = proposalMessage.copy();
						cpy.setDest(address);
						down_prot.down(new Event(Event.MSG, cpy));     
					}
				}catch(Exception ex) {
					log.error("failed proposing message to members");
				}    


			}

		}


	}

	//	final class SendToFollower implements Runnable {
	//		private short id;
	//
	//		public SendToFollower(short id) {
	//			this.id=id;
	//		}
	//
	//		@Override
	//		public void run() {
	//			startSentToFollower();
	//		}

	//		private void startSentToFollower() {
	//			ZabCoinTossingHeader hdrACK=null;
	//			Message ackMessage = null;
	//			long ackedzxid = 0;
	//			long currentTime=0;
	//			while (true) {
	//				try {
	//					ackedzxid = sendACKToFollower.take();
	//					currentTime=System.currentTimeMillis();
	//					Timeout timeout = new Timeout();				
	//					if(!timeouts.isEmpty()){
	//						timeout = timeouts.first();
	//						timeouts.remove(timeout);
	//						timeout.setSendBackTimeout(currentTime);
	//						//log.info("timeouts!=isEmpty()");
	//						//log.info("Zxid="+ackedzxid+"/Send Time="+timeout.getSendTime()+"SB="+currentTime);
	//					}
	//					else{
	//						timeout = new Timeout(currentTime);
	//						//log.info("timeouts=isEmpty()");
	//						//log.info("Zxid="+ackedzxid+"/Send Time="+currentTime+"NoSB");
	//					}
	//					hdrACK = new ZabCoinTossingHeader(ZabCoinTossingHeader.ACK, ackedzxid, timeout);
	//					ackMessage = new Message(otherFollower).putHeader(this.id, hdrACK);
	//					ackMessage.setFlag(Message.Flag.DONT_BUNDLE);
	//					Message cpyy = ackMessage.copy();
	//					down_prot.down(new Event(Event.MSG, cpyy));
	//				} catch (InterruptedException e) {
	//					e.printStackTrace();
	//				}          
	//
	//			}
	//		}
	//
	//	} 

	final class ProcessorAck implements Runnable {
		@Override
		public void run() {
			processAckedZxid();

		}

		private void processAckedZxid() {
			ACK ackedzxid = null;
			while (true) {
				try {
					ackedzxid = ackToProcess.take();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}          
				processACK(ackedzxid);

			}
		}

	} 

	//	final class ProcessorAckForWarmUp implements Runnable {
	//		@Override
	//		public void run() {
	//			processAckedZxid();
	//
	//		}
	//
	//		private void processAckedZxid() {
	//			ACK ackedzxid = null;
	//			while (true) {
	//				try {
	//					ackedzxid = ackToProcess.take();
	//				} catch (InterruptedException e) {
	//					e.printStackTrace();
	//				}          
	//				processACKForWarmUp(ackedzxid);
	//
	//			}
	//		}
	//
	//	} 

	//	final class ProcessACKDelay implements Runnable {
	//		private long sendTime=0;
	//		private long arriveTime=0;
	//		private long sendBackTime=0;
	//		private long idleTime=0;
	//		private long delay=0;
	//		private long arriveTimeForRoundTrip=0;
	//
	//
	//		@Override
	//		public void run() {
	//			startProcess();
	//		}
	//
	//		private void startProcess() {
	//			ZabCoinTossingHeader hdrTimeout= null;
	//			while (true) {
	//				try {
	//					hdrTimeout = processTimeout.take();
	//					Timeout timeout = hdrTimeout.getTimeout();
	//					sendBackTime = timeout.getSendBackTimeout();
	//					arriveTime =  timeout.getArriveTime();
	//					sendTime = timeout.getSendTime();
	//					arriveTimeForRoundTrip = timeout.getArriveTimeForRoundTrip();
	//					idleTime =sendBackTime-arriveTime;
	//					delay=(arriveTimeForRoundTrip-sendTime)-idleTime;
	//					//log.info("ST="+sendTime+"/AT="+arriveTime+"/SB="+sendBackTime+"/LAT="
	//					//+arriveTimeForRoundTrip+"/id="+idleTime+"/d="+delay);
	//
	//					delays.add((delay/2));//divide by 2 to get one round;
	//				} catch (InterruptedException e) {
	//					e.printStackTrace();
	//				}          
	//			}
	//		}
	//
	//	} 
	final class MessageHandler implements Runnable {
		@Override
		public void run() {
			deliverMessages();

		}

		private void deliverMessages() {
			ZabCTHeader hdrDelivery= null;
			while (true) {
				try {
					hdrDelivery = delivery.take();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}    
				//log.info("DEL------------------->"+hdrDelivery.getZxid());
				deliver(hdrDelivery.getZxid());

			}
		}

	} 

	class Throughput extends TimerTask {

		public Throughput() {

		}

		private long startTime = 0;
		private long currentTime = 0;
		private double currentThroughput = 0;
		private int finishedThroughput = 0;

		@Override
		public void run() {
			startTime = stats.getLastThroughputTime();
			currentTime = System.currentTimeMillis();
			finishedThroughput=stats.getnumReqDelivered();			
			currentThroughput = (((double)finishedThroughput - stats
					.getLastNumReqDeliveredBefore()) / ((double)(currentTime - startTime)/1000.0));
			stats.setLastNumReqDeliveredBefore(finishedThroughput);
			stats.setLastThroughputTime(currentTime);
			stats.addThroughput(currentThroughput);
		}

		public String convertLongToTimeFormat(long time) {
			Date date = new Date(time);
			SimpleDateFormat longToTime = new SimpleDateFormat("HH:mm:ss.SSSZ");
			return longToTime.format(date);
		}
	}

	//	class Rate extends TimerTask {
	//
	//		public Rate() {
	//
	//		}
	//
	//		private long startTime = 0;
	//		private long currentTime = 0;
	//		private double currentThroughput = 0;
	//		private int finishedThroughput = 0;
	//
	//		@Override
	//		public void run() {
	//			startTime = stats.getLastThroughputTime();
	//			currentTime = System.currentTimeMillis();
	//			finishedThroughput=stats.getnumReqDelivered();			
	//			currentThroughput = (((double)finishedThroughput - stats
	//					.getLastNumReqDeliveredBefore()) / ((double)(currentTime - startTime)/1000.0));
	//			stats.setLastNumReqDeliveredBefore(finishedThroughput);
	//			stats.setLastThroughputTime(currentTime);
	//			stats.addThroughput(currentThroughput);
	//		}
	//
	//		public String convertLongToTimeFormat(long time) {
	//			Date date = new Date(time);
	//			SimpleDateFormat longToTime = new SimpleDateFormat("HH:mm:ss.SSSZ");
	//			return longToTime.format(date);
	//		}
	//	}


	//	class TailTimeOutTask extends TimerTask {
	//		public TailTimeOutTask() {
	//		}
	//
	//		public void run() {
	//			ZabCoinTossingHeader hdrACK = null;
	//			Message ackMessage = null;
	//			if(!tailProposal.isEmpty()){
	//				synchronized(tailProposal){
	//					copyTailProposals= new LinkedHashMap<Long,Long>(tailProposal);
	//				}
	//				//stats.countTailTimeout.incrementAndGet();
	//				//log.info("Tail size: "+tailProposal.size());
	//				for (long zx:copyTailProposals.keySet()){
	//					long diffTime = copyTailProposals.get(zx) - System.currentTimeMillis();
	//					if (diffTime <= 0 ){//tail timeout elapses
	//						log.info("Procssing ACK casuing by timeout====>: "+zx);
	//						hdrACK = new ZabCoinTossingHeader(ZabCoinTossingHeader.ACK, zx);
	//						ackMessage = new Message(leader).putHeader(id, hdrACK);
	//						down_prot.down(new Event(Event.MSG, ackMessage)); 
	//						ackToProcess.add(zx);
	//						log.info("Send Ack timeout  for zxid="+zx);
	//						//log.info("count Tail timeout elapsed:="+stats.countTailTimeout.incrementAndGet());
	//						//sendACKToFollower.add(zx);
	//						synchronized(tailProposal){
	//							tailProposal.remove(zx);
	//						}
	//					}
	//					else{
	//						timerForTail.cancel();
	//						timerForTail = new Timer();
	//						timerForTail.schedule(new TailTimeOutTask(), 0, diffTime);  							
	//					}
	//					break;
	//				}
	//				copyTailProposals.clear();
	//			}
	//
	//
	//
	//		}
	//
	//
	//
	//	}

	final class ProcessACKDelay implements Runnable {
		private long sendTime=0;
		private long arriveTime=0;
		private long sendBackTime=0;
		private long idleTime=0;
		private long delay=0;
		private long arriveTimeForRoundTrip=0;



		@Override
		public void run() {
			startProcess();
		}

		private void startProcess() {
			Timeout delayInfo= null;
			while (true) {
				try {
					delayInfo = processTimeoutd.take();
					//String id = delayInfo.getZxid();
					sendBackTime = delayInfo.getSendBackTimeout();
					arriveTime =  delayInfo.getArriveTime();
					sendTime = delayInfo.getSendTime();
					arriveTimeForRoundTrip = delayInfo.getArriveTimeForRoundTrip();
					idleTime =sendBackTime-arriveTime;
					delay=(arriveTimeForRoundTrip-sendTime)-idleTime;
					//log.info("ST="+sendTime+"/AT="+arriveTime+"/SB="+sendBackTime+"/LAT="
					//+arriveTimeForRoundTrip+"/id="+idleTime+"/d="+delay+" /d/2="+((double) delay/2)+" /Addres-Zxid="+id);
					//For Measuring d
					synchronized (delays_d) {
						delays_d.add(((double)delay/2));//divide by 2 to get one round;
					}

				} catch (InterruptedException e) {
					e.printStackTrace();
				}          
			}
		}

	} 

	class MeasuePropArrivalRate extends TimerTask {
		//private int countSec=0;
		//private double d=0;
		private int lastNumProposal=0;
		private double c2p2=0; //store result of (((double) theta/n) * ((double) 1/numProposalPerec));
		private double p=0.0;
		private double dMuliPropArr=0.0;
		private int sec=0, count0=0, countNo0=0 ;
		private String result=null;
		private SortedSet<Double> ps = new TreeSet<Double>();
		private DecimalFormat roundValue = new DecimalFormat("#.000");
		private TreeMap<Double, Double> extraCopypW = new TreeMap<Double, Double>();
		private boolean foundp=false;
		public MeasuePropArrivalRate() {

		}

		@Override
		public void run() {
			sec++;
			extraCopypW = new TreeMap<Double, Double>(stats.getExtrapW());
			foundp=false;
			lastNumProposal = (stats.numProposal.get()-stats.lastNumProposal.get());
			stats.lastNumProposal.set(stats.numProposal.get());

			ArrayList<Double> removedp = new ArrayList<Double>();
			TreeMap<Double, Double> copypW = new TreeMap<Double, Double>(pW);

			if(delays_d.size()!=0)
				d = (double) measured()/1000.0;
			if (lastNumProposal!=0){
				propArrivalRate.set( ((double) 1/lastNumProposal));
				c2p2 = findCondtion2Part2(lastNumProposal);
				c2p2= Double.parseDouble(roundValue.format(c2p2));
			//	log.info("Max="+(Math.max(lastNumProposal,((double) 1/D))));
				dMuliPropArr = d*(Math.max(lastNumProposal,((double) 1/D)));
				for (double p: copypW.keySet()){
					if (p>=c2p2 || copypW.get(p)>=dMuliPropArr){
						removedp.add(p);
					}
				}
				if(!removedp.isEmpty()){
					for (double p:removedp){
						copypW.remove(p);							
					}
				}
				if (!copypW.isEmpty()){
					if (runingProtocol==ZabCT){
						final Entry<Double, Double> largeKey = copypW.lastEntry();
						zUnit.setP(largeKey.getKey());
						stats.addResult(""+lastNumProposal+","+dMuliPropArr+","+c2p2+","+largeKey.getKey()+","+sec);
						return;
					}
					else{
						log.info("copypW is empty !!!!!");
						log.info("Must Change to ZabCT   ********************************");
						final Entry<Double, Double> largeKey = copypW.lastEntry();
						zUnit.setP(largeKey.getKey());
						return;
					}
				}
				else if(!stats.getExtrapW().isEmpty()){
					//log.info("Extra1");
					//log.info("extrapW"+stats.getExtrapW());
					for (double pp: copypW.keySet()){
						if (pp<c2p2 && extraCopypW.get(pp)<dMuliPropArr){
							zUnit.setP(pp);
							foundp = true;
							stats.addResult(""+lastNumProposal+","+dMuliPropArr+","+c2p2+","+pp+","+sec+","+"extra1");
							return;
						}

					}
				}
				if((stats.getExtrapW().isEmpty()) || !foundp){
					//log.info("Extra2");
					if(!stats.getExtrapW().isEmpty()){
						this.p = stats.findExtrap(0, n, dMuliPropArr,c2p2, (stats.getExtrapW().lastEntry().getKey()));
						if(this.p!=0.0){
							zUnit.setP(this.p);
							stats.addResult(""+lastNumProposal+","+dMuliPropArr+","+c2p2+","+this.p+","+sec+","+"extra2");
							return;
						}
					}
					else{
						this.p = stats.findExtrap(0, n, dMuliPropArr,c2p2, (pW.lastEntry().getKey()));
						if(this.p!=0.0){
							zUnit.setP(this.p);
							stats.addResult(""+lastNumProposal+","+dMuliPropArr+","+c2p2+","+this.p+","+sec+","+"extra2");
							return;
						}						
					}
				}
				log.info("Must Change to Zab  $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
				log.info("ArrivalRate=:"+lastNumProposal+" /d*Lambda=:"+dMuliPropArr+
						" /(Theta/n * 1/Lambda)=:"+c2p2+" /p=: not found");
			}

		}

		public double measured(){
			double sumd=0.0, avgDelay_d = 0.0;;
			synchronized (delays_d) {

				for (double d:delays_d){
					sumd+=d;
				}
				avgDelay_d = sumd/delays_d.size();
				delays_d.clear();
			}
			return avgDelay_d;
		}
		public double findCondtion2Part2(int numProposalPerec){
			double c2p2=0.0;
			c2p2 = (((double) theta/n) * ((double) 1/numProposalPerec));
			return c2p2;
		}

	}

	//	class Ack extends TimerTask {
	//
	//		public Ack() {
	//
	//		}
	//
	//		private long startTime = 0;
	//		private long currentTime = 0;
	//		private double currentAck = 0;
	//		private int CurrentNumAcks = 0;
	//
	//		@Override
	//		public void run() {
	//			startTime = stats.getLastAcktTime();
	//			currentTime = System.currentTimeMillis();
	//			CurrentNumAcks=stats.countAckMessage.get();
	//			currentAck = (((double)CurrentNumAcks - stats
	//					.getLastNumAcks()) / ((double)(currentTime - startTime)/1000.0));
	//			stats.setLastNumAcks(CurrentNumAcks);
	//			stats.setLastAckTime(currentTime);
	//			stats.addAck(currentAck);
	//		}
	//
	//		public String convertLongToTimeFormat(long time) {
	//			Date date = new Date(time);
	//			SimpleDateFormat longToTime = new SimpleDateFormat("HH:mm:ss.SSSZ");
	//			return longToTime.format(date);
	//		}
	//	}

}
