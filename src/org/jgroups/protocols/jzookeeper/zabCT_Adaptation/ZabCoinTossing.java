package org.jgroups.protocols.jzookeeper.zabCT_Adaptation;
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
public class ZabCoinTossing extends Protocol {
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
	private Map<Long, ZabCoinTossingHeader> queuedCommitMessage = new HashMap<Long, ZabCoinTossingHeader>();
	private final LinkedBlockingQueue<ZabCoinTossingHeader> queuedMessages =
			new LinkedBlockingQueue<ZabCoinTossingHeader>();
	private final LinkedBlockingQueue<ZabCoinTossingHeader> delivery = new LinkedBlockingQueue<ZabCoinTossingHeader>();
	private final LinkedBlockingQueue<Long> ackToProcess = new LinkedBlockingQueue<Long>();
	private ConcurrentMap<Long, Proposal> outstandingProposals = new ConcurrentHashMap<Long, Proposal>();
	private Map<Long, Long> tailProposal = Collections.synchronizedMap(new LinkedHashMap<Long, Long>());
	private LinkedHashMap<Long, Long> copyTailProposals = new LinkedHashMap<Long, Long>();
	private final Map<Long, ZabCoinTossingHeader> queuedProposalMessage = Collections.synchronizedMap(new HashMap<Long, ZabCoinTossingHeader>());
	private final Map<MessageId, Message> messageStore = Collections.synchronizedMap(new HashMap<MessageId, Message>());
	//private ConcurrentMap<Long, Integer> followerACKs = new ConcurrentHashMap<Long, Integer>();

	private TreeMap<Double, Double> pW = new TreeMap<Double, Double>();
	protected volatile boolean                  running=true;
	private final static String outDir = "/work/ZabCoinTossing/";
	private static double percentRW = 0;
	private static long waitSentTime = 0;
	private ProtocolStats stats = new ProtocolStats();
	private int numABRecieved = 0;
	@Property(name = "ZabCoinTossing_size", description = "It is ZabCoinTossing cluster size")
	private final int N = 5;
	private final double d = (double) 27.575/1000;
	private final int theta = 2251;
	private int n=N-1;
	private double thDivByn= (double) theta/n;
	@Property(name = "tail_timeout", description = "pending Proposal timeout in Millisecond, before deliver")
	private AtomicInteger tailTimeout = new AtomicInteger(2000);
	private Timer timerForTail = new Timer();	

	private static int numReadCoundRecieved=0;
	private static int warmUp = 0;

	private Timer timer = new Timer();
	private ZUtil zUnit= new ZUtil(1.0);
	private final SortedSet<Timeout> timeouts= Collections.synchronizedSortedSet(new TreeSet<Timeout>());
	private final LinkedBlockingQueue<ZabCoinTossingHeader> processTimeout = new LinkedBlockingQueue<ZabCoinTossingHeader>();
	private final List<Long> delays = new ArrayList<Long>();
	//private final LinkedBlockingQueue<Long> sendACKToFollower = new LinkedBlockingQueue<Long>();
	private long latestZxidSeen=0;
	//private boolean ackedNextProposal=false;
	private static String info = null;
	private static int thshot = 9950;
	private Timer measureXd = new Timer();		
	private AtomicDouble propArrivalRate = new AtomicDouble(0.0);// Next arrival proposal time
	private String RuningProtocol = "ZabCT";
	private final String ZabCT = "ZabCT";
	private final String Zab= "Zab";






	public ZabCoinTossing(){

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
		latestZxidSeen = 0;
		//ackedNextProposal=false;

		//zUnit.setP(1.0);
		this.pW=this.stats.findpW(N, zabMembers.size());
		log.info("pW====="+this.pW);
		if(!is_leader){
			timerForTail.schedule(new TailTimeOutTask(), 5, 2000);//For tail proposal timeout
			measureXd.schedule(new MeasuePropArrivalRate(), 5, 1000);//For tail proposal timeout
			log.info("Starting Task");
			log.info("I am Follower");
		}
		else
			log.info("I am Leader");
		warmUp = queuedCommitMessage.size();
		log.info("Reset done");

	}

	@Override
	public void stop() {
		running=false;
		executor1.shutdown();
		executor2.shutdown();
		executor3ProcessAck.shutdown();
		//delayTimeout.shutdown();
		super.stop();
	}

	public int getn() {
		return n;
	}

	public void setn(int n){
		this.n=n;
	}


	public String getRuningProtocol() {
		return RuningProtocol;
	}

	public void setRuningProtocol(String runingProtocol) {
		RuningProtocol = runingProtocol;
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
		ZabCoinTossingHeader hdr;

		switch(evt.getType()) {
		case Event.MSG:            	
			msg=(Message)evt.getArg();
			hdr=(ZabCoinTossingHeader)msg.getHeader(this.id);
			if(hdr == null){
				break; // pass up
			}
			switch(hdr.getType()) {                
			case ZabCoinTossingHeader.REQUESTW:
				forwardToLeader(msg);
				break;
			case ZabCoinTossingHeader.REQUESTR:
				//log.info(">>>>>>>>>>>>>>>>>Receive read<<<<<<<<<<<<<<<<<<<");
				hdr.getMessageOrderInfo().getId().setStartTime(System.nanoTime());
				readData(hdr.getMessageOrderInfo());
				break;
			case ZabCoinTossingHeader.FORWARD:
				queuedMessages.add(hdr);
				break;
			case ZabCoinTossingHeader.PROPOSAL:
				stats.numProposal.incrementAndGet();
				sendACK(msg, hdr);
				break;          		
			case ZabCoinTossingHeader.ACK:
				ackToProcess.add(hdr.getZxid());
				break;
			case ZabCoinTossingHeader.STARTWORKLOAD:
				info = (String) msg.getObject();
				log.info("info=====----> "+info);

				String waitSTString = info.split(":")[0];
				waitSentTime = Long.parseLong(waitSTString);
				String r = info.split(":")[1];
				percentRW = Double.parseDouble(r);
				if (percentRW==0.0){
					thshot+=1000000;
				}
				else{
					thshot+=(1000000*percentRW);
				}
				//percentRW = (Double) msg.getObject();
				numberOfSenderInEachClient = Integer.parseInt(info.split(":")[2]);
				this.stats = new ProtocolStats(ProtocolName, 10,
						numberOfSenderInEachClient, outDir, false, info);
				stats.setStartThroughputTime(System.currentTimeMillis());
				stats.setLastNumReqDeliveredBefore(0);
				stats.setLastThroughputTime(System.currentTimeMillis());
				//timer.schedule(new Throughput(), 1000, 5000);
				reset();
				break;
			case ZabCoinTossingHeader.FINISHED:
				log.info("I Have notfied from Client----> "+msg.getSrc());
				//if (clientFinished.incrementAndGet() == 10) {
				running = false;
				timer.cancel();
				sendCountRead();
				log.info("Printing stats");
				//}
				break;
			case ZabCoinTossingHeader.COUNTMESSAGE:
				addCountReadToTotal(hdr);
				break;
				//			case ZabCoinTossingHeader.COUNTACK:
				//				addCountAckToTotal(hdr);
				//				break;     	
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
			}

			leader = zabMembers.get(0);
			if (leader.equals(local_addr)) {
				is_leader = true;
			}
			//else {
			//delayTimeout = Executors.newSingleThreadExecutor();
			//delayTimeout.execute(new ProcessACKDelay());
			//sendACKToF=Executors.newSingleThreadExecutor();
			//sendACKToF.execute(new SendToFollower(this.id));
			//}
			//for(Address add:zabMembers){
			//if(!is_leader && !add.equals(local_addr)){
			//otherFollower = add;
			//log.info("DDDDDDDDDDDDDDDotherFollower=DDDDDDDDDDDDDDDD"+otherFollower);
			//}
			//}
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
		ZabCoinTossingHeader hdrReq = (ZabCoinTossingHeader) msg.getHeader(this.id);
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
		ZabCoinTossingHeader hdrReq = (ZabCoinTossingHeader) msg.getHeader(this.id);
		ZabCoinTossingHeader hdr = new ZabCoinTossingHeader(ZabCoinTossingHeader.FORWARD, hdrReq.getMessageOrderInfo());
		Message forward_msg = new Message(target).putHeader(this.id, hdr);
		forward_msg.setBuffer(new byte[1000]);
		//forward_msg.setFlag(Message.Flag.DONT_BUNDLE);
		try {			
			//forward_msg.setFlag(Message.Flag.DONT_BUNDLE);
			down_prot.down(new Event(Event.MSG, forward_msg));
		} catch (Exception ex) {
			log.error("failed forwarding message to " + msg, ex);
		}

	}

	private synchronized void sendACK(Message msg, ZabCoinTossingHeader hrdAck){
		Proposal p;
		MessageOrderInfo msgInfo = hrdAck.getMessageOrderInfo();
		long zxidACK = msgInfo.getOrdering();
		//		if(stats.getnumReqDelivered()>=thshot){
		//			ackedNextProposal=true;
		//			log.info("Reach 9999999999999---?"+zxidACK);
		//		}
		latestZxidSeen=zxidACK;
		p = new Proposal();
		p.AckCount++; // Ack from leader
		p.setMessageOrderInfo(msgInfo);
		outstandingProposals.put(zxidACK, p);
		queuedProposalMessage.put(zxidACK, hrdAck);

		//if(followerACKs.containsKey(zxidACK)){
		//log.info("Received Ack, So will not send ACK for ----> "+zxidACK);
		//ackToProcess.add(zxidACK);
		//return;
		//}
		if (zUnit.SendAckOrNoSend()){// || ackedNextProposal) {
			ZabCoinTossingHeader hdrACK = new ZabCoinTossingHeader(ZabCoinTossingHeader.ACK, zxidACK);
			Message ackMessage = new Message().putHeader(this.id, hdrACK);
			//ackMessage.setFlag(Message.Flag.DONT_BUNDLE);
			try{
				for(Address addr:zabMembers){
					if (local_addr.equals(addr))
						continue;
					Message cpy = ackMessage.copy();
					cpy.setDest(addr);
					down_prot.down(new Event(Event.MSG, cpy));  
					//stats.countAck.incrementAndGet();
				}	
				ackToProcess.add(zxidACK);
			}catch(Exception ex) {
				log.error("failed proposing message to members");
			}    
		}
		else {
			//log.info("get Tail--->"+zxidACK);
			synchronized(tailProposal){
				tailProposal.put(zxidACK, (System.currentTimeMillis()+tailTimeout.get()));
			}
		}
	}

	private synchronized void processACK(long ackedzxid){
		Proposal p = null;
		if (lastZxidCommitted >= ackedzxid) {
			return;
		}
		p = outstandingProposals.get(ackedzxid);
		if (p == null) {  
			//followerACKs.put(ackedzxid,1);
			return;
		}
		p.AckCount++;
		//if (followerACKs.containsKey(ackedzxid)){
		//followerACKs.remove(ackedzxid);
		//}
		if (isQuorum(p.getAckCount())) {
			if (ackedzxid == lastZxidCommitted+1){
				synchronized(tailProposal){
					if (tailProposal.containsKey(ackedzxid)){
						tailProposal.remove(ackedzxid);
					}
				}
				outstandingProposals.remove(ackedzxid);	
				delivery.add(new ZabCoinTossingHeader(ZabCoinTossingHeader.DELIVER, ackedzxid));
				lastZxidCommitted = ackedzxid;
			} else {
				long zxidCommiting = lastZxidCommitted +1;
				for (long z = zxidCommiting; z < ackedzxid+1; z++){
					synchronized(tailProposal){
						if (tailProposal.containsKey(z)){
							tailProposal.remove(z);
						}
					}
				}
				for (long z = zxidCommiting; z < ackedzxid+1; z++){
					outstandingProposals.remove(z);
					delivery.add(new ZabCoinTossingHeader(ZabCoinTossingHeader.DELIVER, z));
					lastZxidCommitted = z;			
					//log.info("processACK------------> Delivery Zxid--->"+z);

				}
			}

		}


	}


	private void deliver(long committedZxid){
		MessageOrderInfo messageOrderInfo = null;
		ZabCoinTossingHeader hdrOrginal = queuedProposalMessage.get(committedZxid);
		if (hdrOrginal == null){
			log.info("****hdrOrginal is null ****");
			return;
		}
		messageOrderInfo = hdrOrginal.getMessageOrderInfo();
		queuedProposalMessage.remove(committedZxid);
		queuedCommitMessage.put(committedZxid, hdrOrginal);
		//if(hdrOrginal==null)
		//log.info("****hdrOrginal is null ****");
		stats.incnumReqDelivered();
		stats.setEndThroughputTime(System.currentTimeMillis());
		//log.info("Zxid=:"+committedZxid);
		if (requestQueue.contains(messageOrderInfo.getId())){
			//log.info("Inside if&&&&&&&&&&&&&&&&&&:");
			long startTime = hdrOrginal.getMessageOrderInfo().getId().getStartTime();
			long endTime = System.nanoTime();
			stats.addLatency((long) (endTime - startTime));
			sendOrderResponse(messageOrderInfo);
			requestQueue.remove(messageOrderInfo.getId());
		}

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
		ZabCoinTossingHeader hdrOrginal = null;
		//synchronized(queuedCommitMessage){
		hdrOrginal = queuedCommitMessage.get(messageInfo.getOrdering());
		//}

		if (hdrOrginal != null){
			hdrResponse = new CSInteractionHeader(CSInteractionHeader.RESPONSER, 
					messageInfo);
			readReplay = new Message(messageInfo.getId().getOriginator()).putHeader((short) 79, hdrResponse);
		}
		else{//Simulate return null if the requested data is not stored in Zab
			log.info(" Read null%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
			hdrResponse = new CSInteractionHeader(CSInteractionHeader.RESPONSER, messageInfo);
			readReplay = new Message(messageInfo.getId().getOriginator()).putHeader((short) 79, hdrResponse);
		}
		long startTime = messageInfo.getId().getStartTime();
		long endTime = System.nanoTime();
		stats.addReadLatency((endTime - startTime));
		messageInfo.getId().setStartTime(0);
		readReplay.setBuffer(new byte[1000]);
		stats.incnumReqDelivered();
		stats.setEndThroughputTime(System.currentTimeMillis());
		//readReplay.setFlag(Message.Flag.DONT_BUNDLE);
		down_prot.down(new Event(Event.MSG, readReplay));
	}

	private boolean isQuorum(int majority){
		return majority >= ((N/2) + 1)? true : false;
	}

	private void sendCountRead(){
		int writeOnly= queuedCommitMessage.size()-warmUp;
		int readOnly = stats.getnumReqDelivered() - writeOnly;
		ZabCoinTossingHeader readCount = new ZabCoinTossingHeader(ZabCoinTossingHeader.COUNTMESSAGE, readOnly);
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

	private synchronized void addCountReadToTotal(ZabCoinTossingHeader countReadHeader) {
		long readCount = countReadHeader.getZxid();
		stats.addToNumReqDelivered((int) readCount);
		numReadCoundRecieved++;
		if(numReadCoundRecieved==(zabMembers.size()-1)){
			stats.printProtocolStats(queuedCommitMessage.size(), N, (int) (percentRW*100), waitSentTime);	

		}

	}
	private synchronized void addTotalABMssages(ZabCoinTossingHeader carryCountMessageLeader) {
		long followerMsg = carryCountMessageLeader.getZxid();
		stats.addCountTotalMessagesFollowers((int) followerMsg);
		numABRecieved++;
		if(numABRecieved==zabMembers.size()-1){
			ZabCoinTossingHeader headertStats = new ZabCoinTossingHeader(ZabCoinTossingHeader.STATS);
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
			ZabCoinTossingHeader hdrReq = null;
			while (running) {
				try {
					hdrReq=queuedMessages.take();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				long new_zxid = getNewZxid();
				MessageOrderInfo messageOrderInfo = hdrReq.getMessageOrderInfo();
				messageOrderInfo.setOrdering(new_zxid);

				ZabCoinTossingHeader hdrProposal = new ZabCoinTossingHeader(ZabCoinTossingHeader.PROPOSAL, messageOrderInfo); 
				Message proposalMessage=new Message().putHeader(this.id, hdrProposal);

				proposalMessage.setSrc(local_addr);
				proposalMessage.setBuffer(new byte[1000]);
				//proposalMessage.setFlag(Message.Flag.DONT_BUNDLE);
				Proposal p = new Proposal();
				p.setMessageOrderInfo(hdrReq.getMessageOrderInfo());
				p.AckCount++;
				outstandingProposals.put(new_zxid, p);
				queuedProposalMessage.put(new_zxid, hdrProposal);

				try{
					for (Address address : zabMembers) {
						if(address.equals(leader))
							continue; 
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
			long ackedzxid = 0;
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
			ZabCoinTossingHeader hdrDelivery= null;
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

	class Rate extends TimerTask {

		public Rate() {

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


	class TailTimeOutTask extends TimerTask {
		public TailTimeOutTask() {
		}

		public void run() {
			ZabCoinTossingHeader hdrACK = null;
			Message ackMessage = null;
			if(!tailProposal.isEmpty()){
				synchronized(tailProposal){
					copyTailProposals= new LinkedHashMap<Long,Long>(tailProposal);
				}
				//stats.countTailTimeout.incrementAndGet();
				//log.info("Tail size: "+tailProposal.size());
				for (long zx:copyTailProposals.keySet()){
					long diffTime = copyTailProposals.get(zx) - System.currentTimeMillis();
					if (diffTime <= 0 ){//tail timeout elapses
						log.info("Procssing ACK casuing by timeout====>: "+zx);
						hdrACK = new ZabCoinTossingHeader(ZabCoinTossingHeader.ACK, zx);
						ackMessage = new Message(leader).putHeader(id, hdrACK);
						down_prot.down(new Event(Event.MSG, ackMessage)); 
						ackToProcess.add(zx);
						log.info("Send Ack timeout  for zxid="+zx);
						//log.info("count Tail timeout elapsed:="+stats.countTailTimeout.incrementAndGet());
						//sendACKToFollower.add(zx);
						synchronized(tailProposal){
							tailProposal.remove(zx);
						}
					}
					else{
						timerForTail.cancel();
						timerForTail = new Timer();
						timerForTail.schedule(new TailTimeOutTask(), 0, diffTime);  							
					}
					break;
				}
				copyTailProposals.clear();
			}



		}



	}

	class MeasuePropArrivalRate extends TimerTask {
		private int lastNumProposal=0;
		double c2p2=0; //store result of (((double) theta/n) * ((double) 1/numProposalPerec));
		double p=0.0;
		double dMuliPropArr=0.0;
		SortedSet<Double> ps = new TreeSet<Double>();
		DecimalFormat roundValue = new DecimalFormat("#.000");
		public MeasuePropArrivalRate() {

		}

		@Override
		public void run() {
			lastNumProposal = (stats.numProposal.get()-stats.lastNumProposal.get());
			ArrayList<Double> removedp = new ArrayList<Double>();
			TreeMap<Double, Double> copypW = new TreeMap<Double, Double>(pW);

			if (lastNumProposal!=0){
				propArrivalRate.set( ((double) 1/lastNumProposal));
				log.info("lastNumProposal="+lastNumProposal);
				c2p2 = findCondtion2Part2(lastNumProposal);
				c2p2= Double.parseDouble(roundValue.format(c2p2));
				dMuliPropArr = d*lastNumProposal;
				log.info("c2p2="+c2p2);
				log.info("dMuliPropArr="+dMuliPropArr);

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
					if (RuningProtocol.equals(ZabCT)){
						final Entry<Double, Double> largeKey = copypW.lastEntry();
						log.info("p-->W=:"+copypW);
						log.info("New p id ------------->=:"+largeKey.getKey());
						zUnit.setP(largeKey.getKey());
						log.info("p change to ---->"+zUnit.getP());
					}
					else{
						log.info("Must Change to ZabCT   ********************************");
						setRuningProtocol(ZabCT);
						final Entry<Double, Double> largeKey = copypW.lastEntry();
						log.info("p-->W=:"+copypW);
						log.info("New p id ------------->=:"+largeKey.getKey());
						zUnit.setP(largeKey.getKey());
						log.info("p change to ---->"+zUnit.getP());
					}
				}
				else{
					log.info("Must Change to Zab  $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
					setRuningProtocol(ZabCT);
				}

			}
			else{
				log.info("Must Change to Zab    $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
				setRuningProtocol(ZabCT);
			}
			stats.lastNumProposal.set(stats.numProposal.get());
		}
		public double findCondtion2Part2(int numProposalPerec){
			double c2p2=0.0;
			c2p2 = (((double) theta/n) * ((double) 1/numProposalPerec));
			return c2p2;
		}

	}

}
