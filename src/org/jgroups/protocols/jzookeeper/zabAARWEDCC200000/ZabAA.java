package org.jgroups.protocols.jzookeeper.zabAARWEDCC200000;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.Timer;
import java.util.TimerTask;
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
import org.jgroups.protocols.jzookeeper.zabCT_AdaptationUsingWriteRatioSyncV3.ZabCTHeader;
import org.jgroups.stack.Protocol;


/* 
 * Zab_3 (main approach ) is the same implementation as Zab_2.
 * Note that all the code and implementation are simaller to Zab_2, just we change probability 
 * parameter in ZUtil class from 1.0 10 0.5.
 * Also it has features of testing throughput, latency (in Nano), ant etc. 
 * When using testing, it provides warm up test before starting real test.
 */
public class ZabAA extends Protocol {
	private final static String ProtocolName = "ZabAA";
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
	private Map<Long, ZabAAHeader> queuedCommitMessage = new HashMap<Long, ZabAAHeader>();
	private final LinkedBlockingQueue<ZabAAHeader> queuedMessages =
			new LinkedBlockingQueue<ZabAAHeader>();
	private final LinkedBlockingQueue<ZabAAHeader> delivery = new LinkedBlockingQueue<ZabAAHeader>();
	private final LinkedBlockingQueue<ACK> ackToProcess = new LinkedBlockingQueue<ACK>();
	private ConcurrentMap<Long, Proposal> outstandingProposals = new ConcurrentHashMap<Long, Proposal>();
	private Map<Long, Long> tailProposal = Collections.synchronizedMap(new LinkedHashMap<Long, Long>());
	private LinkedHashMap<Long, Long> copyTailProposals = new LinkedHashMap<Long, Long>();
	private final Map<Long, ZabAAHeader> queuedProposalMessage = Collections.synchronizedMap(new HashMap<Long, ZabAAHeader>());
	private final Map<MessageId, Message> messageStore = Collections.synchronizedMap(new HashMap<MessageId, Message>());
	//private ConcurrentMap<Long, Integer> followerACKs = new ConcurrentHashMap<Long, Integer>();

	protected volatile boolean                  running=true;
	private final static String outDir = "/work/ZabAA/";
	private static double percentRW = 0;
	private static long waitSentTime = 0;
	private ProtocolStats stats = new ProtocolStats();
	private int numABRecieved = 0;
	@Property(name = "ZabCoinTossing_size", description = "It is ZabCoinTossing cluster size")
	private int N = 9;
	@Property(name = "tail_timeout", description = "pending Proposal timeout in Millisecond, before deliver")
	private AtomicInteger tailTimeout = new AtomicInteger(2000);
	private Timer timerForTail = new Timer();	

	private static int numReadCoundRecieved=0;
	private static int warmUp = 0;

	private Timer timer = new Timer();
	private ZUtil zUnit= new ZUtil(1.0);
	private final List<Long> delays = new ArrayList<Long>();
	//private final LinkedBlockingQueue<Long> sendACKToFollower = new LinkedBlockingQueue<Long>();
	private long latestZxidSeen=0;
	//private boolean ackedNextProposal=false;
	private static String info = null;
	private final List<ACK> vector = new ArrayList<ACK>();
	private long latestZxidProposed=0; // From the leader
	private int majLargest=(N-1)/2 ; //  ((N-1)/2)-1 follower in ZabCT// (N-1)/2 follower in ZabCt
	private Timer changeCoinP = new Timer();	// change coin p when expereient nearly finish


	public ZabAA(){

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
		zUnit.setP(0.5); //Change to 0.5 to have ZabCT
		if(!is_leader){
			changeCoinP.schedule(new Changep(), 30000, 500);
			log.info("Starting Task");
			log.info("I am Follower");
		}
		else
			log.info("I am Leader");
		warmUp = queuedCommitMessage.size();
		//log.info("Vector size=:"+vector.size());
	//	log.info("Vector=: "+vector);
		//log.info("MajLargest=: "+majLargest);
		//log.info("Reset done");

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
		ZabAAHeader hdr;

		switch(evt.getType()) {
		case Event.MSG:            	
			msg=(Message)evt.getArg();
			hdr=(ZabAAHeader)msg.getHeader(this.id);
			if(hdr == null){
				break; // pass up
			}
			switch(hdr.getType()) {                
			case ZabAAHeader.REQUESTW:
				forwardToLeader(msg);
				break;
			case ZabAAHeader.REQUESTR:
				//log.info(">>>>>>>>>>>>>>>>>Receive read<<<<<<<<<<<<<<<<<<<");
				hdr.getMessageOrderInfo().getId().setStartTime(System.nanoTime());
				readData(hdr.getMessageOrderInfo());
				break;
			case ZabAAHeader.FORWARD:
				queuedMessages.add(hdr);
				break;
			case ZabAAHeader.PROPOSAL:
				sendACK(msg, hdr);
				break;          		
			case ZabAAHeader.ACK:
				//stats.countACK.incrementAndGet();
				ackToProcess.add(new ACK(msg.getSrc(), hdr.getZxid()));
				break;
			case ZabAAHeader.STARTWORKLOAD:
				info = (String) msg.getObject();
				log.info("info=====----> "+info);
				//percentRW = (Double) msg.getObject();
				numberOfSenderInEachClient = Integer.parseInt(info.split(":")[1]);
				this.stats = new ProtocolStats(ProtocolName, 10,
						numberOfSenderInEachClient, outDir, false, info);
				stats.setStartThroughputTime(System.currentTimeMillis());
				stats.setLastNumReqDeliveredBefore(0);
				stats.setLastThroughputTime(System.currentTimeMillis());
				timer.schedule(new Throughput(), 1030, 1000);
				reset();
				break;
			case ZabAAHeader.FINISHED:
				log.info("I Have notfied from Client----> "+msg.getSrc());
				//if (clientFinished.incrementAndGet() == 10) {
				running = false;
				//timer.cancel();
				sendCountRead();
				log.info("Printing stats");
				//}
				break;
			case ZabAAHeader.COUNTMESSAGE:
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
				majLargest = (N-1)/2 ;//For Leader  in ZabCT/ZabAA
			}
			else{
				vector.remove(new ACK(leader, 0));
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
		ZabAAHeader hdrReq = (ZabAAHeader) msg.getHeader(this.id);
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
		ZabAAHeader hdrReq = (ZabAAHeader) msg.getHeader(this.id);
		ZabAAHeader hdr = new ZabAAHeader(ZabAAHeader.FORWARD, hdrReq.getMessageOrderInfo());
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

	private synchronized void sendACK(Message msg, ZabAAHeader hrdAck){
		Proposal p;
		MessageOrderInfo msgInfo = hrdAck.getMessageOrderInfo();
		long zxidACK = msgInfo.getOrdering();

		p = new Proposal();
		p.setMessageOrderInfo(msgInfo);
		outstandingProposals.put(zxidACK, p);
		queuedProposalMessage.put(zxidACK, hrdAck);
		// For ZabAA with p=1
		//				ZabAAHeader hdrACK = new ZabAAHeader(ZabAAHeader.ACK, zxidACK);
		//				Message ackMessage = new Message().putHeader(this.id, hdrACK);
		//				try{
		//					for(Address addr:zabMembers){
		//						if (local_addr.equals(addr)){
		//				            ackToProcess.add(new ACK(local_addr, zxidACK));
		//							continue;
		//						}
		//						Message cpy = ackMessage.copy();
		//						cpy.setDest(addr);
		//						down_prot.down(new Event(Event.MSG, cpy));  
		//						//stats.countAck.incrementAndGet();
		//					}	
		//				}catch(Exception ex) {
		//					log.error("failed proposing message to members");
		//				}    

		ackToProcess.add(new ACK(local_addr, zxidACK));
		//For ZabCT with p=0.5
		if (zUnit.SendAckOrNoSend()){// || ackedNextProposal) {
			//stats.countACKPerBroadcast.incrementAndGet(); //For counting ACK broadcast per W%
			ZabAAHeader hdrACKCT = new ZabAAHeader(ZabAAHeader.ACK, zxidACK);
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



	}

	private synchronized void processACK(ACK ack){
		//log.info("Vector size=:"+vector);
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
				delivery.add(new ZabAAHeader(ZabAAHeader.DELIVER, z));
			}
		}
	}


	private void deliver(long committedZxid){
		MessageOrderInfo messageOrderInfo = null;
		ZabAAHeader hdrOrginal = queuedProposalMessage.get(committedZxid);
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
		ZabAAHeader hdrOrginal = null;

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
		long startTime = messageInfo.getId().getStartTime();
		long endTime = System.nanoTime();
		stats.addReadLatency((endTime - startTime));
		messageInfo.getId().setStartTime(0);
		readReplay.setBuffer(new byte[1000]);
		//stats.incnumReqDelivered();
		stats.setEndThroughputTime(System.currentTimeMillis());
		readReplay.setFlag(Message.Flag.DONT_BUNDLE);
		down_prot.down(new Event(Event.MSG, readReplay));
	}

	private boolean isQuorum(int majority){
		return majority >= ((N/2) + 1)? true : false;
	}

	private void sendCountRead(){
		int writeOnly= queuedCommitMessage.size()-warmUp;
		int readOnly = stats.getnumReqDelivered() - writeOnly;
		ZabAAHeader readCount = new ZabAAHeader(ZabAAHeader.COUNTMESSAGE, readOnly);
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

	private synchronized void addCountReadToTotal(ZabAAHeader countReadHeader) {
		long readCount = countReadHeader.getZxid();
		stats.addToNumReqDelivered((int) readCount);
		numReadCoundRecieved++;
		if(numReadCoundRecieved==(zabMembers.size()-1)){
			stats.printProtocolStats(queuedCommitMessage.size(), N, (int) ((Double.parseDouble(info.split(":")[0])*100)), waitSentTime);	
		}
	}
	private synchronized void addTotalABMssages(ZabAAHeader carryCountMessageLeader) {
		long followerMsg = carryCountMessageLeader.getZxid();
		stats.addCountTotalMessagesFollowers((int) followerMsg);
		numABRecieved++;
		if(numABRecieved==zabMembers.size()-1){
			ZabAAHeader headertStats = new ZabAAHeader(ZabAAHeader.STATS);
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
			ZabAAHeader hdrReq = null;
			while (running) {
				try {
					hdrReq=queuedMessages.take();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				long new_zxid = getNewZxid();
				MessageOrderInfo messageOrderInfo = hdrReq.getMessageOrderInfo();
				messageOrderInfo.setOrdering(new_zxid);

				ZabAAHeader hdrProposal = new ZabAAHeader(ZabAAHeader.PROPOSAL, messageOrderInfo); 
				Message proposalMessage=new Message().putHeader(this.id, hdrProposal);

				proposalMessage.setSrc(local_addr);
				proposalMessage.setBuffer(new byte[1000]);
				//proposalMessage.setFlag(Message.Flag.DONT_BUNDLE);
				Proposal p = new Proposal();
				p.setMessageOrderInfo(hdrReq.getMessageOrderInfo());
				//p.AckCount++;
				outstandingProposals.put(new_zxid, p);
				queuedProposalMessage.put(new_zxid, hdrProposal);
				ackToProcess.add(new ACK(local_addr, new_zxid)); //For ZabCt/ZabCT/ZabAA
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

	final class MessageHandler implements Runnable {
		@Override
		public void run() {
			deliverMessages();

		}

		private void deliverMessages() {
			ZabAAHeader hdrDelivery= null;
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


	class Changep extends TimerTask { //This to make the protocol not stop printing the result by cganging coin p tp 1
		private final long THROS = 209500;

		public Changep(){

		}

		@Override
		public void run() {
			if(lastZxidCommitted>=THROS){
				zUnit.setP(1.0);
				log.info("**Make p=1**/lastZxidCommitted="+lastZxidCommitted);
				changeCoinP.cancel();
			}
		}


	}


	//
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







}
