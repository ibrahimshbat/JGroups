package org.jgroups.protocols.jzookeeper.skeen;

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
import java.util.concurrent.atomic.AtomicLong;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.Message.Flag;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;

/*
 * It is orignal protocol of Apache Zookeeper. Also it has features of testing throuhput, latency (in Nano), ant etc. 
 * When using testing, it provides warm up test before starting real test.
 * @author Ibrahim EL-Sanosi
 */

public class Skeen extends Protocol {
	private LogicalClock logicalClock= new LogicalClock();
	private final static String ProtocolName = "Skeen";
	private static int numberOfSenderInEachClient = 25;
	private ExecutorService executorProposal;
	private ExecutorService executorDelivery;
	private Address local_addr;
	private volatile Address leader;
	private volatile View view;
	private volatile boolean is_leader = false;
	private List<Address> skeenMembers = Collections
			.synchronizedList(new ArrayList<Address>());
	private long lastZxidProposed = 0, lastZxidCommitted = 0;
	private final Set<MessageId> requestQueue = Collections
			.synchronizedSet(new HashSet<MessageId>());
	private final Map<MessageId, SkeenHeader> undeliveredMessages = Collections
			.synchronizedMap(new HashMap<MessageId, SkeenHeader>());
	private final Map<MessageId, SkeenHeader> deliveredMessages = Collections
			.synchronizedMap(new HashMap<MessageId, SkeenHeader>());
	private final LinkedBlockingQueue<SkeenHeader> queuedMessages = new LinkedBlockingQueue<SkeenHeader>();
	private final LinkedBlockingQueue<MessageOrderInfo> delivery= new LinkedBlockingQueue<MessageOrderInfo>();
	private final LinkedBlockingQueue<Long> notDeliverable= new LinkedBlockingQueue<Long>();
	private final Set<Long> waitingToDeliver= Collections.synchronizedSet(new HashSet<Long>());
	private ConcurrentMap<MessageId, Proposal> outstandingProposals = new ConcurrentHashMap<MessageId, Proposal>();
	private final Map<MessageId, Message> messageStore = Collections
			.synchronizedMap(new HashMap<MessageId, Message>());
	private volatile boolean running = true;
	private volatile boolean startThroughput = false;
	private final static String outDir = "/work/Zab/";
	private static String info = null;
	private ProtocolStats stats = new ProtocolStats();
	@Property(name = "Zab_size", description = "It is Zab cluster size")
	private Timer timer = new Timer();
	private int clusterSize = 7;
	private static int warmUp = 0;

	//private Timer checkFinished = new Timer();	
	private static int numReadCoundRecieved=0;






	/*
	 * Empty constructor
	 */
	public Skeen() {

	}

	@ManagedAttribute
	public boolean isleaderinator() {
		return is_leader;
	}

	public Address getleaderinator() {
		return leader;
	}

	public Address getLocalAddress() {
		return local_addr;
	}


	@Override
	public void start() throws Exception {
		super.start();
		running = true;
		executorProposal = Executors.newSingleThreadExecutor();
		executorProposal.execute(new FollowerMessageHandler(this.id));
		executorDelivery = Executors.newSingleThreadExecutor();
		executorDelivery.execute(new MessageHandler());
		this.stats = new ProtocolStats(ProtocolName, 10,
				numberOfSenderInEachClient, outDir, false, "");
		//checkFinished.schedule(new CheckFinished(), 5, 10000);//For tail proposal timeout
		log.setLevel("trace");
	}
	/*
	 * Reset all protocol fields, reset invokes after warm up has finished, then callback the clients to start 
	 * main test
	 */
	public void reset() {
		//zxid.set(0);
		//lastZxidProposed = 0;
		//lastZxidCommitted = 0;
		requestQueue.clear();
		//queuedCommitMessage.clear();
		queuedMessages.clear();
		outstandingProposals.clear();
		messageStore.clear();
		//startThroughput = false;
		warmUp = deliveredMessages.size();
		//executorDelivery.shutdown();
		log.info("Reset done"+" Time="+System.currentTimeMillis());

	}

	@Override
	public void stop() {
		running = false;
		executorProposal.shutdown();
		executorDelivery.shutdown();
		super.stop();
	}

	public Object down(Event evt) {
		switch (evt.getType()) {
		case Event.MSG:
			return null; // don't pass down
		case Event.SET_LOCAL_ADDRESS:
			local_addr = (Address) evt.getArg();
			break;
		}
		return down_prot.down(evt);
	}

	public Object up(Event evt) {
		Message msg = null;
		SkeenHeader hdr;

		switch (evt.getType()) {
		case Event.MSG:
			msg = (Message) evt.getArg();
			hdr = (SkeenHeader) msg.getHeader(this.id);
			if (hdr == null) {
				break; // pass up
			}
			switch (hdr.getType()) {
			case SkeenHeader.REQUESTW:
				handleRequest(msg);
				break;
			case SkeenHeader.REQUESTR:
				//log.info(">>>>>>>>>>>>>>>>>Receive read<<<<<<<<<<<<<<<<<<<");
				hdr.getMessageOrderInfo().getId().setStartTime(System.nanoTime());
				readData(hdr.getMessageOrderInfo());
				break;
			case SkeenHeader.FORWARD:
				queuedMessages.add(hdr);
				break;
			case SkeenHeader.PROPOSAL:
				sendACK(msg, hdr);
				break;
			case SkeenHeader.ACK:
				processACK(msg);
				break;
			case SkeenHeader.COMMIT:
				delivery.add(hdr.getZxid());
				break;
			case SkeenHeader.STARTWORKLOAD:
				info = (String) msg.getObject();
				numberOfSenderInEachClient = Integer.parseInt(info.split(":")[1]);
				this.stats = new ProtocolStats(ProtocolName, 10,
						numberOfSenderInEachClient, outDir, false, info);
				startThroughput = true;
				stats.setStartThroughputTime(System.currentTimeMillis());
				stats.setLastNumReqDeliveredBefore(0);
				stats.setLastThroughputTime(System.currentTimeMillis());
				timer.schedule(new Throughput(), 1000, 1000);
				reset();
				break;
			case SkeenHeader.COUNTMESSAGE:
				addCountReadToTotal(hdr);
				break;
			case SkeenHeader.FINISHED:
				log.info("I Have notfied from Client----> "+msg.getSrc());
				//if (clientFinished.incrementAndGet() == 10) {
				running = false;
				timer.cancel();
				sendCountRead();
				log.info("Printing stats");
				//}
				break;
			}
			//s
			return null;
		case Event.VIEW_CHANGE:
			handleViewChange((View) evt.getArg());
			break;

		}

		return up_prot.up(evt);
	}


	/*
	 * --------------------------------- Private Methods  --------------------------------
	 */



	private void handleViewChange(View v) {
		List<Address> mbrs = v.getMembers();
		//leader = mbrs.get(0);
		//make the first three joined server as ZK servers
		if (mbrs.size() == (clusterSize+2)) {
			for (int i=2;i<mbrs.size();i++){
				skeenMembers.add(mbrs.get(i));
			}
			leader = mbrs.get(2);
			if (leader.equals(local_addr)) {
				is_leader = true;
			}
		}
		if (mbrs.size() > (clusterSize+2) && skeenMembers.isEmpty()) {
			for (int i = 2; i < mbrs.size(); i++) {
				skeenMembers.add(mbrs.get(i));
				if(i>=(clusterSize+2))
					break;
			}
			leader = mbrs.get(2);
			if (leader.equals(local_addr)) {
				is_leader = true;
			}
		}
		log.info("skeenMembers size = " + skeenMembers);
		if (mbrs.isEmpty())
			return;

		if (view == null || view.compareTo(v) < 0)
			view = v;
		else
			return;
	}

	/*
	 * If this server is a leader put the request in queue for processing it.
	 * otherwise forwards request to the leader
	 */
	private synchronized void handleRequest(Message msg) {
		//stats.incNumRequest();
		SkeenHeader hdrReq = (SkeenHeader) msg.getHeader(this.id);
		requestQueue.add(hdrReq.getMessageOrderInfo().getId());
			long stp = System.nanoTime();
			hdrReq.getMessageOrderInfo().getId().setStartTime(stp);
			queuedMessages.add(hdrReq);
	}
	
	/*
	 * This method is invoked by follower. 
	 * Follower receives a proposal. This method generates ACK message and send it to the leader.
	 */
	private synchronized void sendACK(Message msg, SkeenHeader hdrAck) {
		Address initiator = msg.src();
		MessageOrderInfo messageOrderInfo = hdrAck.getMessageOrderInfo();
		MessageId msgId = messageOrderInfo.getId();
		long clock = messageOrderInfo.getOrdering();
		//log.info("Recieved Proposal Zxid= "+proZxid);
		undeliveredMessages.put(msgId, hdrAck);
		long clockToReplay = logicalClock.incLogicalClock(clock);
		messageOrderInfo.setOrdering(clockToReplay);
		SkeenHeader hdrACK = new SkeenHeader(SkeenHeader.ACK, messageOrderInfo);
		Message ACKMessage = new Message(initiator).putHeader(this.id, hdrACK);
		//.setFlag(Message.Flag.DONT_BUNDLE);
		try {
			down_prot.down(new Event(Event.MSG, ACKMessage));
		} catch (Exception ex) {
			log.error("failed sending ACK message to Leader");
		}
	}

	/*
	 * This method is invoked by leader. It receives ACK message from a follower
	 * and check if a majority is reached for particular proposal. 
	 */
	private synchronized void processACK(Message msgACK) {

		SkeenHeader hdr = (SkeenHeader) msgACK.getHeader(this.id);
		long ackZxid = hdr.getZxid();
		if (lastZxidCommitted >= ackZxid) {
			return;
		}
		Proposal p = outstandingProposals.get(ackZxid);
		if (p == null) {
			return;
		}
		p.AckCount++;
		if (isQuorum(p.getAckCount())) {
			outstandingProposals.remove(ackZxid);
			commit(ackZxid);
			//} 
		}

	}

	/*
	 * This method is invoked by leader. It sends COMMIT message to all follower and itself.
	 */
	private synchronized void commit(long zxidd) {
		//if (zxidd != lastZxidCommitted + 1) {
		//if (log.isDebugEnabled()){
		//log.debug("delivering Zxid out of order "+zxidd + " should be "
		//+ lastZxidCommitted + 1);
		//}
		//}
		SkeenHeader hdrCommit = new SkeenHeader(SkeenHeader.COMMIT, zxidd);
		Message commitMessage = new Message().putHeader(this.id, hdrCommit);
		//.setFlag(Message.Flag.DONT_BUNDLE);;
		for (Address address : skeenMembers) {
			if(address.equals(leader)){
				delivery.add(zxidd);
				continue;
			}
			Message cpy = commitMessage.copy();
			cpy.setDest(address);			
			down_prot.down(new Event(Event.MSG, cpy));
		}
	}

	/*
	 * Deliver the proposal locally and if the current server is the receiver of the request, 
	 * replay to the client.
	 */
	private void deliver(long dZxid) {
		MessageOrderInfo messageOrderInfo = null;
		SkeenHeader hdrOrginal = queuedProposalMessage.remove(dZxid);
		//log.info("hdrOrginal zxid = " + hdrOrginal.getZxid());
		if(hdrOrginal==null){
			//log.info("delivering zxid=" + dZxid+" Lastdelivered="+lastZxidCommitted);
			//			waitingToDeliver.add(dZxid);
			//			try {
			//				synchronized (delivery) {
			//					delivery.wait();
			//				}
			//			} catch (InterruptedException e) {
			//				// TODO Auto-generated catch block
			//				e.printStackTrace();
			//			}
			//			hdrOrginal = queuedProposalMessage.remove(dZxid);

			return;
		}
		messageOrderInfo = hdrOrginal.getMessageOrderInfo();
		queuedCommitMessage.put(dZxid, hdrOrginal);
		stats.incnumReqDelivered();
		stats.setEndThroughputTime(System.currentTimeMillis());
		//log.info("Zxid=:"+dZxid+" Time="+System.currentTimeMillis());
		if (requestQueue.contains(messageOrderInfo.getId())) {
			long startTime = hdrOrginal.getMessageOrderInfo().getId().getStartTime();
			long endTime = System.nanoTime();
			stats.addLatency((endTime - startTime));
			sendOrderResponse(messageOrderInfo);
			requestQueue.remove((messageOrderInfo.getId()));
		}
		//synchronized (this) {
		lastZxidCommitted = dZxid;
		//}
		//return true;
	}

	private synchronized void readData(MessageOrderInfo messageInfo){
		//log.info(" readData ");

		Message readReplay = null;
		CSInteractionHeader hdrResponse = null;
		SkeenHeader hdrOrginal = null;
		//synchronized(queuedCommitMessage){
		hdrOrginal = queuedCommitMessage.get(messageInfo.getOrdering());
		//}

		if (hdrOrginal != null){
			//log.info(" Find data==== "+hdrOrginal.getMessageOrderInfo().getOrdering());

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

	private void sendOrderResponse(MessageOrderInfo messageOrderInfo){
		CSInteractionHeader hdrResponse = new CSInteractionHeader(CSInteractionHeader.RESPONSEW, messageOrderInfo);
		Message msgResponse = new Message(messageOrderInfo.getId()
				.getOriginator()).putHeader((short) 79, hdrResponse);
		//msgResponse.setFlag(Message.Flag.DONT_BUNDLE);
		down_prot.down(new Event(Event.MSG, msgResponse));
	}


	/*
	 * Check a majority
	 */
	private boolean isQuorum(int majority) {
		return majority >= ((clusterSize / 2) + 1) ? true : false;
		//return majority >= (clusterSize) ? true : false;
	}

	private void sendCountRead(){
		int writeOnly= queuedCommitMessage.size()-warmUp;
		int readOnly = stats.getnumReqDelivered() - writeOnly;
		SkeenHeader readCount = new SkeenHeader(
				SkeenHeader.COUNTMESSAGE, readOnly);
		Message countRead = new Message(leader).putHeader(this.id,
				readCount);
		countRead.setFlag(Flag.DONT_BUNDLE);
		for (Address address : skeenMembers) {
			if (address.equals(local_addr))
				continue;
			Message cpy = countRead.copy();
			cpy.setDest(address);
			down_prot.down(new Event(Event.MSG, cpy));
		}
	}

	private synchronized void addCountReadToTotal(SkeenHeader countReadHeader) {
		long readCount = countReadHeader.getZxid();
		stats.addToNumReqDelivered((int) readCount);
		numReadCoundRecieved++;
		if(numReadCoundRecieved==(skeenMembers.size()-1)){
			stats.printProtocolStats(queuedCommitMessage.size(), clusterSize, (int) ((Double.parseDouble(info.split(":")[0])*100)));			
		}

	}


	/*
	 * ----------------------------- End of Private Methods --------------------------------
	 */

	final class FollowerMessageHandler implements Runnable {

		private short id;

		public FollowerMessageHandler(short id) {
			this.id = id;
		}

		/**
		 * create a proposal and send it out to all the members
		 * 
		 * @param message
		 */
		@Override
		public void run() {
			handleRequests();
		}

		public void handleRequests() {
			SkeenHeader hdrReq = null;
			while (running) {

				try {
					hdrReq = queuedMessages.take();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				long clock = logicalClock.getLogicalClock();
				logicalClock.incLogicalClock();
				MessageOrderInfo messageOrderInfo = hdrReq.getMessageOrderInfo();
				messageOrderInfo.setOrdering(clock);
				SkeenHeader hdrProposal = new SkeenHeader(SkeenHeader.PROPOSAL,
						messageOrderInfo);
				Message proposalMessage = new Message().putHeader(this.id,
						hdrProposal);//.setFlag(Message.Flag.DONT_BUNDLE);
				proposalMessage.setBuffer(new byte[1000]);
				proposalMessage.setSrc(local_addr);
				Proposal p = new Proposal();
				p.setMessageOrderInfo(hdrReq.getMessageOrderInfo());
				outstandingProposals.put(messageOrderInfo.getId(), p);
				undeliveredMessages.put(messageOrderInfo.getId(), hdrProposal);

				try {

					for (Address address : skeenMembers) {
						if (address.equals(local_addr))
							continue;
						Message cpy = proposalMessage.copy();
						cpy.setDest(address);
						down_prot.down(new Event(Event.MSG, cpy));
					}
				} catch (Exception ex) {
					log.error("failed proposing message to members");
				}

			}

		}

	}


	final class MessageHandler implements Runnable {
		@Override
		public void run() {
			log.info("call deliverMessages()");
			deliverMessages();
		}
		private void deliverMessages() {
			Long zxidDeliver = null;
			ArrayList<Long> tempZxid = new ArrayList<Long>();
			boolean isDelivered= false;
			//log.info("Before While");
			while(true){
				//log.info("Insider While");

				//				//try {
				try {
					zxidDeliver= delivery.take();
					//} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					//	e.printStackTrace();
					//}
					//				//log.info("delivery.peek()="+zxidDeliver);
					//				//if (queuedProposalMessage.containsKey(zxidDeliver)) {
					//				//delivery.poll();
					//isDelivered=deliver(zxidDeliver);
					synchronized(delivery){
						if(!queuedProposalMessage.containsKey(zxidDeliver)){
							//if (!isDelivered){
							tempZxid.addAll(delivery);
							delivery.clear();
							delivery.add(zxidDeliver);
							delivery.addAll(tempZxid);
							tempZxid.clear();
							//try {
							log.info("Before waiting for Zxid= "+zxidDeliver);
							delivery.wait();
							log.info("Afther waiting for Zxid= "+zxidDeliver);
						}
						else
							deliver(zxidDeliver);
					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
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

	//	class CheckFinished extends TimerTask {
	//		private int workload=1000000;
	//		public CheckFinished() {
	//		}
	//
	//		public void run() {
	//			if(lastZxidCommitted>=workload){
	//				stats.printProtocolStats(queuedCommitMessage.size(), clusterSize);
	//				log.info("Printing stats");
	//				checkFinished.cancel();
	//				timer.cancel();
	//			}
	//
	//		}
	//
	//	}


}
