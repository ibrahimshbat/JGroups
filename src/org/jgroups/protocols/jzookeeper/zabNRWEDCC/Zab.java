package org.jgroups.protocols.jzookeeper.zabNRWEDCC;

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
import java.util.Set;
import java.util.SortedSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
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

public class Zab extends Protocol {
	private final static String ProtocolName = "Zab";
	private static int numberOfSenderInEachClient = 25;
	private final AtomicLong zxid = new AtomicLong(0);
	private ExecutorService executorProposal;
	private ExecutorService executorDelivery;
	private Address local_addr;
	private volatile Address leader;
	private volatile View view;
	private volatile boolean is_leader = false;
	private List<Address> zabMembers = Collections
			.synchronizedList(new ArrayList<Address>());
	private long lastZxidProposed = 0, lastZxidCommitted = 0;
	private final Set<MessageId> requestQueue = Collections
			.synchronizedSet(new HashSet<MessageId>());
	private final Map<Long, ZabHeader> queuedCommitMessage = Collections
			.synchronizedMap(new HashMap<Long, ZabHeader>());
	private final Map<Long, ZabHeader> queuedProposalMessage = Collections
			.synchronizedMap(new HashMap<Long, ZabHeader>());
	private final LinkedBlockingQueue<ZabHeader> queuedMessages = new LinkedBlockingQueue<ZabHeader>();
	private final LinkedBlockingQueue<MessageOrderInfo> delivery= new LinkedBlockingQueue<MessageOrderInfo>();
	private final LinkedBlockingQueue<Long> notDeliverable= new LinkedBlockingQueue<Long>();
	private final Set<Long> waitingToDeliver= Collections.synchronizedSet(new HashSet<Long>());
	private ConcurrentMap<Long, Proposal> outstandingProposals = new ConcurrentHashMap<Long, Proposal>();
	private final Map<MessageId, Message> messageStore = Collections
			.synchronizedMap(new HashMap<MessageId, Message>());
	private volatile boolean running = true;
	private volatile boolean startThroughput = false;
	private final static String outDir = "/work/Zab/";
	private static String info = null;
	private ProtocolStats stats = new ProtocolStats();
	@Property(name = "Zab_size", description = "It is Zab cluster size")
	private Timer timerCommit = new Timer();
	private Timer timer = new Timer();
	private int clusterSize = 3;
	private static int warmUp = 0;

	//private Timer checkFinished = new Timer();	
	private static int numReadCoundRecieved=0;
	//private double writeRatio= 0;
	private int latencyIndex = 0;
	private final List<Double> delays = new ArrayList<Double>(); //For Measuring d
	Map<Address, SortedSet<Timeout>> mapTimeouts = new HashMap<Address, SortedSet<Timeout>>(); //For Measuring d
	private final LinkedBlockingQueue<Timeout> processTimeout = new LinkedBlockingQueue<Timeout>();//For Measuring d
	private ExecutorService delayTimeout; //For Measuring d
	//private Timer measureXd = new Timer();		







	/*
	 * Empty constructor
	 */
	public Zab() {

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
		queuedProposalMessage.clear();
		queuedMessages.clear();
		outstandingProposals.clear();
		messageStore.clear();
		//startThroughput = false;
		warmUp = queuedCommitMessage.size();
		//executorDelivery.shutdown();
		log.info("Reset done"+" Time="+System.currentTimeMillis());
		//if(!is_leader){
		//measureXd.schedule(new MeasuePropArrivalRate(), 5, 1000);//For tail proposal timeout

		//}

	}

	@Override
	public void stop() {
		running = false;
		executorProposal.shutdown();
		executorDelivery.shutdown();
		delayTimeout.shutdown();

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
		ZabHeader hdr;

		switch (evt.getType()) {
		case Event.MSG:
			msg = (Message) evt.getArg();
			hdr = (ZabHeader) msg.getHeader(this.id);
			if (hdr == null) {
				break; // pass up
			}
			switch (hdr.getType()) {
			case ZabHeader.REQUESTW:
				forwardToLeader(msg);
				break;
			case ZabHeader.REQUESTR:
				//log.info(">>>>>>>>>>>>>>>>>Receive read<<<<<<<<<<<<<<<<<<<");
				hdr.getMessageOrderInfo().getId().setStartTime(System.nanoTime());
				readData(hdr.getMessageOrderInfo());
				break;
			case ZabHeader.FORWARD:
				queuedMessages.add(hdr);
				break;
			case ZabHeader.PROPOSAL:
				//stats.numProposal.incrementAndGet();
				sendACK(msg, hdr);
				break;
			case ZabHeader.ACK:
				processACK(msg);
				break;
			case ZabHeader.COMMIT:
				delivery.add(hdr.getMessageOrderInfo());
				stats.countCommitMessage.incrementAndGet();
				break;
			case ZabHeader.STARTWORKLOAD:
				info = (String) msg.getObject();
				numberOfSenderInEachClient = Integer.parseInt(info.split(":")[1]);
				this.stats = new ProtocolStats(ProtocolName, 10,
						numberOfSenderInEachClient, outDir, false, info);
				startThroughput = true;
				stats.setStartThroughputTime(System.currentTimeMillis());
				stats.setLastNumReqDeliveredBefore(0);
				stats.setLastThroughputTime(System.currentTimeMillis());
				timer.schedule(new Throughput(), 1030, 1000);
				timerCommit.schedule(new Commit(), 1030, 1000);
				reset();
				break;
			case ZabHeader.COUNTMESSAGE:
				addCountReadToTotal(hdr);
				break;
			case ZabHeader.FINISHED:
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
				zabMembers.add(mbrs.get(i));
			}
			leader = mbrs.get(2);
			if (leader.equals(local_addr)) {
				is_leader = true;

			}
		}
		if (mbrs.size() > (clusterSize+2) && zabMembers.isEmpty()) {
			for (int i = 2; i < mbrs.size(); i++) {
				zabMembers.add(mbrs.get(i));
				if(i>=(clusterSize+2))
					break;
			}
			leader = mbrs.get(2);
			if (leader.equals(local_addr)) {
				is_leader = true;
			}
		}
		log.info("zabMembers size = " + zabMembers);
		if (mbrs.isEmpty())
			return;

		if (view == null || view.compareTo(v) < 0)
			view = v;
		else
			return;
	}

	private long getNewZxid() {
		return zxid.incrementAndGet();
	}

	/*
	 * If this server is a leader put the request in queue for processing it.
	 * otherwise forwards request to the leader
	 */
	private void forwardToLeader(Message msg) {
		//stats.incNumRequest();
		ZabHeader hdrReq = (ZabHeader) msg.getHeader(this.id);
		requestQueue.add(hdrReq.getMessageOrderInfo().getId());
		if (is_leader) {
			long stp = System.nanoTime();
			hdrReq.getMessageOrderInfo().getId().setStartTime(stp);
			queuedMessages.add(hdrReq);
		} else {
			long stf = System.nanoTime();
			hdrReq.getMessageOrderInfo().getId().setStartTime(stf);
			forward(msg);
		}

	}
	/*
	 * Forward request to the leader
	 */
	private synchronized void forward(Message msg) {
		Address target = leader;
		ZabHeader hdrReq = (ZabHeader) msg.getHeader(this.id);
		ZabHeader hdr = new ZabHeader(ZabHeader.FORWARD, hdrReq.getMessageOrderInfo());
		Message forward_msg = new Message(target).putHeader(this.id, hdr);
		forward_msg.setBuffer(new byte[1000]);
		try {			
			//forward_msg.setFlag(Message.Flag.DONT_BUNDLE);
			down_prot.down(new Event(Event.MSG, forward_msg));
		} catch (Exception ex) {
			log.error("failed forwarding message to " + msg, ex);
		}

	}

	/*
	 * This method is invoked by follower. 
	 * Follower receives a proposal. This method generates ACK message and send it to the leader.
	 */
	private synchronized void sendACK(Message msg, ZabHeader hdrAck) {
		MessageOrderInfo messageOrderInfo = hdrAck.getMessageOrderInfo();
		long proZxid = messageOrderInfo.getOrdering();
		queuedProposalMessage.put(proZxid, hdrAck);

		ZabHeader hdrACK = new ZabHeader(ZabHeader.ACK, proZxid);
		Message ACKMessage = new Message(leader).putHeader(this.id, hdrACK);
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

		ZabHeader hdr = (ZabHeader) msgACK.getHeader(this.id);
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
	private void commit(long zxidd) {

		ZabHeader hdrCommit = new ZabHeader(ZabHeader.COMMIT, zxidd);
		Message commitMessage = new Message().putHeader(this.id, hdrCommit);
		//.setFlag(Message.Flag.DONT_BUNDLE);;
		ZabHeader hdrOrginal = queuedProposalMessage.get(zxidd);
		//log.info("hdrOrginal zxid = " + hdrOrginal.getZxid());
		MessageOrderInfo messageOrderInfo = hdrOrginal.getMessageOrderInfo();
		hdrCommit.setMessageOrderInfo(messageOrderInfo);


		for (Address address : zabMembers) {
			if(address.equals(leader)){
				delivery.add(hdrOrginal.getMessageOrderInfo());
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
	private void deliver(MessageOrderInfo info) {
		long ddZxid = info.getOrdering();
		ZabHeader hdrOrginal = queuedProposalMessage.remove(ddZxid);
		//log.info("hdrOrginal zxid = " + hdrOrginal.getZxid());
		//if(hdrOrginal!=null){
		// messageOrderInfo = hdrOrginal.getMessageOrderInfo();
		//}
		synchronized(queuedCommitMessage){
			queuedCommitMessage.put(ddZxid, hdrOrginal);
		}
		stats.incnumReqDelivered();
		stats.setEndThroughputTime(System.currentTimeMillis());
		//log.info("Zxid=:"+dZxid+" Time="+System.currentTimeMillis());
		//log.info("Zxid=:"+dZxid);
		//log.info("Zxid=:"+ddZxid);
		if (requestQueue.contains(info.getId())) {
			long startTime = info.getId().getStartTime();
			long endTime = System.nanoTime();
			stats.addLatency((endTime - startTime));
			sendOrderResponse(info);
			requestQueue.remove((info.getId()));
		}
		//synchronized (this) {
		lastZxidCommitted = ddZxid;
		//}
		//return true;
	}

	private synchronized void readData(MessageOrderInfo messageInfo){
		//log.info(" readData ");

		Message readReplay = null;
		CSInteractionHeader hdrResponse = null;
		ZabHeader hdrOrginal = null;
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
		ZabHeader readCount = new ZabHeader(
				ZabHeader.COUNTMESSAGE, readOnly);
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

	private synchronized void addCountReadToTotal(ZabHeader countReadHeader) {
		long readCount = countReadHeader.getZxid();
		stats.addToNumReqDelivered((int) readCount);
		numReadCoundRecieved++;
		if(numReadCoundRecieved==(zabMembers.size()-1)){
			stats.printProtocolStats(queuedCommitMessage.size(), clusterSize, (int) ((Double.parseDouble(info.split(":")[0])*100)), is_leader);			
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
			ZabHeader hdrReq = null;
			while (running) {

				try {
					hdrReq = queuedMessages.take();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				long new_zxid = getNewZxid();
				MessageOrderInfo messageOrderInfo = hdrReq.getMessageOrderInfo();
				messageOrderInfo.setOrdering(new_zxid);
				ZabHeader hdrProposal = new ZabHeader(ZabHeader.PROPOSAL,
						messageOrderInfo);
				Message proposalMessage = new Message().putHeader(this.id,
						hdrProposal);//.setFlag(Message.Flag.DONT_BUNDLE);
				proposalMessage.setBuffer(new byte[1000]);
				proposalMessage.setSrc(local_addr);
				Proposal p = new Proposal();
				p.setMessageOrderInfo(hdrReq.getMessageOrderInfo());

				p.AckCount++;
				outstandingProposals.put(new_zxid, p);
				queuedProposalMessage.put(new_zxid, hdrProposal);

				try {

					for (Address address : zabMembers) {
						if (address.equals(leader))
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
			MessageOrderInfo info = null;
			//ArrayList<Long> tempZxid = new ArrayList<Long>();
			boolean isDelivered= false;
			//log.info("Before While");
			while(true){
				try {
					info= delivery.take();
					deliver(info);
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

			class Commit extends TimerTask {
	
				public Commit() {
	
				}
	
				private long startTime = 0;
				private long currentTime = 0;
				private double currentCommit = 0;
				private int CurrentNumCommits = 0;
	
				@Override
				public void run() {
					startTime = stats.getLastCommitTime();
					currentTime = System.currentTimeMillis();
					CurrentNumCommits=stats.countCommitMessage.get();
					currentCommit = (((double)CurrentNumCommits - stats
							.getLastNumCommits()) / ((double)(currentTime - startTime)/1000.0));
					stats.setLastNumCommits(CurrentNumCommits);
					stats.setLastCommitTime(currentTime);
					stats.addCommit(currentCommit);
				}
	
				public String convertLongToTimeFormat(long time) {
					Date date = new Date(time);
					SimpleDateFormat longToTime = new SimpleDateFormat("HH:mm:ss.SSSZ");
					return longToTime.format(date);
				}
			}
	//		
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
	//			Timeout delayInfo= null;
	//			while (true) {
	//				try {
	//					delayInfo = processTimeout.take();
	//					String id = delayInfo.getZxid();
	//					sendBackTime = delayInfo.getSendBackTimeout();
	//					arriveTime =  delayInfo.getArriveTime();
	//					sendTime = delayInfo.getSendTime();
	//					arriveTimeForRoundTrip = delayInfo.getArriveTimeForRoundTrip();
	//					idleTime =sendBackTime-arriveTime;
	//					delay=(arriveTimeForRoundTrip-sendTime)-idleTime;
	//					//log.info("ST="+sendTime+"/AT="+arriveTime+"/SB="+sendBackTime+"/LAT="
	//					//+arriveTimeForRoundTrip+"/id="+idleTime+"/d="+delay+" /d/2="+((double) delay/2)+" /Addres-Zxid="+id);
	//					//For Measuring d
	//					stats.addDelays_d(((double)delay/2));//divide by 2 to get one round;
	//				} catch (InterruptedException e) {
	//					e.printStackTrace();
	//				}          
	//			}
	//		}
	//
	//	} 


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

	//	class MeasuePropArrivalRate extends TimerTask {
	//		private int lastNumProposal=0;
	//
	//		public MeasuePropArrivalRate() {
	//
	//		}
	//
	//		@Override
	//		public void run() {
	//			lastNumProposal = (stats.numProposal.get()-stats.lastNumProposal.get());
	//
	//			log.info("ArrivalRate=:"+lastNumProposal);
	//
	//			stats.lastNumProposal.set(stats.numProposal.get());
	//		}
	//
	//	}



}
