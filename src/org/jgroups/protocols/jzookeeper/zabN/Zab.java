package org.jgroups.protocols.jzookeeper.zabN;

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
import java.util.Timer;
import java.util.TimerTask;
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
	private final static int numberOfSenderInEachClient = 25;
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
	private final LinkedBlockingQueue<Long> delivery = new LinkedBlockingQueue<Long>();
	private ConcurrentMap<Long, Proposal> outstandingProposals = new ConcurrentHashMap<Long, Proposal>();
	private final Map<MessageId, Message> messageStore = Collections
			.synchronizedMap(new HashMap<MessageId, Message>());
	private volatile boolean running = true;
	private volatile boolean startThroughput = false;
	private final static String outDir = "/work/Zab/";
	private ProtocolStats stats = new ProtocolStats();

	@Property(name = "Zab_size", description = "It is Zab cluster size")
	private Timer timer = new Timer();
	private int clusterSize = 3;
	//private Timer checkFinished = new Timer();	
	private static AtomicInteger clientFinished = new AtomicInteger();



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
				numberOfSenderInEachClient, outDir, false);
		//checkFinished.schedule(new CheckFinished(), 5, 10000);//For tail proposal timeout
		log.setLevel("trace");
	}
	/*
	 * Reset all protocol fields, reset invokes after warm up has finished, then callback the clients to start 
	 * main test
	 */
	public void reset(Address client) {
		zxid.set(0);
		lastZxidProposed = 0;
		lastZxidCommitted = 0;
		requestQueue.clear();
		queuedCommitMessage.clear();
		queuedProposalMessage.clear();
		queuedMessages.clear();
		outstandingProposals.clear();
		messageStore.clear();
		startThroughput = false;
		log.info("Reset done");

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
		ZabHeader hdr;

		switch (evt.getType()) {
		case Event.MSG:
			msg = (Message) evt.getArg();
			hdr = (ZabHeader) msg.getHeader(this.id);
			if (hdr == null) {
				break; // pass up
			}
			switch (hdr.getType()) {
			case ZabHeader.FINISHED:
				log.info("I Have notfied from Client----> "+msg.getSrc());
				if (clientFinished.incrementAndGet() == 10) {
					stats.printProtocolStats(queuedCommitMessage.size(), clusterSize);
					log.info("Printing stats");
					running = false;
					timer.cancel();
				}
				break;
			case ZabHeader.REQUEST:
				//log.info("Receive REQUEST --->");
				forwardToLeader(msg);
				break;
			case ZabHeader.RESET:
				reset(msg.getSrc());
				break;
			case ZabHeader.FORWARD:

				queuedMessages.add(hdr);
				break;
			case ZabHeader.PROPOSAL:
				if (!stats.isWarmup() && !startThroughput) {
					startThroughput = true;
					stats.setStartThroughputTime(System.currentTimeMillis());
					stats.setLastNumReqDeliveredBefore(0);
					stats.setLastThroughputTime(System.currentTimeMillis());
					timer.schedule(new Throughput(), 1000, 1000);
				}
				sendACK(msg, hdr);
				break;
			case ZabHeader.ACK:
				processACK(msg);
				break;
			case ZabHeader.COMMIT:
				delivery.add(hdr.getZxid());
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
		
		System.out.println("** view: " + v);
		
		if (mbrs.size() == (clusterSize+2)) {
			for (int i=2;i<mbrs.size();i++){
				zabMembers.add(mbrs.get(i));
			}
			leader = mbrs.get(2);
			if (leader.equals(local_addr)) {
				is_leader = true;
			}
			System.out.println("** zabMembers: " + zabMembers);
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
		if (is_leader && !startThroughput) {
			startThroughput = true;
			stats.setStartThroughputTime(System.currentTimeMillis());
			timer.schedule(new Throughput(), 1000, 1000);
		}
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
	private void forward(Message msg) {
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
	private  void sendACK(Message msg, ZabHeader hdrAck) {
		MessageOrderInfo messageOrderInfo = hdrAck.getMessageOrderInfo();
		queuedProposalMessage.put(messageOrderInfo.getOrdering(), hdrAck);
		ZabHeader hdrACK = new ZabHeader(ZabHeader.ACK, messageOrderInfo.getOrdering());
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
		if (zxidd != lastZxidCommitted + 1) {
			if (log.isDebugEnabled()){
				log.debug("delivering Zxid out of order "+zxidd + " should be "
						+ lastZxidCommitted + 1);
			}
		}
		ZabHeader hdrCommit = new ZabHeader(ZabHeader.COMMIT, zxidd);
		Message commitMessage = new Message().putHeader(this.id, hdrCommit);
		//.setFlag(Message.Flag.DONT_BUNDLE);;
		for (Address address : zabMembers) {
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
		ZabHeader hdrOrginal = queuedProposalMessage.remove(dZxid);
		//log.info("hdrOrginal zxid = " + hdrOrginal.getZxid());
		if(hdrOrginal==null){
			log.info("delivering zxid=" + dZxid+" Lastdelivered="+lastZxidCommitted);
		}
		messageOrderInfo = hdrOrginal.getMessageOrderInfo();
		queuedCommitMessage.put(dZxid, hdrOrginal);
		stats.incnumReqDelivered();
		stats.setEndThroughputTime(System.currentTimeMillis());
		//log.info("Zxid=:"+dZxid);
		if (requestQueue.contains(messageOrderInfo.getId())) {
			long startTime = hdrOrginal.getMessageOrderInfo().getId().getStartTime();
			long endTime = System.nanoTime();
			stats.addLatency((endTime - startTime));
			sendOrderResponse(messageOrderInfo);
			requestQueue.remove((messageOrderInfo.getId()));
		}
		synchronized (this) {
			lastZxidCommitted = dZxid;
		}
	}

	private void sendOrderResponse(MessageOrderInfo messageOrderInfo){
		CSInteractionHeader hdrResponse = new CSInteractionHeader(CSInteractionHeader.RESPONSE, messageOrderInfo);
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
			Long zxidDeliver = null;
			ArrayList<Long> tempZxid = new ArrayList<Long>();
			while(true){
				try {
					zxidDeliver= delivery.take();
					if (queuedProposalMessage.containsKey(zxidDeliver)) {
						//delivery.poll();
						deliver(zxidDeliver);
					}
					else{
						synchronized(delivery){
							tempZxid.addAll(delivery);
							delivery.add(zxidDeliver);
							delivery.addAll(tempZxid);
							tempZxid.clear();
						}
					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			
//			long zxidDeliver = 0;
//
//			while (true) {
//				try {
//					zxidDeliver = delivery.take();
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				}          
//				deliver(zxidDeliver);
//
//			}
			
			
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
