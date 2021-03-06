package org.jgroups.protocols.jzookeeper.zab1P;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
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
import java.util.concurrent.atomic.AtomicLong;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

/* 
 * Zab_1 is the same implementation as Zab_0, but the follower will commit locally. 
 * This called Zab with 2 phases, can only work in 3 ZK servers.
 * Also it has features of testing throuhput, latency (in Nano), ant etc. 
 * When using testing, it provides warm up test before starting real test.
 */

public class Zab2Phases extends Protocol {

	private final static String ProtocolName = "Zab2Phases";
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
	private long lastZxidCommitted = 0;
	private final Set<MessageId> requestQueue = Collections
			.synchronizedSet(new HashSet<MessageId>());
	private Map<Long, Zab2PhasesHeader> queuedCommitMessage =  Collections
			.synchronizedMap(new HashMap<Long, Zab2PhasesHeader>());
	private final Map<Long, Zab2PhasesHeader> queuedProposalMessage = Collections
			.synchronizedMap(new HashMap<Long, Zab2PhasesHeader>());
	private final LinkedBlockingQueue<Zab2PhasesHeader> queuedMessages = new LinkedBlockingQueue<Zab2PhasesHeader>();
	private final LinkedBlockingQueue<Long> delivery = new LinkedBlockingQueue<Long>();

	private ConcurrentMap<Long, Proposal> outstandingProposals = new ConcurrentHashMap<Long, Proposal>();
	private final Map<MessageId, Message> messageStore = Collections
			.synchronizedMap(new HashMap<MessageId, Message>());
	private int index = -1;
	private int clientFinished = 0;
	private int numABRecieved = 0;
	private volatile boolean running = true;
	private volatile boolean startThroughput = false;
	private final static String outDir = "/work/Zab2P/";
	private AtomicLong countMessageLeader = new AtomicLong(0);
	private long countMessageFollower = 0;
	private int clusterSize = 3;

	private ProtocolStats stats = new ProtocolStats();
	private Timer timer = new Timer();
	private Timer checkFinished = new Timer();	

	public Zab2Phases() {

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

	@ManagedOperation
	public String printStats() {
		return dumpStats().toString();
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
		checkFinished.schedule(new CheckFinished(), 5, 10000);//For tail proposal timeout
		log.setLevel("trace");
	}

	/*
	 * reset all protocol fields, reset invokes after warm up has finished, then
	 * callback the clients to start main test
	 */
	public void reset() {
		zxid.set(0);
		lastZxidCommitted = 0;
		requestQueue.clear();
		queuedCommitMessage.clear();
		queuedProposalMessage.clear();
		queuedMessages.clear();
		outstandingProposals.clear();
		messageStore.clear();
		startThroughput = false;
		countMessageLeader = new AtomicLong(0);
		countMessageFollower = 0;
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
		Zab2PhasesHeader hdr;

		switch (evt.getType()) {
		case Event.MSG:
			msg = (Message) evt.getArg();
			hdr = (Zab2PhasesHeader) msg.getHeader(this.id);
			if (hdr == null) {
				break; // pass up
			}
			switch (hdr.getType()) {
			case Zab2PhasesHeader.REQUEST:
				forwardToLeader(msg);
				break;
			case Zab2PhasesHeader.RESET:
				System.out.println("RESET");
				reset();
				break;
			case Zab2PhasesHeader.FORWARD:
				queuedMessages.add(hdr);
				break;
			case Zab2PhasesHeader.PROPOSAL:
				if (!stats.isWarmup() && !startThroughput) {
					startThroughput = true;
					stats.setStartThroughputTime(System.currentTimeMillis());
					stats.setLastNumReqDeliveredBefore(0);
					stats.setLastThroughputTime(System.currentTimeMillis());
					timer.schedule(new Throughput(), 1000, 1000);
				}
				sendACK(msg, hdr);
				break;
			case Zab2PhasesHeader.ACK:
					processACK(msg);
				break;
			}
			return null;
		case Event.VIEW_CHANGE:
			handleViewChange((View) evt.getArg());
			break;

		}

		return up_prot.up(evt);
	}

	/*
	 * --------------------------------- Private Methods-----------------------------------
	 */

	private void handleViewChange(View v) {
		List<Address> mbrs = v.getMembers();

		if (mbrs.size() == (clusterSize+2)) {
			for (int i=2;i<mbrs.size();i++){
				zabMembers.add(mbrs.get(i));
			}
			leader = zabMembers.get(0);
			if (leader.equals(local_addr)) {
				is_leader = true;
			}
		}
		if (mbrs.size() > (clusterSize+2) && zabMembers.isEmpty()) {
			for (int i = 2; i < mbrs.size(); i++) {
				zabMembers.add(mbrs.get(i));
				if ((zabMembers.size()>=clusterSize)){
					break;
				}
			}
			leader = zabMembers.get(0);
			if (leader.equals(local_addr)) {
				is_leader = true;
			}
		}

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
		Zab2PhasesHeader hdrReq = (Zab2PhasesHeader) msg.getHeader(this.id);
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
		Zab2PhasesHeader hdrReq = (Zab2PhasesHeader) msg.getHeader(this.id);
		Zab2PhasesHeader hdr = new Zab2PhasesHeader(
				Zab2PhasesHeader.FORWARD, hdrReq.getMessageOrderInfo());
		Message forward_msg = new Message(target).putHeader(this.id, hdr);//.setFlag(Message.Flag.DONT_BUNDLE);;
		forward_msg.setBuffer(new byte[1000]);
		try {
			down_prot.down(new Event(Event.MSG, forward_msg));
		} catch (Exception ex) {
			log.error("failed forwarding message to " + msg, ex);
		}

	}

	/*
	 * This method is invoked by follower. Follower receives a proposal. This
	 * method generates ACK message and send it to the leader and call commit
	 * message locally as it receives a majority.
	 */
	private synchronized void sendACK(Message msg, Zab2PhasesHeader hdrAck) {
		if (msg == null)
			return;

		if (hdrAck == null)
			return;

		MessageOrderInfo messageOrderInfo = hdrAck.getMessageOrderInfo();
		queuedProposalMessage.put(messageOrderInfo.getOrdering(), hdrAck);
		Zab2PhasesHeader hdrACK = new Zab2PhasesHeader(Zab2PhasesHeader.ACK, messageOrderInfo.getOrdering());
		Message ackMessage = new Message(leader).putHeader(this.id, hdrACK);//.setFlag(Message.Flag.DONT_BUNDLE);
		try {
			down_prot.down(new Event(Event.MSG, ackMessage));
		} catch (Exception ex) {
			log.error("failed sending ACK message to Leader");
		}
		delivery.add(messageOrderInfo.getOrdering());

	}

	/*
	 * This method is invoked by leader. It receives ACK message from a follower
	 * and check if a majority is reached for particular proposal.
	 */
	private synchronized void processACK(Message msgACK) {
		Zab2PhasesHeader hdr = (Zab2PhasesHeader) msgACK.getHeader(this.id);
		long ackZxid = hdr.getZxid();
		if (lastZxidCommitted >= ackZxid) {
			return;
		}

		Proposal p = outstandingProposals.get(ackZxid);
		if (p == null) {
			return;
		}
		p.AckCount++;
		outstandingProposals.remove(ackZxid);
		delivery.add(hdr.getZxid());
	}

	/*
	 * This method is invoked by leader and follower.
	 */
	private void commit(long zxidd) {

		if (zxidd != lastZxidCommitted + 1) {
			if (log.isDebugEnabled()){
				log.debug("delivering Zxid out of order "+zxidd + " should be "
						+ lastZxidCommitted + 1);
			}
		}
		synchronized (this) {
			lastZxidCommitted = zxidd;
		}
		deliver(zxidd);
	}

	/*
	 * Deliver the proposal locally and if the current server is the receiver of
	 * the request, replay to the client.
	 */
	private void deliver(long dZxid) {
		MessageOrderInfo messageOrderInfo = null;
		Zab2PhasesHeader hdrOrginal = queuedProposalMessage.remove(dZxid);
		if(hdrOrginal==null){
			log.info("****hdrOrginal is null ****");
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
	}

	/*
	 * ----------------------------- End of Private
	 * Methods--------------------------------
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
			Zab2PhasesHeader hdrReq = null;
			while (running) {

				try {
					hdrReq = queuedMessages.take();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				long new_zxid = getNewZxid();
				MessageOrderInfo messageOrderInfo = hdrReq.getMessageOrderInfo();
				messageOrderInfo.setOrdering(new_zxid);
				Zab2PhasesHeader hdrProposal = new Zab2PhasesHeader(Zab2PhasesHeader.PROPOSAL,
						messageOrderInfo);
				Message proposalMessage = new Message().putHeader(this.id,
						hdrProposal);//.setFlag(Message.Flag.DONT_BUNDLE);;
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
			long zxidDeliver = 0;
			while (true) {
				try {
					zxidDeliver = delivery.take();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}          
				commit(zxidDeliver);

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

	class CheckFinished extends TimerTask {
		private int workload=1000000;
		public CheckFinished() {
		}

		public void run() {
			if(lastZxidCommitted>=workload){
				stats.printProtocolStats(queuedCommitMessage.size(), clusterSize);
				log.info("Printing stats");
				checkFinished.cancel();
				timer.cancel();
			}

		}

	}


}
