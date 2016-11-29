package org.jgroups.protocols.jzookeeper.testrw;

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
import org.jgroups.Message.Flag;
import org.jgroups.View;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

/*
 * It is  Zookeeper Atomic Broadcast. Also it has features of testing throughput, latency, and etc. 
 * When using testing, it provides warm up test before starting real test.
 */

public class Zab extends Protocol {
	private final static String ProtocolName = "Zab";
	private final static int numberOfSenderInEachClient = 10;
	private final AtomicLong zxid = new AtomicLong(0);
	private ExecutorService executor1;
	private ExecutorService executor2;
	private Address local_addr;
	private volatile Address leader;
	private volatile View view;
	private volatile boolean is_leader = false;
	private List<Address> zabMembers = Collections
			.synchronizedList(new ArrayList<Address>());
	private long lastZxidCommitted = 0;
	private final Set<MessageId> requestQueue = Collections
			.synchronizedSet(new HashSet<MessageId>());
	private Map<Long, Proposal> queuedCommitMessage = new HashMap<Long, Proposal>();
	private final Map<Long, Proposal> queuedProposalMessage = Collections
			.synchronizedMap(new HashMap<Long, Proposal>()); // Emulate the
																// log, logging
																// the Proposal
	private final LinkedBlockingQueue<MessageInfo> queuedMessages = new LinkedBlockingQueue<MessageInfo>();
	private final LinkedBlockingQueue<MessageInfo> delivery = new LinkedBlockingQueue<MessageInfo>();
	private ConcurrentMap<Long, Proposal> outstandingProposals = new ConcurrentHashMap<Long, Proposal>();
	private final Map<MessageId, Message> messageStore = Collections
			.synchronizedMap(new HashMap<MessageId, Message>());
	private int index = -1;
	private int clientFinished = 0;
	private int numABRecieved = 0;
	private volatile boolean running = true;
	private volatile boolean startThroughput = false;
	private final static String outDir = "/work/Zab/";
	private AtomicLong countMessageLeader = new AtomicLong(0);
	private long countMessageFollower = 0;
	private List<Address> clients = Collections
			.synchronizedList(new ArrayList<Address>());
	private ProtocolStats stats = new ProtocolStats();
	private Timer timer = new Timer();

	@Property(name = "Zab_size", description = "It is Zab cluster size")
	private int clusterSize = 3;

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

	@ManagedOperation
	public String printStats() {
		return dumpStats().toString();
	}

	@Override
	public void start() throws Exception {
		super.start();
		running = true;
		executor1 = Executors.newSingleThreadExecutor();
		executor1.execute(new FollowerMessageHandler(this.id));
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
		countMessageLeader = new AtomicLong(0);// timer.cancel();
		countMessageFollower = 0;
		timer.schedule(new Throughput(), 1000, 1000);
		this.stats = new ProtocolStats(ProtocolName, clients.size(),
				numberOfSenderInEachClient, outDir, false);
		log.info("Reset done");

	}

	@Override
	public void stop() {
		running = false;
		executor1.shutdown();
		executor2.shutdown();
		super.stop();
	}

	public Object down(Event evt) {
		switch (evt.getType()) {
		case Event.MSG:
			Message m = (Message) evt.getArg();
			ZabHeader zh = (ZabHeader) m.getHeader(this.id);
			if (zh instanceof ZabHeader) {
				handleClientRequest(m);
				return null;
			} else {
				break;
			}
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
			case ZabHeader.RESET:
				reset();
				break;
			case ZabHeader.REQUESTW:
				forwardToLeader(msg);
				break;
			case ZabHeader.REQUESTR:
				//log.info("Read Request");
				stats.incNumRequest();
				hdr.getMessageInfo().getId().setStartTime(System.nanoTime());
				readData(hdr.getMessageInfo());
				break;
			case ZabHeader.FORWARD:
				queuedMessages.add(hdr.getMessageInfo());
				break;
			case ZabHeader.PROPOSAL:
				if (!stats.isWarmup() && !startThroughput) {
					startThroughput = true;
					stats.setStartThroughputTime(System.currentTimeMillis());
					stats.setLastNumReqDeliveredBefore(0);
					stats.setLastThroughputTime(System.currentTimeMillis());
				}
				sendACK(msg, hdr);
				break;
			case ZabHeader.ACK:
					processACK(msg);
				break;
			case ZabHeader.COMMIT:
				delivery.add(hdr.getMessageInfo());
				break;
			case ZabHeader.CLIENTFINISHED:
				clientFinished++;
				if (clientFinished == clients.size() && !is_leader) {
					running = false;
					timer.cancel();
					sendMyTotalBroadcast();
				}
				break;
			case ZabHeader.STATS:
				stats.printProtocolStats(is_leader);
				break;
			case ZabHeader.COUNTMESSAGE:
				addTotalABMssages(hdr);
				log.info("Yes, I recieved count request");
				break;
			case ZabHeader.SENDMYADDRESS:
				if (!zabMembers.contains(msg.getSrc())) {
					clients.add(msg.getSrc());
					System.out.println("Rceived clients address "
							+ msg.getSrc());
				}
				break;
			case ZabHeader.RESPONSEW:
				handleWriteResponse(hdr);
				break;
			case ZabHeader.RESPONSER:
				handleReadResponse(msg);
				break;
			}
			return null;
		case Event.VIEW_CHANGE:
			handleViewChange((View) evt.getArg());
			break;

		}

		return up_prot.up(evt);
	}

	public void up(MessageBatch batch) {
		for (Message msg : batch) {
			if (msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER)
					|| msg.isFlagSet(Message.Flag.OOB)
					|| msg.getHeader(id) == null)
				continue;
			batch.remove(msg);

			try {
				up(new Event(Event.MSG, msg));
			} catch (Throwable t) {
				log.error("failed passing up message", t);
			}
		}

		if (!batch.isEmpty())
			up_prot.up(batch);
	}

	/*
	 * --------------------------------- Private Methods
	 * --------------------------
	 */

	/*
	 * Handling all client requests, processing them according to request type
	 */
	private synchronized void handleClientRequest(Message message) {
		ZabHeader clientHeader = ((ZabHeader) message.getHeader(this.id));

		if (clientHeader != null && clientHeader.getType() == ZabHeader.RESET) {

			for (Address server : zabMembers) {
				Message resetMessage = new Message(server).putHeader(this.id,
						clientHeader);
				resetMessage.setSrc(local_addr);
				// resetMessage.setFlag(Flag.DONT_BUNDLE);
				down_prot.down(new Event(Event.MSG, resetMessage));
			}
		}

		else if (clientHeader != null
				&& clientHeader.getType() == ZabHeader.STATS) {

			for (Address server : zabMembers) {
				Message statsMessage = new Message(server).putHeader(this.id,
						clientHeader);
				// statsMessage.setFlag(Flag.DONT_BUNDLE);
				down_prot.down(new Event(Event.MSG, statsMessage));
			}
		}

		else if (clientHeader != null
				&& clientHeader.getType() == ZabHeader.CLIENTFINISHED) {
			for (Address server : zabMembers) {
				Message countMessages = new Message(server).putHeader(this.id,
						clientHeader);
				// countMessages.setFlag(Flag.DONT_BUNDLE);
				down_prot.down(new Event(Event.MSG, countMessages));
			}

		}

		else if (!clientHeader.getMessageInfo().equals(null)
				&& (clientHeader.getType() == ZabHeader.REQUESTR  ||
				    clientHeader.getType() == ZabHeader.REQUESTW)) {
			Address destination = null;
			++index;
			if (index > (clusterSize - 1))
				index = 0;
			destination = zabMembers.get(index);
			Message requestMessage = new Message(destination).putHeader(
					this.id, clientHeader);
			if(clientHeader.getType() == ZabHeader.REQUESTW){
				requestMessage.setBuffer(new byte[1000]);
			    messageStore.put(clientHeader.getMessageInfo().getId(), message);
			}
			// requestMessage.setFlag(Flag.DONT_BUNDLE);
			down_prot.down(new Event(Event.MSG, requestMessage));
		}

		else if (!clientHeader.getMessageInfo().equals(null)
				&& clientHeader.getType() == ZabHeader.SENDMYADDRESS) {
			log.info("ZabMemberSize = " + zabMembers.size());
			for (Address server : zabMembers) {
				log.info("server address = " + server);
				message.dest(server);
				message.src(message.getSrc());
				// message.setFlag(Flag.DONT_BUNDLE);
				down_prot.down(new Event(Event.MSG, message));
			}
		}

	}

	private void handleViewChange(View v) {
		List<Address> mbrs = v.getMembers();
		leader = mbrs.get(0);
		if (leader.equals(local_addr)) {
			is_leader = true;
		}
		// make the first three joined servers as ZooKeeper servers
		if (mbrs.size() == clusterSize) {
			zabMembers.addAll(v.getMembers());
			if (zabMembers.contains(local_addr)) {
				executor2 = Executors.newSingleThreadExecutor();
				executor2.execute(new MessageHandler());
			}

		}
		if (mbrs.size() > clusterSize && zabMembers.isEmpty()) {
			for (int i = 0; i < clusterSize; i++) {
				zabMembers.add(mbrs.get(i));
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
	 * If this node is a leader put the request in queue for processing it.
	 * otherwise forwards request to the leader
	 */
	private void forwardToLeader(Message msg) {
		ZabHeader hdrReq = (ZabHeader) msg.getHeader(this.id);
		requestQueue.add(hdrReq.getMessageInfo().getId());
		if (!stats.isWarmup() && is_leader && !startThroughput) {
			startThroughput = true;
			stats.setStartThroughputTime(System.currentTimeMillis());
		}

		if (is_leader) {
			if (!stats.isWarmup()) {
				long stp = System.nanoTime();
				hdrReq.getMessageInfo().getId().setStartTime(stp);
			}
			queuedMessages.add(hdrReq.getMessageInfo());
		} else {
			if (!stats.isWarmup()) {
				long stf = System.nanoTime();
				hdrReq.getMessageInfo().getId().setStartTime(stf);
			}
			forward(msg);
		}

	}

	/*
	 * Forward request to the leader
	 */
	private void forward(Message msg) {
		Address target = leader;
		ZabHeader hdrReq = (ZabHeader) msg.getHeader(this.id);
		try {
			ZabHeader hdr = new ZabHeader(ZabHeader.FORWARD,
					hdrReq.getMessageInfo());
			Message forward_msg = new Message(target).putHeader(this.id, hdr);
			forward_msg.setBuffer(new byte[1000]);
			// forward_msg.setFlag(Flag.DONT_BUNDLE);
			down_prot.down(new Event(Event.MSG, forward_msg));
		} catch (Exception ex) {
			log.error("failed forwarding message to " + msg, ex);
		}

	}

	/*
	 * This method is invoked by follower. Follower receives a proposal. This
	 * method generates ACK message and send it to the leader.
	 */
	private void sendACK(Message msg, ZabHeader hdrAck) {
		Proposal proposal = hdrAck.getProposal();
		if (msg == null)
			return;

		if (hdrAck == null)
			return;
		queuedProposalMessage.put(proposal.getZxid(), proposal); // Emulate the log,
																// logging the
																// Proposal,
																// instead will
																// use disk in
																// future
		MessageInfo messageInfo = new MessageInfo(proposal.getZxid());
		ZabHeader hdrACK = new ZabHeader(ZabHeader.ACK, messageInfo);
		Message ackMessage = new Message(leader).putHeader(this.id, hdrACK);
		// ackMessage.setFlag(Flag.DONT_BUNDLE);
		if (!stats.isWarmup()) {
			countMessageFollower++;
			stats.incCountMessageFollower();
		}
		try {
			down_prot.down(new Event(Event.MSG, ackMessage));
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
		MessageInfo messageInfo = hdr.getMessageInfo();
		long ackZxid = messageInfo.getZxid();
		if (lastZxidCommitted >= ackZxid) {
			return;
		}
		Proposal p = outstandingProposals.get(ackZxid);
		if (p == null) {
			return;
		}
		p.ackCount++;
		if (isQuorum(p.getackCount())) {
			outstandingProposals.remove(ackZxid);
			commit(messageInfo);
		}

	}

	/*
	 * This method is invoked by leader. It sends COMMIT message to all
	 * followers and itself.
	 */
	private void commit(MessageInfo messageInfo) {
		if (messageInfo.getZxid() != lastZxidCommitted + 1) {
			if (log.isDebugEnabled()) {
				log.debug("delivering Zxid out of order " + messageInfo.getZxid()
						+ " should be " + lastZxidCommitted + 1);
			}
		}
		synchronized (this) {
			lastZxidCommitted = messageInfo.getZxid();
		}
		ZabHeader hdrCommit = new ZabHeader(ZabHeader.COMMIT, messageInfo);
		Message commitMessage = new Message().putHeader(this.id, hdrCommit);
		// commitMessage.setFlag(Flag.DONT_BUNDLE);
		for (Address address : zabMembers) {
			if (!address.equals(leader) && !stats.isWarmup()) {
				countMessageLeader.incrementAndGet();
				stats.incCountMessageLeader();
			}
			Message cpy = commitMessage.copy();
			cpy.setDest(address);
			down_prot.down(new Event(Event.MSG, cpy));

		}

	}

	/*
	 * Deliver the proposal locally and if the current server is the receiver of
	 * the request, replay to the client.
	 */
	private void deliver(long dZxid) {
		Proposal proposal = queuedProposalMessage.remove(dZxid);
		queuedCommitMessage.put(dZxid, proposal);
		if (!stats.isWarmup()) {
			stats.incnumReqDelivered();
			stats.setEndThroughputTime(System.currentTimeMillis());
		}

		if (proposal == null) {
			if (log.isInfoEnabled())
				log.info("****proposal is null ****");
		}
		//log.info("Deliver Zxid: " + dZxid);
		if (requestQueue.contains(proposal.getMessageId())) {
			if (!stats.isWarmup()) {
				long startTime = proposal.getMessageId().getStartTime();
				long endTime = System.nanoTime();
				stats.addWriteLatency((endTime - startTime));

			}
			ZabHeader hdrResponse = new ZabHeader(ZabHeader.RESPONSEW, new MessageInfo(proposal.getMessageId(), proposal.getZxid()));
			Message msgResponse = new Message(proposal.getMessageId()
					.getOriginator()).putHeader(this.id, hdrResponse);
			// msgResponse.setFlag(Flag.DONT_BUNDLE);
			down_prot.down(new Event(Event.MSG, msgResponse));
			requestQueue.remove(proposal.getMessageId());

		}

	}
	
	/*
	 * Read data and replay to the clients, Zab may return stall data.
	 */
	private synchronized void readData(MessageInfo messageInfo){
		Message readReplay = null;
		ZabHeader readReplayHdr = null;
		Proposal proposal = queuedCommitMessage.get(messageInfo.getZxid());
		if (proposal != null){
			readReplayHdr = new ZabHeader(ZabHeader.RESPONSER, new MessageInfo(proposal.getMessageId(), proposal.getZxid()));
			readReplay = new Message(messageInfo.getId().getOriginator()).putHeader(this.id, readReplayHdr);
			readReplay.setBuffer(new byte[1000]);
		}
		else{//Simulate return null if the requested data is not stored in Zab
			readReplayHdr = new ZabHeader(ZabHeader.RESPONSER);
			readReplay = new Message(messageInfo.getId().getOriginator()).putHeader(this.id, readReplayHdr);
		}
		long startTime = messageInfo.getId().getStartTime();
		long endTime = System.nanoTime();
		stats.addReadLatency((endTime - startTime));	
		stats.incnumReqDelivered();
		down_prot.down(new Event(Event.MSG, readReplay));
		
	}

	/*
	 * Send replay to client
	 */
	private synchronized void handleWriteResponse(ZabHeader hdrResponse) {
		Message message = messageStore.get(hdrResponse.getMessageInfo().getId());
		message.putHeader(this.id, hdrResponse);
		//log.info("handleWriteResponse zxid: "+hdrResponse.getMessageInfo().getZxid());
		up_prot.up(new Event(Event.MSG, message));
		messageStore.remove(hdrResponse.getMessageInfo().getId());

	}
	
	private synchronized void handleReadResponse(Message readResponse) {
		//ZabHeader hdr = (ZabHeader) readResponse.getHeader(this.id);
		//log.info("handleReadResponse zxid: "+hdr.getMessageInfo().getZxid());
		up_prot.up(new Event(Event.MSG, readResponse));
	}

	/*
	 * Check a majority
	 */
	private boolean isQuorum(int majority) {
		return majority >= ((clusterSize / 2) + 1) ? true : false;
	}

	private synchronized void addTotalABMssages(
			ZabHeader carryCountMessageLeader) {
		long followerMsg = carryCountMessageLeader.getMessageInfo().getZxid();
		stats.addCountTotalMessagesFollowers((int) followerMsg);
		numABRecieved++;
		if (numABRecieved == zabMembers.size() - 1) {
			ZabHeader headertStats = new ZabHeader(ZabHeader.STATS);
			for (Address zabServer : zabMembers) {
				Message messageStats = new Message(zabServer).putHeader(
						this.id, headertStats);
				// messageStats.setFlag(Flag.DONT_BUNDLE);
				down_prot.down(new Event(Event.MSG, messageStats));
			}
		}
	}

	private void sendMyTotalBroadcast() {
		ZabHeader followerMsgCount = new ZabHeader(ZabHeader.COUNTMESSAGE,
				new MessageInfo(null, countMessageFollower));
		Message requestMessage = new Message(leader).putHeader(this.id,
				followerMsgCount);
		// requestMessage.setFlag(Flag.DONT_BUNDLE);
		down_prot.down(new Event(Event.MSG, requestMessage));
	}

	/*
	 * ----------------------------- End of Private Methods
	 * --------------------------------
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
			MessageInfo messageInfo = null;
			while (running) {

				try {
					messageInfo = queuedMessages.take();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				long  new_zxid = getNewZxid();
				if (!stats.isWarmup()) {
					stats.incNumRequest();
				}
				Proposal proposal = new Proposal(new_zxid, messageInfo.getId());

				ZabHeader hdrProposal = new ZabHeader(ZabHeader.PROPOSAL,
						proposal);
				Message ProposalMessage = new Message().putHeader(this.id,
						hdrProposal);
				// ProposalMessage.setFlag(Flag.DONT_BUNDLE);
				ProposalMessage.setSrc(local_addr);
				ProposalMessage.setBuffer(new byte[1000]);
				proposal.ackCount++;
				outstandingProposals.put(new_zxid, proposal);
				queuedProposalMessage.put(new_zxid, proposal);

				try {

					for (Address address : zabMembers) {
						if (address.equals(leader))
							continue;
						if (!stats.isWarmup()) {
							countMessageLeader.incrementAndGet();
							stats.incCountMessageLeader();
						}
						Message cpy = ProposalMessage.copy();
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
			MessageInfo messageInfo = null;
			while (true) {
				try {
					messageInfo = delivery.take();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				deliver(messageInfo.getZxid());

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
			finishedThroughput = stats.getnumReqDelivered();
			currentThroughput = (((double) finishedThroughput - stats
					.getLastNumReqDeliveredBefore()) / ((double) (currentTime - startTime) / 1000.0));
			stats.setLastNumReqDeliveredBefore(finishedThroughput);
			stats.setLastThroughputTime(currentTime);
			stats.addThroughput(convertLongToTimeFormat(currentTime),
					currentThroughput);
		}

		public String convertLongToTimeFormat(long time) {
			Date date = new Date(time);
			SimpleDateFormat longToTime = new SimpleDateFormat("HH:mm:ss.SSSZ");
			return longToTime.format(date);
		}
	}

}