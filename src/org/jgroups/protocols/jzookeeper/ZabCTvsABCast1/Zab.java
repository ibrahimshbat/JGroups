package org.jgroups.protocols.jzookeeper.ZabCTvsABCast1;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.jgroups.Address;
import org.jgroups.AnycastAddress;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.protocols.jzookeeper.ProtocolStats;
import org.jgroups.protocols.jzookeeper.ZabCTvsABCast.InfiniteClients.TestHeader;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

/*
 * It is orignal protocol of Apache Zookeeper. Also it has features of testing throuhput, latency (in Nano), ant etc. 
 * When using testing, it provides warm up test before starting real test.
 * @author Ibrahim EL-Sanosi
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
	private List<Address> zabMembers = Collections.synchronizedList(new ArrayList<Address>());
	private long lastZxidCommitted = 0;
	private Map<Long, ZabHeader> queuedCommitMessage = new HashMap<Long, ZabHeader>();
	private final Map<Long, ZabHeader> queuedProposalMessage = Collections
			.synchronizedMap(new HashMap<Long, ZabHeader>());
	private final LinkedBlockingQueue<ZabHeader> queuedMessages = new LinkedBlockingQueue<ZabHeader>();
	private final LinkedBlockingQueue<ZabHeader> delivery = new LinkedBlockingQueue<ZabHeader>();
	private ConcurrentMap<Long, Proposal> outstandingProposals = new ConcurrentHashMap<Long, Proposal>();
	private final Map<MessageId, Message> messageStore = Collections.synchronizedMap(new HashMap<MessageId, Message>());
	private int clientFinished = 0;
	private int numABRecieved = 0;
	private volatile boolean running = true;
	private volatile boolean startThroughput = false;
	private final static String outDir = "/work/Zab/";
	private AtomicLong countMessageLeader = new AtomicLong(0);
	private long countMessageFollower = 0;
	private List<Address> clients = Collections.synchronizedList(new ArrayList<Address>());
	private ProtocolStats stats = new ProtocolStats();

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
		this.stats = new ProtocolStats(ProtocolName, clients.size(), numberOfSenderInEachClient, outDir, false);

	}

	/*
	 * reset all protocol fields, reset invokes after warm up has finished, then
	 * callback the clients to start main test
	 */
	public void reset() {
		zxid.set(0);
		lastZxidCommitted = 0;
		queuedCommitMessage.clear();
		queuedProposalMessage.clear();
		queuedMessages.clear();
		outstandingProposals.clear();
		messageStore.clear();
		startThroughput = false;
		countMessageLeader = new AtomicLong(0);
		countMessageFollower = 0;

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
			if (m.getDest() instanceof AnycastAddress && !m.isFlagSet(Message.Flag.NO_TOTAL_ORDER)) {
				handleClientRequest(m);
				return null;
			} else {
				break;
			} // don't pass down
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
			case ZabHeader.START_SENDING:
				if (!zabMembers.contains(local_addr))
					return up_prot.up(new Event(Event.MSG, msg));
				break;
			case ZabHeader.RESET:
				reset();
				break;
			case ZabHeader.FORWARD:
				queuedMessages.add(hdr);
				break;
			case ZabHeader.PROPOSAL:
				if (!is_leader) {
					if (!stats.isWarmup() && !startThroughput) {
						startThroughput = true;
						stats.setStartThroughputTime(System.currentTimeMillis());
						stats.setLastNumReqDeliveredBefore(0);
						stats.setLastThroughputTime(System.currentTimeMillis());
					}
					sendACK(msg, hdr);
				}
				break;
			case ZabHeader.ACK:
				if (is_leader) {
					processACK(msg, msg.getSrc());
				}
				break;
			case ZabHeader.COMMIT:
				delivery.add(hdr);
				break;
			case ZabHeader.STATS:
				stats.printProtocolStats(is_leader);
				break;
			case ZabHeader.RESPONSE:
				handleOrderingResponse(hdr);

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
			if (msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER) || msg.isFlagSet(Message.Flag.OOB)
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
	 * --------------------------------
	 */

	/*
	 * Handling all client requests, processing them according to request type
	 */
	private synchronized void handleClientRequest(Message message) {
		TestHeader clientHeader = ((TestHeader) message.getHeader((short) 1026));
		if (clientHeader != null) {
			MessageId msgId = new MessageId(local_addr, clientHeader.getSeq());
			messageStore.put(msgId, message);
			ZabHeader zabHeader = new ZabHeader(ZabHeader.REQUEST, msgId);
			forwardToLeader(message, zabHeader);

		}

	}

	private void handleViewChange(View v) {
		List<Address> mbrs = v.getMembers();
		leader = mbrs.get(0);
		if (leader.equals(local_addr)) {
			is_leader = true;
		}
		// make the first three joined server as ZK servers
		if (mbrs.size() == 3) {
			zabMembers.addAll(v.getMembers());
			if (zabMembers.contains(local_addr)) {
				executor2 = Executors.newSingleThreadExecutor();
				executor2.execute(new MessageHandler());
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
	private void forwardToLeader(Message msg, ZabHeader hdrCT) {

		if (!startThroughput) {
			startThroughput = true;
			stats.setStartThroughputTime(System.currentTimeMillis());
		}
		if (is_leader) {
			hdrCT.getMessageId().setStartTime(System.nanoTime());
			queuedMessages.add(hdrCT);
		} else {
			hdrCT.getMessageId().setStartTime(System.nanoTime());
			forward(hdrCT);
		}

	}

	/*
	 * Forward request to the leader
	 */
	private void forward(ZabHeader hdrCT) {
		try {
			ZabHeader hdr = new ZabHeader(ZabHeader.FORWARD, hdrCT.getMessageId());
			Message forward_msg = new Message(leader).putHeader(this.id, hdr);
			forward_msg.setBuffer(new byte[1000]);
			down_prot.down(new Event(Event.MSG, forward_msg));
		} catch (Exception ex) {
			log.error("failed forwarding message to ", ex);
		}

	}

	/*
	 * This method is invoked by follower. Follower receives a proposal. This
	 * method generates ACK message and send it to the leader.
	 */
	private void sendACK(Message msg, ZabHeader hdrAck) {
		if (!stats.isWarmup()) {
			stats.incNumRequest();
		}
		if (msg == null)
			return;

		if (hdrAck == null)
			return;

		queuedProposalMessage.put(hdrAck.getZxid(), hdrAck);
		ZabHeader hdrACK = new ZabHeader(ZabHeader.ACK, hdrAck.getZxid(), hdrAck.getMessageId());
		Message ACKMessage = new Message(leader).putHeader(this.id, hdrACK);
		if (!stats.isWarmup()) {
			countMessageFollower++;
			stats.incCountMessageFollower();
		}
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
	private synchronized void processACK(Message msgACK, Address sender) {

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
		}

	}

	/*
	 * This method is invoked by leader. It sends COMMIT message to all follower
	 * and itself.
	 */
	private void commit(long zxidd) {
		if (zxidd != lastZxidCommitted + 1) {
			if (log.isDebugEnabled()) {
				log.debug("delivering Zxid out of order " + zxidd + " should be " + lastZxidCommitted + 1);
			}
		}
		synchronized (this) {
			lastZxidCommitted = zxidd;
		}
		ZabHeader hdrCommit = new ZabHeader(ZabHeader.COMMIT, zxidd);
		Message commitMessage = new Message().putHeader(this.id, hdrCommit);
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
		ZabHeader hdrOrginal = queuedProposalMessage.remove(dZxid);
		queuedCommitMessage.put(dZxid, hdrOrginal);
		if (!stats.isWarmup()) {
			stats.incnumReqDelivered();
			stats.setEndThroughputTime(System.currentTimeMillis());
		}
		if (hdrOrginal == null)
			log.info("****hdrOrginal is null ****");

		if (messageStore.containsKey(hdrOrginal.getMessageId())) {
			if (!stats.isWarmup()) {
				long startTime = hdrOrginal.getMessageId().getStartTime();
				long endTime = System.nanoTime();
				stats.addLatency((endTime - startTime));
			}
			ZabHeader hdrResponse = new ZabHeader(ZabHeader.RESPONSE, dZxid, hdrOrginal.getMessageId());
			handleOrderingResponse(hdrResponse);

		}

	}

	/*
	 * Send replay to client
	 */
	private void handleOrderingResponse(ZabHeader hdrResponse) {
		Message message = messageStore.get(hdrResponse.getMessageId());
		message.putHeader(this.id, hdrResponse);
		up_prot.up(new Event(Event.MSG, message));
		messageStore.remove(hdrResponse.getMessageId());

	}

	/*
	 * Check a majority
	 */
	private boolean isQuorum(int majority) {
		return majority >= ((zabMembers.size() / 2) + 1) ? true : false;
	}
	/*
	 * ----------------------------- End of Private
	 * Methods-------------------------
	 */

	final class FollowerMessageHandler implements Runnable {

		private short id;

		public FollowerMessageHandler(short id) {
			this.id = id;
		}

		/**
		 * create a proposal and send it out to all the member
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
				if (!stats.isWarmup()) {
					stats.incNumRequest();
				}
				ZabHeader hdrProposal = new ZabHeader(ZabHeader.PROPOSAL, new_zxid, hdrReq.getMessageId());
				Message ProposalMessage = new Message().putHeader(this.id, hdrProposal);

				ProposalMessage.setSrc(local_addr);
				ProposalMessage.setBuffer(new byte[1000]);
				Proposal p = new Proposal();
				p.setMessageId(hdrReq.getMessageId());
				p.setZxid(new_zxid);
				p.setMessage(ProposalMessage);
				p.AckCount++;
				outstandingProposals.put(new_zxid, p);
				queuedProposalMessage.put(new_zxid, hdrProposal);

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
			// if(is_leader)
			log.info("call deliverMessages()");
			deliverMessages();

		}

		private void deliverMessages() {
			ZabHeader hdrDelivery = null;
			while (true) {
				try {
					hdrDelivery = delivery.take();
					// log.info("(deliverMessages) deliver zxid = "+
					// hdrDelivery.getZxid());
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				// log.info("(going to call deliver zxid = "+
				// hdrDelivery.getZxid());
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
			// if (running){
			startTime = stats.getLastThroughputTime();
			currentTime = System.currentTimeMillis();
			finishedThroughput = stats.getnumReqDelivered();
			// log.info("Start Time="+startTime);
			// log.info("currentTime Time="+currentTime);
			// elpasedThroughputTimeInSecond = (int) (TimeUnit.MILLISECONDS
			// .toSeconds(currentThroughput - startTime));
			// log.info("elpasedThroughputTimeInSecond
			// Time="+elpasedThroughputTimeInSecond);
			// log.info("finishedThroughput="+finishedThroughput);
			// log.info("stats.getLastNumReqDeliveredBefore()="+stats
			// .getLastNumReqDeliveredBefore());
			currentThroughput = (((double) finishedThroughput - stats.getLastNumReqDeliveredBefore())
					/ ((double) (currentTime - startTime) / 1000.0));
			stats.setLastNumReqDeliveredBefore(finishedThroughput);
			stats.setLastThroughputTime(currentTime);
			stats.addThroughput(convertLongToTimeFormat(currentTime), currentThroughput);
			// }
		}

		public String convertLongToTimeFormat(long time) {
			Date date = new Date(time);
			SimpleDateFormat longToTime = new SimpleDateFormat("HH:mm:ss.SSSZ");
			return longToTime.format(date);
		}
	}

}