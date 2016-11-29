package org.jgroups.protocols.jzookeeper.ZabCTvsABCast1;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
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
import org.jgroups.AnycastAddress;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.protocols.jzookeeper.ZUtil;
import org.jgroups.protocols.jzookeeper.ProtocolStats;
import org.jgroups.protocols.jzookeeper.ZabCTvsABCast1.InfiniteClients.TestHeader;
import org.jgroups.protocols.jzookeeper.ZabCTvsABCast1.CSInteractionHeader;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

/* 
 * Zab_3 (main approach ) is the same implementation as Zab_2.
 * Note that all the code and implementation are simaller to Zab_2, just we change probability 
 * parameter in ZUtil class from 1.0 10 0.5.
 * Also it has features of testing throughput, latency (in Nano), ant etc. 
 * When using testing, it provides warm up test before starting real test.
 */
public class ZabCoinTossing extends Protocol {
	private final static String ProtocolName = "ZabCoinTossing";
	private final static int numberOfSenderInEachClient = 3;
	protected final AtomicLong zxid = new AtomicLong(0);
	private ExecutorService executor1;
	private ExecutorService executor2;
	protected Address local_addr;
	protected volatile Address leader;
	protected volatile View view;
	protected volatile boolean is_leader = false;
	private List<Address> zabMembers = Collections.synchronizedList(new ArrayList<Address>());
	private long lastZxidCommitted = 0;
	private Map<Long, ZabCoinTossingHeader> queuedCommitMessage = new HashMap<Long, ZabCoinTossingHeader>();
	private final LinkedBlockingQueue<ZabCoinTossingHeader> queuedMessages = new LinkedBlockingQueue<ZabCoinTossingHeader>();
	private final LinkedBlockingQueue<ZabCoinTossingHeader> delivery = new LinkedBlockingQueue<ZabCoinTossingHeader>();

	private ConcurrentMap<Long, Proposal> outstandingProposals = new ConcurrentHashMap<Long, Proposal>();
	private final Map<Long, ZabCoinTossingHeader> queuedProposalMessage = Collections
			.synchronizedMap(new HashMap<Long, ZabCoinTossingHeader>());
	//private final Map<MessageId, Message> messageStore = Collections.synchronizedMap(new HashMap<MessageId, Message>());
	Calendar cal = Calendar.getInstance();
	protected volatile boolean running = true;
	SortedSet<Long> wantCommit = new TreeSet<Long>();
	private List<Integer> latencies = new ArrayList<Integer>();
	private List<Integer> avgLatencies = new ArrayList<Integer>();
	private List<String> avgLatenciesTimer = new ArrayList<String>();
	private final static String outDir = "/work/ZabCoinTossing/";
	private List<String> largeLatencies = new ArrayList<String>();
	private volatile boolean makeAllFollowersAck = false;

	public ZabCoinTossing() {

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
		log.setLevel("trace");
		running = true;
		executor1 = Executors.newSingleThreadExecutor();
		executor1.execute(new FollowerMessageHandler(this.id));
		// this.stats = new ProtocolStats(ProtocolName, clients.size(),
		// numberOfSenderInEachClient, outDir, false);
		// log.info("**** start protocol **** ");

	}

	public void reset() {
		zxid.set(0);
		lastZxidCommitted = 0;
		queuedCommitMessage.clear();
		queuedProposalMessage.clear();
		queuedMessages.clear();
		outstandingProposals.clear();
		//messageStore.clear();
		wantCommit.clear();
		latencies.clear();
		largeLatencies.clear();
		avgLatencies.clear();
		avgLatenciesTimer.clear();
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
			Message msg = (Message) evt.getArg();
			if (!msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER)) {
				handleClientRequest(msg);
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
		ZabCoinTossingHeader hdr;

		switch (evt.getType()) {
		case Event.MSG:
			msg = (Message) evt.getArg();
			hdr = (ZabCoinTossingHeader) msg.getHeader(this.id);
			if (hdr == null)
				break; // pass up
			switch (hdr.getType()) {
			case ZabCoinTossingHeader.RESET:
				reset();
				break;
			case ZabCoinTossingHeader.FORWARD:
				queuedMessages.add(hdr);
				break;
			case ZabCoinTossingHeader.PROPOSAL:

				sendACK(msg, hdr);
				break;
			case ZabCoinTossingHeader.ACK:
				processACK(msg, msg.getSrc());
				break;
			case ZabCoinTossingHeader.RESPONSE:
				handleOrderingResponse(hdr);

			}
			return null;

		case Event.VIEW_CHANGE:
			handleViewChange((View) evt.getArg());
			break;

		}

		return up_prot.up(evt);
	}

	/*
	 * ------------------ Private Methods -------------------
	 */

	private void handleClientRequest(Message message) {
		TestHeader clientHeader = ((TestHeader) message.getHeader((short) 1026));
		if (clientHeader != null && clientHeader.getType() == TestHeader.TEST_MSG) {
			MessageId msgId = new MessageId(local_addr, clientHeader.getSeq());
			msgId.setStartTime(clientHeader.getTimestamp());
		//	messageStore.put(msgId, message);
			ZabCoinTossingHeader zabCTHeader = new ZabCoinTossingHeader(ZabCoinTossingHeader.REQUEST, msgId);
			forwardToLeader(message, zabCTHeader);
		}

	}

	private void handleViewChange(View v) {
		this.view = v;
		List<Address> mbrs = v.getMembers();
		leader = mbrs.get(0);
		if (leader.equals(local_addr)) {
			is_leader = true;
		}
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

	private void forwardToLeader(Message msg, ZabCoinTossingHeader hdrCT) {
		forward(hdrCT);
	}

	private void forward(ZabCoinTossingHeader hdrCT) {
		try {
			ZabCoinTossingHeader hdr = new ZabCoinTossingHeader(ZabCoinTossingHeader.FORWARD, hdrCT.getMessageId());
			Message forward_msg = new Message(leader).putHeader(this.id, hdr);
			forward_msg.setBuffer(new byte[1000]);
			forward_msg.setFlag(Message.Flag.DONT_BUNDLE);
			down_prot.down(new Event(Event.MSG, forward_msg));
		} catch (Exception ex) {
			log.error("failed forwarding message to ", ex);
		}

	}

	private void sendACK(Message msg, ZabCoinTossingHeader hrdAck) {
		Proposal p;
		if (msg == null)
			return;
		if (hrdAck == null)
			return;

		p = new Proposal();
		p.AckCount++; // Ack from leader
		p.setZxid(hrdAck.getZxid());
		p.setMessage(msg);
		outstandingProposals.put(hrdAck.getZxid(), p);
		queuedProposalMessage.put(hrdAck.getZxid(), hrdAck);
		if (ZUtil.SendAckOrNoSend() || makeAllFollowersAck) {

			ZabCoinTossingHeader hdrACK = new ZabCoinTossingHeader(ZabCoinTossingHeader.ACK, hrdAck.getZxid());
			Message ackMessage = new Message().putHeader(this.id, hdrACK);
			try {
				for (Address address : zabMembers) {
					Message cpy = ackMessage.copy();
					cpy.setDest(address);
					down_prot.down(new Event(Event.MSG, cpy));
				}
			} catch (Exception ex) {
				log.error("failed proposing message to members");
			}
		}

	}

	private synchronized void processACK(Message msgACK, Address sender) {
		Proposal p = null;
		ZabCoinTossingHeader hdr = (ZabCoinTossingHeader) msgACK.getHeader(this.id);
		long ackZxid = hdr.getZxid();

		if (lastZxidCommitted >= ackZxid) {
			return;
		}
		p = outstandingProposals.get(ackZxid);
		if (p == null) {
			return;
		}

		p.AckCount++;
		if (isQuorum(p.getAckCount())) {
			if (ackZxid == lastZxidCommitted + 1) {
				outstandingProposals.remove(ackZxid);
				delivery.add(hdr);
				lastZxidCommitted = ackZxid;
			} else {
				long zxidCommiting = lastZxidCommitted + 1;
				for (long z = zxidCommiting; z < ackZxid + 1; z++) {
					outstandingProposals.remove(z);
					delivery.add(new ZabCoinTossingHeader(ZabCoinTossingHeader.DELIVER, z));
					lastZxidCommitted = z;
				}
			}

		}

	}

	private void commit(long zxidd) {

		ZabCoinTossingHeader hdrOrginal = null;
		hdrOrginal = queuedProposalMessage.get(zxidd);
		if (hdrOrginal == null) {
			return;
		}
		deliver(zxidd);

	}

	private void deliver(long committedZxid) {

		ZabCoinTossingHeader hdrOrginal = queuedProposalMessage.remove(committedZxid);
		queuedCommitMessage.put(committedZxid, hdrOrginal);

		// if (stats.getnumReqDelivered() > 999000) {
		// makeAllFollowersAck = true;
		// }
		// }
		if (hdrOrginal == null)
			log.info("****hdrOrginal is null ****");
		// log.info("****Deliever Zxid **** "+hdrOrginal.getZxid());

		ZabCoinTossingHeader hdrResponse = new ZabCoinTossingHeader(ZabCoinTossingHeader.RESPONSE, committedZxid,
				hdrOrginal.getMessageId());
		handleOrderingResponse(hdrResponse);

	}

	private void handleOrderingResponse(ZabCoinTossingHeader hdrResponse) {

		MessageOrderInfo msgInfo = new MessageOrderInfo(hdrResponse.getMessageId(), hdrResponse.getZxid());
		CSInteractionHeader response = new CSInteractionHeader(CSInteractionHeader.RESPONSE, msgInfo);
		Message msgResponse = new Message(hdrResponse.getMessageId()
				.getOriginator()).putHeader((short) 78, response);
		down_prot.down(new Event(Event.MSG, msgResponse));

	}

	private boolean isQuorum(int majority) {
		return majority >= ((zabMembers.size() / 2) + 1) ? true : false;
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

					hdrReq = queuedMessages.take();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				long new_zxid = getNewZxid();

				ZabCoinTossingHeader hdrProposal = new ZabCoinTossingHeader(ZabCoinTossingHeader.PROPOSAL, new_zxid,
						hdrReq.getMessageId());
				Message ProposalMessage = new Message().putHeader(this.id, hdrProposal);

				ProposalMessage.setSrc(local_addr);
				ProposalMessage.setBuffer(new byte[1000]);
				ProposalMessage.setFlag(Message.Flag.DONT_BUNDLE);
				Proposal p = new Proposal();
				p.setMessageId(hdrReq.getMessageId());
				p.setZxid(new_zxid);
				p.setMessage(ProposalMessage);
				p.AckCount++;
				outstandingProposals.put(new_zxid, p);
				queuedProposalMessage.put(new_zxid, hdrProposal);
				// log.info("R FROM="+hdrReq.getMessageId().getOriginator() + "
				// Zxid= "+new_zxid);

				try {
					for (Address address : zabMembers) {
						if (address.equals(leader))
							continue;

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
			deliverMessages();

		}

		private void deliverMessages() {
			ZabCoinTossingHeader hdrDelivery = null;
			while (true) {
				try {
					hdrDelivery = delivery.take();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				commit(hdrDelivery.getZxid());

			}
		}

	}

}
