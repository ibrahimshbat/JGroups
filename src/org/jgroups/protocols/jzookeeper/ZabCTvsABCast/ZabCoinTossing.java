package org.jgroups.protocols.jzookeeper.ZabCTvsABCast;

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
import org.jgroups.protocols.jzookeeper.ZabCTvsABCast.InfiniteClients.TestHeader;
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
	private final Map<MessageId, Message> messageStore = Collections.synchronizedMap(new HashMap<MessageId, Message>());
	Calendar cal = Calendar.getInstance();
	protected volatile boolean running = true;
	SortedSet<Long> wantCommit = new TreeSet<Long>();
	private List<Integer> latencies = new ArrayList<Integer>();
	private List<Integer> avgLatencies = new ArrayList<Integer>();
	private List<String> avgLatenciesTimer = new ArrayList<String>();
	private boolean startThroughput = false;
	private final static String outDir = "/work/ZabCoinTossing/";
	private Timer timerThro = new Timer();
	private AtomicLong countMessageLeader = new AtomicLong(0);
	private List<String> largeLatencies = new ArrayList<String>();
	private volatile boolean makeAllFollowersAck = false;
	private List<Address> clients = Collections.synchronizedList(new ArrayList<Address>());
	private ProtocolStats stats = new ProtocolStats();

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
		this.stats = new ProtocolStats(ProtocolName, clients.size(), numberOfSenderInEachClient, outDir, false);
		log.info("**** start protocol **** ");

	}

	public void reset() {
		zxid.set(0);
		lastZxidCommitted = 0;
		queuedCommitMessage.clear();
		queuedProposalMessage.clear();
		queuedMessages.clear();
		outstandingProposals.clear();
		messageStore.clear();
		wantCommit.clear();
		latencies.clear();
		largeLatencies.clear();
		countMessageLeader = new AtomicLong(0);
		avgLatencies.clear();
		avgLatenciesTimer.clear();
		//timerThro.schedule(new Throughput(), 1000, 1000);
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
			   //log.info("Received Message FORWARD "+msg);
				queuedMessages.add(hdr);
				break;
			case ZabCoinTossingHeader.PROPOSAL:
				if (!stats.isWarmup() && !startThroughput) {
					startThroughput = true;
					stats.setStartThroughputTime(System.currentTimeMillis());
					stats.setLastNumReqDeliveredBefore(0);
					stats.setLastThroughputTime(System.currentTimeMillis());
				}
				sendACK(msg, hdr);
				break;
			case ZabCoinTossingHeader.ACK:
				processACK(msg, msg.getSrc());
				break;
			case ZabCoinTossingHeader.STATS:
				stats.printProtocolStats(is_leader);
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
	 * ------------------ Private Methods  -------------------
	 */

	private void handleClientRequest(Message message) {
		TestHeader clientHeader = ((TestHeader) message.getHeader((short) 1026));
		if (clientHeader != null && clientHeader.getType() == TestHeader.TEST_MSG) {
			MessageId msgId = new MessageId(local_addr, clientHeader.getSeq());
	         messageStore.put(msgId, message);
			ZabCoinTossingHeader zabCTHeader = new ZabCoinTossingHeader(ZabCoinTossingHeader.REQUEST, msgId);
			forwardToLeader(message, zabCTHeader);

		}
		
		else if (clientHeader != null && clientHeader.getType() == TestHeader.STATS) {
			stats.printProtocolStats(is_leader);
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
		if (!startThroughput) {
			startThroughput = true;
			stats.setStartThroughputTime(System.currentTimeMillis());
		}
		//if (is_leader) {
			//hdrCT.getMessageId().setStartTime(System.nanoTime());
		//	queuedMessages.add(hdrCT);
		//} else {
			hdrCT.getMessageId().setStartTime(System.nanoTime());
			forward(hdrCT);
		//}

	}

	private void forward(ZabCoinTossingHeader hdrCT) {
		try {
			ZabCoinTossingHeader hdr = new ZabCoinTossingHeader(ZabCoinTossingHeader.FORWARD, hdrCT.getMessageId());
			Message forward_msg = new Message(leader).putHeader(this.id, hdr);
			forward_msg.setBuffer(new byte[1000]);
			//forward_msg.setFlag(Message.Flag.OOB);
			forward_msg.setFlag(Message.Flag.DONT_BUNDLE);
			//forward_msg.setFlag(Message.Flag.NO_TOTAL_ORDER);
			down_prot.down(new Event(Event.MSG, forward_msg));
		} catch (Exception ex) {
			log.error("failed forwarding message to ", ex);
		}

	}

	private void sendACK(Message msg, ZabCoinTossingHeader hrdAck) {
		if (!stats.isWarmup()) {
			stats.incNumRequest();
		}
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
		log.info("R FROM="+hrdAck.getMessageId().getOriginator() + " Zxid= "+hrdAck.getZxid());
		if (stats.isWarmup()) {
			ZabCoinTossingHeader hdrACK = new ZabCoinTossingHeader(ZabCoinTossingHeader.ACK, hrdAck.getZxid());
			Message ackMessage = new Message().putHeader(this.id, hdrACK);
			ackMessage.setFlag(Message.Flag.DONT_BUNDLE);

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

		else if (ZUtil.SendAckOrNoSend() || makeAllFollowersAck) {

			ZabCoinTossingHeader hdrACK = new ZabCoinTossingHeader(ZabCoinTossingHeader.ACK, hrdAck.getZxid());
			Message ackMessage = new Message().putHeader(this.id, hdrACK);
			try {
				for (Address address : zabMembers) {
					if (!stats.isWarmup() && !address.equals(local_addr)) {
						stats.incCountMessageFollower();
					}
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
		if (!stats.isWarmup()) {
			stats.incnumReqDelivered();
			stats.setEndThroughputTime(System.currentTimeMillis());

			if (stats.getnumReqDelivered() > 999000) {
				makeAllFollowersAck = true;
			}
		}
		if (hdrOrginal == null)
			log.info("****hdrOrginal is null ****");
		//log.info("****Deliever Zxid **** "+hdrOrginal.getZxid());
		if (messageStore.containsKey(hdrOrginal.getMessageId())) {
			if (!stats.isWarmup()) {
				long startTime = hdrOrginal.getMessageId().getStartTime();
				long endTime = System.nanoTime();
				stats.addLatency((long) (endTime - startTime));
			}
			ZabCoinTossingHeader hdrResponse = new ZabCoinTossingHeader(ZabCoinTossingHeader.RESPONSE, committedZxid,
					hdrOrginal.getMessageId());
			handleOrderingResponse(hdrResponse);
		}
		if(hdrOrginal.getZxid()==1000000){
			try {
				Thread.sleep(15000);
				stats.printProtocolStats(is_leader);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}

	}

	private void handleOrderingResponse(ZabCoinTossingHeader hdrResponse) {

		Message message = messageStore.get(hdrResponse.getMessageId());
		message.putHeader(this.id, hdrResponse);
		up_prot.up(new Event(Event.MSG, message));
		messageStore.remove(hdrResponse.getMessageId());

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
				if (!stats.isWarmup()) {
					stats.incNumRequest();
				}

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
				log.info("R FROM="+hdrReq.getMessageId().getOriginator() + " Zxid= "+new_zxid);

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
			currentThroughput = (((double) finishedThroughput - stats.getLastNumReqDeliveredBefore())
					/ ((double) (currentTime - startTime) / 1000.0));
			stats.setLastNumReqDeliveredBefore(finishedThroughput);
			stats.setLastThroughputTime(currentTime);
			stats.addThroughput(convertLongToTimeFormat(currentTime), currentThroughput);
		}

		public String convertLongToTimeFormat(long time) {
			Date date = new Date(time);
			SimpleDateFormat longToTime = new SimpleDateFormat("HH:mm:ss.SSSZ");
			return longToTime.format(date);
		}
	}

}
