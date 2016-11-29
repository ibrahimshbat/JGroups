package org.jgroups.protocols.jzookeeper.autocode;

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
import org.jgroups.protocols.jzookeeper.MessageId;
import org.jgroups.protocols.jzookeeper.Proposal;
import org.jgroups.protocols.jzookeeper.ProtocolStats;
import org.jgroups.protocols.jzookeeper.Zab2PhasesHeader;
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
	private Map<Long, Zab2PhasesHeader> queuedCommitMessage = new HashMap<Long, Zab2PhasesHeader>();
	private final Map<Long, Zab2PhasesHeader> queuedProposalMessage = Collections
			.synchronizedMap(new HashMap<Long, Zab2PhasesHeader>());
	private final LinkedBlockingQueue<Zab2PhasesHeader> queuedMessages = new LinkedBlockingQueue<Zab2PhasesHeader>();
	private final LinkedBlockingQueue<Zab2PhasesHeader> delivery = new LinkedBlockingQueue<Zab2PhasesHeader>();

	private ConcurrentMap<Long, Proposal> outstandingProposals = new ConcurrentHashMap<Long, Proposal>();
	private final Map<MessageId, Message> messageStore = Collections
			.synchronizedMap(new HashMap<MessageId, Message>());
	private int index = -1;
	private int clientFinished = 0;
	private int numABRecieved = 0;
	private volatile boolean running = true;
	private volatile boolean startThroughput = false;
	private final static String outDir = "/work/Zab2Phases/";
	private AtomicLong countMessageLeader = new AtomicLong(0);
	private long countMessageFollower = 0;
	private List<Address> clients = Collections
			.synchronizedList(new ArrayList<Address>());
	private ProtocolStats stats = new ProtocolStats();
	private Timer timer = new Timer();

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
		countMessageLeader = new AtomicLong(0);
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
			Zab2PhasesHeader zh=(Zab2PhasesHeader) m.getHeader(this.id);
			if (zh instanceof Zab2PhasesHeader){
				handleClientRequest(m);
				return null;
			}
			else{
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
			case Zab2PhasesHeader.ACK:
				if (is_leader) {
					processACK(msg);
				}
				break;
			case Zab2PhasesHeader.CLIENTFINISHED:
				clientFinished++;
				if(clientFinished==clients.size() && !is_leader){
					running = false;	
					timer.cancel();
					sendMyTotalBroadcast();			
				}
				break;
			case Zab2PhasesHeader.STATS:
				stats.printProtocolStats(is_leader);
				break;
			case Zab2PhasesHeader.COUNTMESSAGE:
				addTotalABMssages(hdr);
				log.info("Yes, I recieved count request");
				break;
			case Zab2PhasesHeader.SENDMYADDRESS:
				if (!zabMembers.contains(msg.getSrc())) {
					clients.add(msg.getSrc());
					System.out.println("Rceived clients address "
							+ msg.getSrc());
				}
				break;
			case Zab2PhasesHeader.RESPONSE:
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
	 * --------------------------------- Private Methods-----------------------------------
	 */

	/*
	 * Handling all client requests, processing them according to request type
	 */
	private synchronized void handleClientRequest(Message message) {
		Zab2PhasesHeader clientHeader = ((Zab2PhasesHeader) message
				.getHeader(this.id));

	   if (clientHeader != null
				&& clientHeader.getType() == Zab2PhasesHeader.RESET) {

			for (Address server : zabMembers) {
				Message resetMessage = new Message(server).putHeader(this.id,
						clientHeader);
				resetMessage.setSrc(local_addr);
				down_prot.down(new Event(Event.MSG, resetMessage));
			}
		}

		else if (clientHeader != null
				&& clientHeader.getType() == Zab2PhasesHeader.STATS) {

			for (Address server : zabMembers) {
				Message statsMessage = new Message(server).putHeader(this.id,
						clientHeader);
				down_prot.down(new Event(Event.MSG, statsMessage));
			}
		}

		else if (clientHeader != null
				&& clientHeader.getType() == Zab2PhasesHeader.CLIENTFINISHED) {
			for (Address server : zabMembers) {
					Message countMessages = new Message(server).putHeader(
							this.id, clientHeader);
					down_prot.down(new Event(Event.MSG, countMessages));
			}

		}

		else if (!clientHeader.getMessageId().equals(null)
				&& clientHeader.getType() == Zab2PhasesHeader.REQUEST) {
			Address destination = null;
			messageStore.put(clientHeader.getMessageId(), message);
			Zab2PhasesHeader hdrReq = new Zab2PhasesHeader(
					Zab2PhasesHeader.REQUEST, clientHeader.getMessageId());
			++index;
			if (index > 2)
				index = 0;
			destination = zabMembers.get(index);
			Message requestMessage = new Message(destination).putHeader(
					this.id, hdrReq);
			 requestMessage.setBuffer(new byte[1000]); // Necessary to ensure
			down_prot.down(new Event(Event.MSG, requestMessage));
		}

		else if (!clientHeader.getMessageId().equals(null)
				&& clientHeader.getType() == Zab2PhasesHeader.SENDMYADDRESS) {
			log.info("ZabMemberSize = " + zabMembers.size());
			for (Address server : zabMembers) {
				log.info("server address = " + server);
				message.dest(server);
				message.src(message.getSrc());
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
		if (mbrs.size() == 3) {
			zabMembers.addAll(v.getMembers());
			if(zabMembers.contains(local_addr)){
				executor2 = Executors.newSingleThreadExecutor();
		        executor2.execute(new MessageHandler());
			}

		}
		if (mbrs.size() > 3 && zabMembers.isEmpty()) {
			for (int i = 0; i < 3; i++) {
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
	 * If this server is a leader put the request in queue for processing it.
	 * otherwise forwards request to the leader
	 */
	private void forwardToLeader(Message msg) {
		Zab2PhasesHeader hdrReq = (Zab2PhasesHeader) msg.getHeader(this.id);
		requestQueue.add(hdrReq.getMessageId());
		if (!stats.isWarmup() && is_leader && !startThroughput) {
			startThroughput = true;
			stats.setStartThroughputTime(System.currentTimeMillis());
		}

		if (is_leader) {
			if (!stats.isWarmup()) {
				long stp = System.nanoTime();
				hdrReq.getMessageId().setStartTime(stp);
			}
			queuedMessages.add(hdrReq);
		} else {
			if (!stats.isWarmup()) {
				long stf = System.nanoTime();
				hdrReq.getMessageId().setStartTime(stf);
			}
			forward(msg);
		}

	}

	/*
	 * Forward request to the leader
	 */
	private void forward(Message msg) {
		Address target = leader;
		Zab2PhasesHeader hdrReq = (Zab2PhasesHeader) msg.getHeader(this.id);
		try {
			Zab2PhasesHeader hdr = new Zab2PhasesHeader(
					Zab2PhasesHeader.FORWARD, hdrReq.getMessageId());
			Message forward_msg = new Message(target).putHeader(this.id, hdr);
      forward_msg.setBuffer(new byte[1000]); 
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
	private void sendACK(Message msg, Zab2PhasesHeader hdrAck) {
		if (!stats.isWarmup()) {
			stats.incNumRequest();
		}
		if (msg == null)
			return;

		if (hdrAck == null)
			return;

		queuedProposalMessage.put(hdrAck.getZxid(), hdrAck);
		Zab2PhasesHeader hdrACK = new Zab2PhasesHeader(Zab2PhasesHeader.ACK,
				hdrAck.getZxid(), hdrAck.getMessageId());
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
		delivery.add(hdrAck);

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
		delivery.add(new Zab2PhasesHeader(Zab2PhasesHeader.DELIVER, hdr.getZxid()));
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
		Zab2PhasesHeader hdrOrginal = queuedProposalMessage.remove(dZxid);
		queuedCommitMessage.put(dZxid, hdrOrginal);
		if (!stats.isWarmup()) {
			stats.incnumReqDelivered();
			stats.setEndThroughputTime(System.currentTimeMillis());
		}
		if(hdrOrginal==null)
			log.info("****hdrOrginal is null ****");
		if (requestQueue.contains(hdrOrginal.getMessageId())) {
			if (!stats.isWarmup()) {
				long startTime = hdrOrginal.getMessageId().getStartTime();
				long endTime = System.nanoTime();
				stats.addLatency((long) (endTime - startTime));
			}
			Zab2PhasesHeader hdrResponse = new Zab2PhasesHeader(
					Zab2PhasesHeader.RESPONSE, dZxid, hdrOrginal.getMessageId());
			Message msgResponse = new Message(hdrOrginal.getMessageId()
					.getOriginator()).putHeader(this.id, hdrResponse);
			down_prot.down(new Event(Event.MSG, msgResponse));
			requestQueue.remove(hdrOrginal.getMessageId());

		}

	}

	/*
	 * Send replay to client
	 */
	private void handleOrderingResponse(Zab2PhasesHeader hdrResponse) {
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

	
	private synchronized void addTotalABMssages(Zab2PhasesHeader carryCountMessageLeader) {
		long followerMsg = carryCountMessageLeader.getZxid();
		stats.addCountTotalMessagesFollowers((int) followerMsg);
		numABRecieved++;
		if(numABRecieved==zabMembers.size()-1){
			Zab2PhasesHeader headertStats = new Zab2PhasesHeader(Zab2PhasesHeader.STATS);
			for (Address zabServer:zabMembers){
				Message messageStats = new Message(zabServer).putHeader(this.id,
					headertStats);
				down_prot.down(new Event(Event.MSG, messageStats));
			}
		}
	}
	
	private void sendMyTotalBroadcast(){
		Zab2PhasesHeader followerMsgCount = new Zab2PhasesHeader(
				Zab2PhasesHeader.COUNTMESSAGE, countMessageFollower);
		Message requestMessage = new Message(leader).putHeader(this.id,
				followerMsgCount);
		down_prot.down(new Event(Event.MSG, requestMessage));
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
				if (!stats.isWarmup()) {
					stats.incNumRequest();
				}

				Zab2PhasesHeader hdrProposal = new Zab2PhasesHeader(
						Zab2PhasesHeader.PROPOSAL, new_zxid,
						hdrReq.getMessageId());
				Message ProposalMessage = new Message().putHeader(this.id,
						hdrProposal);

				ProposalMessage.setSrc(local_addr);
        ProposalMessage.setBuffer(new byte[1000]);
				Proposal p = new Proposal();
				p.setMessageId(hdrReq.getMessageId());
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
			log.info("call deliverMessages()");
        	deliverMessages();

         }

        private void deliverMessages() {
			Zab2PhasesHeader hdrDelivery= null;
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
				finishedThroughput=stats.getnumReqDelivered();
				currentThroughput = (((double)finishedThroughput - stats
						.getLastNumReqDeliveredBefore()) / ((double)(currentTime - startTime)/1000.0));
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