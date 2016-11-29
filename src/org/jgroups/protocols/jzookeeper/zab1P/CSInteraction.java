package org.jgroups.protocols.jzookeeper.zab1P;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jgroups.Address;
import org.jgroups.AnycastAddress;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.ViewId;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;

public class CSInteraction extends Protocol {

	public static int minimumServers = 3; // Static hack to allow experiments to dynamically change the value.

	@Property(name = "service_member", description = "Is this process a service member")
	private boolean zMember = false;

	@Property(name = "z_members", description = "A list of hostnames that will be Z members (seperated by a colon)")
	private String boxHostnames = "";

	@Property(name = "msg_size", description = "The max size of a msg between Z members.  Determines the number of msgs that can be bundled")
	private int MSG_SIZE = 1000;


	@Property(name = "Zab_size", description = "It is Zab cluster size")
	private int clusterSize = 3;

	private View view = null;

	private boolean BUNDLE_MSGS = true;

	private Address leader = null;

	private Address local_addr = null;

	private int index = 0;
	private AtomicInteger countDeliver = new AtomicInteger(0);

	private AtomicLong local = new AtomicLong(0);

	private final Map<MessageId, Message> messageStore = Collections.synchronizedMap(new HashMap<MessageId, Message>());
	private final List<Address> zabMembers = new ArrayList<Address>();
	private AtomicInteger localSequence = new AtomicInteger(); // This nodes sequence number
	private ExecutorService executor;

	public CSInteraction() {
	}

	private void logHack() {
		Logger logger = Logger.getLogger(this.getClass().getName());
		ConsoleHandler handler = new ConsoleHandler();
		handler.setLevel(Level.ALL);
		logger.addHandler(handler);
		logger.setUseParentHandlers(false);
	}

	@Override
	public void init() throws Exception {
		logHack();
		setLevel("info");

	}


	@Override
	public void start() throws Exception {
		executor = Executors.newSingleThreadExecutor();
	}

	@Override
	public void stop() {
		executor.shutdown();
	}

	@Override
	public Object up(Event event) {
		switch (event.getType()) {
		case Event.MSG:
			Message message = (Message) event.getArg();
			CSInteractionHeader header = (CSInteractionHeader) message.getHeader(id);
			//log.info("CSInteractionHeader.RESPONSE: "+header);

			if (header == null)
				break;

			switch (header.getType()) {
			case CSInteractionHeader.RESPONSE:
				handleOrderingResponse(header);
				break;
			}
			return null;
		case Event.VIEW_CHANGE:
			handleViewChange((View) event.getArg());
			if (log.isTraceEnabled())
				log.trace("New View := " + view);
			break;
		}
		return up_prot.up(event);
	}

	@Override
	public Object down(Event event) {
		switch (event.getType()) {
		case Event.MSG:
			handleMessageRequest(event);
			return null;
		case Event.SET_LOCAL_ADDRESS:
			local_addr = (Address) event.getArg();
			break;
		}
		return down_prot.down(event);
	}

	/*
	 * Handling all client requests, processing them according to request type
	 */

	private void handleViewChange(View v) {
		List<Address> mbrs = v.getMembers();
		//make the first three joined server as ZK servers
		if (mbrs.size() == (clusterSize+2)) {
			for (int i=2;i<mbrs.size();i++){
				zabMembers.add(mbrs.get(i));
			}
			leader = zabMembers.get(0);
		}
		if ((mbrs.size() > (clusterSize+2)) && zabMembers.isEmpty()) {
			for (int i = 2; i < mbrs.size(); i++) {
				zabMembers.add(mbrs.get(i));
			}
			leader = zabMembers.get(0);
		}

		log.info("zabMembers size = " + zabMembers);
		if (mbrs.isEmpty())
			return;

		if (view == null || view.compareTo(v) < 0){
			view = v;
		}
		else
			return;
	}

	private void handleMessageRequest(Event event) {
		
		Message message = (Message) event.getArg();
		Address destination = message.getDest();
		//log.info("handleMessageRequest   " + message);

		//Store put here, and Forward write to Z to obtain ordering
		if (destination != null && destination instanceof AnycastAddress && !message.isFlagSet(Message.Flag.NO_TOTAL_ORDER)) {
				//log.info("if   " + (destination instanceof AnycastAddress));

			sendOrderingRequest(((AnycastAddress) destination).getAddresses(), message);
		}

		//		else if (destination != null && !(destination instanceof AnycastAddress)){
		//			ZabHeader clientHeader = ((ZabHeader) message.getHeader((short) 78));
		//			log.info("Header = " + clientHeader);
		//
		//			if (clientHeader!=null && clientHeader.getType() == ZabHeader.SENDMYADDRESS){
		//				for (Address server : zabMembers) {
		//					log.info("server address = " + server);
		//					message.dest(server);
		//					message.src(message.getSrc());
		//					down_prot.down(new Event(Event.MSG, message));
		//				}
		//			}
		//		}
		else if (destination != null && !(destination instanceof AnycastAddress)) {
			//log.info("else if " + !(destination instanceof AnycastAddress));
			down_prot.down(new Event(Event.MSG, message));
		}



	}

	private synchronized void sendOrderingRequest(Collection<Address> destinations, Message message) {
		Address destination = null;
		MessageId messageId = new MessageId(local_addr, local.getAndIncrement());
		message.setSrc(local_addr);
		messageStore.put(messageId, message);
		MessageOrderInfo messageOrderInfo = new MessageOrderInfo(messageId);
		Zab2PhasesHeader hdrReq = new Zab2PhasesHeader(Zab2PhasesHeader.REQUEST, messageOrderInfo);
		++index;
		if (index > (clusterSize-1))
			index = 0;
		destination = zabMembers.get(index);
		//log.info("Send To --->"+ destination);
		Message requestMessage = new Message(destination).putHeader((short) 78, hdrReq);
		//.setFlag(Message.Flag.DONT_BUNDLE);
		requestMessage.setBuffer(new byte[1000]);
		down_prot.down(new Event(Event.MSG, requestMessage));

	}

	private synchronized void handleOrderingResponse(CSInteractionHeader responseHeader) {

		if (log.isTraceEnabled())
			log.trace("Ordering response received | " + responseHeader);

		MessageOrderInfo messageOrderInfo = responseHeader.getMessageOrderInfo();
		Message message = messageStore.get(messageOrderInfo.getId());
		//MessageId id = ((CSInteractionHeader)message.getHeader(this.id)).getMessageOrderInfo().getId();
		messageStore.remove(messageOrderInfo.getId());
		message.setDest(local_addr);

		if (log.isTraceEnabled())
			log.trace("Deliver Message | " + (CSInteractionHeader) message.getHeader(this.id));

		up_prot.up(new Event(Event.MSG, message));

	}



	private synchronized void deliverMessage(Message message) {
		MessageId id = ((CSInteractionHeader)message.getHeader(this.id)).getMessageOrderInfo().getId();
		messageStore.remove(id);
		message.setDest(local_addr);

		if (log.isTraceEnabled())
			log.trace("Deliver Message | " + (CSInteractionHeader) message.getHeader(this.id));
		//log.info("Deliver Message Order | " + ((CSInteractionHeader)message.getHeader(this.id)).getMessageOrderInfo().getOrdering()
		//+ " Count deliver | "+countDeliver.incrementAndGet()+ " Destinations | "+ getAddresses(((CSInteractionHeader)message.
		//getHeader(this.id)).getMessageOrderInfo().getDestinations()));
		up_prot.up(new Event(Event.MSG, message));
	}

	private List<Address> getAddresses(byte[] indexes) {
		if (view == null)
			throw new IllegalArgumentException("View cannot be null");

		List<Address> addresses = new ArrayList<Address>();
		for (byte index : indexes) {
			addresses.add(view.getMembers().get(index));
		}
		return addresses;
	}




}
