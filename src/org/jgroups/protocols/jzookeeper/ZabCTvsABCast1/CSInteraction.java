package org.jgroups.protocols.jzookeeper.ZabCTvsABCast1;

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
import org.jgroups.protocols.jzookeeper.ZabCTvsABCast.InfiniteClients.TestHeader;
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

    @Property(name = "queue_capacity", description = "The maximum number of ordering requests that can be queued")
    private int QUEUE_CAPACITY = 500;

    @Property(name = "bundle_msgs", description = "If true then ordering requests will be bundled when possible" +
            "in order to reduce the number of total order broadcasts between box members")
    
    private View view = null;
    

    private boolean BUNDLE_MSGS = true;

    private Address leader = null;

    private Address local_addr = null;
    
    private int index = 0;
	private int clusterSize = 3;
    private final List<Address> zabMembers = new ArrayList<Address>();
    private ExecutorService executor;

    public CSInteraction() {
    }

    private void logHack() {
        Logger logger = Logger.getLogger(this.getClass().getName());
        ConsoleHandler handler = new ConsoleHandler();
        handler.setLevel(Level.FINE);
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
            	log.info("Header "+header);

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
            	handleClientRequest((Message) event.getArg());
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
		leader = mbrs.get(0);
		//make the first three joined server as ZK servers
		if (mbrs.size() == clusterSize) {
			zabMembers.addAll(v.getMembers());
		}
		if (mbrs.size() > clusterSize && zabMembers.isEmpty()) {
			for (int i = 0; i < clusterSize; i++) {
				zabMembers.add(mbrs.get(i));
			}

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

	private void handleClientRequest(Message message) {
		TestHeader clientHeader = ((TestHeader) message.getHeader((short) 1026));
		if (clientHeader != null && clientHeader.getType() == TestHeader.TEST_MSG) {
			MessageId msgId = new MessageId(local_addr, clientHeader.getSeq());
			msgId.setStartTime(clientHeader.getTimestamp());
		//	messageStore.put(msgId, message);
			ZabCoinTossingHeader zabCTHeader = new ZabCoinTossingHeader(ZabCoinTossingHeader.REQUEST, msgId);
			++index;
			if (index > (clusterSize-1))
				index = 0;
			Message msg = new Message(zabMembers.get(index)).putHeader(this.id, zabCTHeader);
			msg.setBuffer(new byte[1000]);
			down_prot.down(new Event(Event.MSG, msg));
		}

	}
    

    private synchronized void handleOrderingResponse(CSInteractionHeader responseHeader) {

        if (log.isTraceEnabled())
            log.trace("Ordering response received | " + responseHeader);

        MessageOrderInfo MessageOrderInfo = responseHeader.getMessageOrderInfo();
        //CSInteractionHeader header = new CSInteractionHeader(CSInteractionHeader.BROADCAST, MessageOrderInfo);
       
        deliverMessage(responseHeader);
    }

    private synchronized void deliverMessage(CSInteractionHeader responseHeader) {
        Message message = new Message().putHeader(this.id, responseHeader); 
        message.setDest(local_addr);

        if (log.isTraceEnabled())
            log.trace("Deliver Message | " + (CSInteractionHeader) message.getHeader(this.id));

        up_prot.up(new Event(Event.MSG, message));
    }

    
}
