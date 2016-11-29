package org.jgroups.protocols.jzookeeper.zWithInfinspan;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.stack.Protocol;


/*
 * It is orignal protocol of Apache Zookeeper. Also it has features of testing throuhput, latency (in Nano), ant etc. 
 * When using testing, it provides warm up test before starting real test.
 * @author Ibrahim EL-Sanosi
 */

public class ZabOneNode extends Protocol {

	private final AtomicLong zxid = new AtomicLong(0);
	private ExecutorService executorDelivery;
	private Address local_addr;
	private volatile View view;

	private Map<Long, MessageOrderInfo> queuedCommitMessage = new HashMap<Long, MessageOrderInfo>();
	private final LinkedBlockingQueue<MessageOrderInfo> delivery = new LinkedBlockingQueue<MessageOrderInfo>();

	private final Map<Address, Long> orderStore = Collections.synchronizedMap(new HashMap<Address, Long>());

	/*
	 * Empty constructor
	 */
	public ZabOneNode() {

	}


	@Override
	public void start() throws Exception {
		super.start();
		executorDelivery = Executors.newSingleThreadExecutor();
		executorDelivery.execute(new MessageHandler());
		log.setLevel("trace");
	}

	@Override
	public void stop() {
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
			case ZabHeader.REQUEST:
				delivery.add(hdr.getMessageOrderInfo());
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
	 * --------------------------------- Private Methods  --------------------------------
	 */



	private void handleViewChange(View v) {
		List<Address> mbrs = v.getMembers();


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
	 * Deliver the proposal locally and if the current server is the receiver of the request, 
	 * replay to the client.
	 */
	private void deliver(MessageOrderInfo messageOrderInfo) {
		long zx = getNewZxid();
		//log.info("Deliver=: "+zx);
		messageOrderInfo.setOrdering(zx);
		setLastOrderSequences(messageOrderInfo);
		queuedCommitMessage.put(zx, messageOrderInfo);
		sendOrderResponse(messageOrderInfo);

	}

	private void sendOrderResponse(MessageOrderInfo messageOrderInfo){
		CSInteractionHeader hdrResponse = new CSInteractionHeader(CSInteractionHeader.RESPONSE, messageOrderInfo);
		Message msgResponse = new Message(messageOrderInfo.getId()
				.getOriginator()).putHeader((short) 79, hdrResponse);
		msgResponse.setFlag(Message.Flag.DONT_BUNDLE);
		down_prot.down(new Event(Event.MSG, msgResponse));
	}


	private void setLastOrderSequences(MessageOrderInfo messageOrderInfo) {
		long[] clientLastOrder = new long[messageOrderInfo.getDestinations().length];
		List<Address> destinations = getAddresses(messageOrderInfo.getDestinations());
		for (int i = 0; i < destinations.size(); i++) {
			Address destination = destinations.get(i);
			if (!orderStore.containsKey(destination)) {
				clientLastOrder[i] = -1;
				orderStore.put(destinations.get(i), messageOrderInfo.getOrdering());
			} else {
				clientLastOrder[i] = orderStore.put(destination, messageOrderInfo.getOrdering());
			}
		}
		messageOrderInfo.setclientsLastOrder(clientLastOrder);
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


	final class MessageHandler implements Runnable {
		@Override
		public void run() {
			log.info("call deliverMessages()");
			deliverMessages();

		}

		private void deliverMessages() {
			MessageOrderInfo messageOrderInfo = null;
			while (true) {
				try {
					messageOrderInfo = delivery.take();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				deliver(messageOrderInfo);

			}
		}

	}

	final class Throughput extends TimerTask {

		public Throughput() {

		}

		private long startTime = 0;
		private long currentTime = 0;
		private double currentThroughput = 0;
		private int finishedThroughput = 0;

		@Override
		public void run() {
			//startTime = stats.getLastThroughputTime();
			currentTime = System.currentTimeMillis();
			//finishedThroughput = stats.getnumReqDelivered();
			//			currentThroughput = (((double) finishedThroughput - stats
			//					.getLastNumReqDeliveredBefore()) / ((double) (currentTime - startTime) / 1000.0));
			//			stats.setLastNumReqDeliveredBefore(finishedThroughput);
			//			stats.setLastThroughputTime(currentTime);
			//			stats.addThroughput(currentThroughput);
		}

		public String convertLongToTimeFormat(long time) {
			Date date = new Date(time);
			SimpleDateFormat longToTime = new SimpleDateFormat("HH:mm:ss.SSSZ");
			return longToTime.format(date);
		}
	}

}