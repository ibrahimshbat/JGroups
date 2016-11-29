package org.jgroups.protocols.jzookeeper.testrw;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.Version;
import org.jgroups.View;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.util.Util;

public class ZabClient extends ReceiverAdapter {
	private String props;
	private static String ProtocotName = "Zab";
	private JChannel channel;
	private Address local_addr = null;
	final AtomicInteger actually_sent = new AtomicInteger(0);
	private final CyclicBarrier barrier;
	private AtomicLong local = new AtomicLong(0);
	private final byte[] payload;
	private final long numsMsg;
	private long num_msgsPerThreads;
	private Sender sender;
	private volatile long msgReceived = 0;
	private List<Address> zabBox = new ArrayList<Address>();
	private List<Long> latencies = new ArrayList<Long>();
	private View view;
	private static Calendar cal = Calendar.getInstance();
	private short ID;
	private static int load = 1;
	private static int count = 0;
	private long numSendMsg = 0;
	private static boolean is_warmUp = false;
	private long warmUpRequests = 0;
	private long currentLoad = 0;
	private int sendTime = 0;
	private ZabTestThreads zabTest = new ZabTestThreads();
	private double read_percentage = 0; // 0% reads, 100% writes
	private static Random rand = new Random();
	private static int min = 0, max = 0;
	private ConcurrentMap<Long, MessageInfo> KeysForRead;
	private AtomicInteger numReceive = new AtomicInteger(0);



	public ZabClient(List<Address> zabbox, CyclicBarrier barrier, long numsMsg, AtomicLong local, byte[] payload,
			String ProtocotName, long num_msgsPerThreads, String propsFile, int load, long warmUpRequests, int sendTime,
			double read_percentage, AtomicInteger numReceive, ZabTestThreads zabTest) {
		this.barrier = barrier;
		this.local = local;
		this.payload = payload;
		this.numsMsg = numsMsg;
		this.zabBox = zabbox;
		this.ProtocotName = ProtocotName;
		this.num_msgsPerThreads = num_msgsPerThreads;
		this.warmUpRequests = warmUpRequests;
		this.props = propsFile;
		this.load = load;
		this.sendTime = sendTime;
		this.min = sendTime - ((int) (sendTime * 0.25));
		this.max = sendTime + ((int) (sendTime * 0.25));
		this.read_percentage = read_percentage;
		this.zabTest = zabTest;
		this.KeysForRead = new ConcurrentHashMap<Long, MessageInfo>();
		this.numReceive = numReceive;
		this.ID = ClassConfigurator.getProtocolId(Zab.class);

	}

	public void init() {
		msgReceived = 0;
		numSendMsg = 0;
		latencies.clear();
	}

	public void setWarmUp(boolean warmUp) {
		is_warmUp = warmUp;
	}

	public void viewAccepted(View new_view) {
		view = new_view;
		System.out.println("** view: " + new_view);
	}

	public void start() throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append("\n\n----------------------- ZABPerf -----------------------\n");
		sb.append("Date: ").append(new Date()).append('\n');
		sb.append("Run by: ").append(System.getProperty("user.name")).append("\n");
		sb.append("JGroups version: ").append(Version.description).append('\n');
		System.out.println(sb);
		channel = new JChannel(props);
		channel.setReceiver(this);
		channel.connect("ZabAll");
		local_addr = channel.getAddress();
		Address coord = channel.getView().getMembers().get(0);
	}

	public void sendMessages(long numMsgs) {
		msgReceived = 0;
		this.currentLoad = numMsgs;
		this.sender = new Sender(this.barrier, this.local, this.payload, numMsgs, load, sendTime);
		System.out.println("Start sending " + sender.getName());
		sender.start();
	}

	public void receive(Message msg) {
		synchronized (this) {
			ZabHeader ZabHdr = (ZabHeader) msg.getHeader(ID);
			if (is_warmUp) {
				msgReceived++;
				if (msgReceived >= warmUpRequests) {
					try {
						zabTest.finishedWarmupTest();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}

			}
			if (!is_warmUp) {
				msgReceived++;
				//System.out.println("# Receiver For clients: "+msgReceived);
				System.out.println("# Receiver For all 10 clients: "+numReceive.incrementAndGet());
				if(ZabHdr.getType() == ZabHeader.RESPONSEW){
					synchronized (KeysForRead) {
						KeysForRead.put(ZabHdr.getMessageInfo().getZxid(), ZabHdr.getMessageInfo());			
						//System.out.println("Recieve Write Zxid: "+ZabHdr.getMessageInfo().getZxid());
					}
				}
				//if(ZabHdr.getType() == ZabHeader.RESPONSER){
				//System.out.println("Recieve Read Zxid: "+ZabHdr.getMessageInfo().getZxid());
				//}
				//System.out.println("#Recieve Respose msgReceived: "+msgReceived);
				if (msgReceived >= num_msgsPerThreads)
					zabTest.finishedRealTest();
			}

		}

	}

	public class Sender extends Thread {
		private final CyclicBarrier barrier;
		private AtomicLong local = new AtomicLong(0);// , actually_sent;
		private final byte[] payload;
		private long num_msgsPerThreads;
		private int load = 1;
		private int sendTime = 100;

		protected Sender(CyclicBarrier barrier, AtomicLong local, byte[] payload, long num_msgsPerThreads, int load,
				int sendTime) {
			super("" + (count++));
			this.barrier = barrier;
			this.payload = payload;
			this.local = local;
			this.num_msgsPerThreads = num_msgsPerThreads;
			this.load = load;
			this.sendTime = sendTime;

		}

		public void run() {
			System.out.println("Thread start " + getName());
			numSendMsg = 0;
			for (int i = 0; i < num_msgsPerThreads; i++) {
				numSendMsg = i;
				//if (is_warmUp) {
					while ((numSendMsg - msgReceived) > load) {
						try {
							Thread.sleep(0, 1);
						} catch (InterruptedException e1) {
							e1.printStackTrace();
						}
					}
				//}
				try {
					ZabHeader hdrReq = null;
					boolean get = Util.tossWeightedCoin(read_percentage);
					if (!is_warmUp){
						if (get && !KeysForRead.isEmpty()){
							List<MessageInfo> MessageInfoList = new ArrayList<MessageInfo>(KeysForRead.values());
							int randomIndex = new Random().nextInt(MessageInfoList.size());
							MessageInfo messageInfoRead = MessageInfoList.get(randomIndex);
							hdrReq = new ZabHeader(ZabHeader.REQUESTR, messageInfoRead);
						}
						else{
							MessageId messageId = new MessageId(local_addr, local.getAndIncrement());
							MessageInfo messageInfo = new MessageInfo(messageId);
							hdrReq = new ZabHeader(ZabHeader.REQUESTW, messageInfo);
						}
					}
					else{
						MessageId messageId = new MessageId(local_addr, local.getAndIncrement());
						MessageInfo messageInfo = new MessageInfo(messageId);
						hdrReq = new ZabHeader(ZabHeader.REQUESTW, messageInfo);
					}
					Message msg = new Message(null);
					msg.putHeader(ID, hdrReq);
					// System.out.println("sender " + this.getName()+ " Sending
					// " + i + " out of " + num_msgsPerThreads);
					channel.send(msg);
//					if (!is_warmUp) {
//						sendTime = min + rand.nextInt((max - min) + 1);
//						Thread.sleep(sendTime);
//					}

				} catch (Exception e) {
				}
			}
		}
	}
}
