package org.jgroups.protocols.jzookeeper.clustersize;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
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
	private volatile long  msgReceived = 0;
	private List<Address> zabBox = new ArrayList<Address>();
	private List<Long> latencies = new ArrayList<Long>();
	private View view;
	private static Calendar cal = Calendar.getInstance();
	private  short ID = ClassConfigurator
			.getProtocolId(Zab.class);
	private static int load = 1;
	private static int count = 0;
	private long numSendMsg=0;
    private static boolean is_warmUp = false;
    private long warmUpRequests = 0;
    private long currentLoad = 0;
    private int sendTime = 0;
    private ZabTestThreads zabTest= new ZabTestThreads();
    private static Random rand = new Random();
 	private static int min = 0, max = 0;


	public ZabClient(List<Address> zabbox, CyclicBarrier barrier, long numsMsg, AtomicLong local,
			byte[] payload, String ProtocotName, long num_msgsPerThreads, String propsFile, int load, long warmUpRequests, int sendTime, ZabTestThreads zabTest ) {
		this.barrier = barrier;
		this.local = local;
		this.payload = payload;
		this.numsMsg = numsMsg;
		this.zabBox =zabbox;
		this.ProtocotName = ProtocotName;
		this.num_msgsPerThreads = num_msgsPerThreads;
		this.warmUpRequests = warmUpRequests;
		this.props = propsFile;
		this.load = load;
		this.sendTime=sendTime;
		this.min = sendTime - ((int)(sendTime * 0.25));
		this.max = sendTime + ((int)(sendTime * 0.25));
		this.zabTest = zabTest;
		this.ID = ClassConfigurator
			.getProtocolId(Zab.class);
	}

	public void init() {
		 msgReceived = 0;
		 numSendMsg=0;
		 latencies.clear();
	}
	
	public void setWarmUp(boolean warmUp){
		is_warmUp= warmUp;
	}

	public void viewAccepted(View new_view) {
		System.out.println("** view: " + new_view);
		view = new_view;
		System.out.println("** view: " + new_view);
	}

	public void start() throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append("\n\n----------------------- ZABPerf -----------------------\n");
		sb.append("Date: ").append(new Date()).append('\n');
		sb.append("Run by: ").append(System.getProperty("user.name"))
				.append("\n");
		sb.append("JGroups version: ").append(Version.description).append('\n');
		System.out.println(sb);
		channel = new JChannel(props);
		channel.setReceiver(this);
		channel.connect("ZabAll");
		local_addr = channel.getAddress();
		Address coord = channel.getView().getMembers().get(0);

	}

	public void sendMessages(long numMsgs) {
		msgReceived=0;
		this.currentLoad = numMsgs;
		this.sender = new Sender(this.barrier, this.local,
				this.payload, numMsgs, load, sendTime);
		System.out.println("Start sending "+ sender.getName());
		sender.start();
	}

	public void receive(Message msg) {
		synchronized (this) {
			final ZabHeader testHeader = (ZabHeader) msg.getHeader(ID);
			if (testHeader.getType() != ZabHeader.START_SENDING) {
				if (is_warmUp){
					msgReceived++;
					if(msgReceived>=warmUpRequests){
						try {
							zabTest.finishedWarmupTest();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
				else if(testHeader.getType()==ZabHeader.RESPONSE){
					msgReceived++;
					if(msgReceived>=num_msgsPerThreads)
						zabTest.finishedRealTest();
				}

			}
		}

	}

	public class Sender extends Thread {
		private final CyclicBarrier barrier;
		private AtomicLong local = new AtomicLong(0);// , actually_sent;
		private final byte[] payload;
		private long num_msgsPerThreads;
		private int load=1;
		private int sendTime=100;


		protected Sender(CyclicBarrier barrier, AtomicLong local,
				byte[] payload, long num_msgsPerThreads, int load, int sendTime) {
			super("" + (count++));
			this.barrier = barrier;
			this.payload = payload;
			this.local = local;
			this.num_msgsPerThreads = num_msgsPerThreads;
			this.load = load;
			this.sendTime=sendTime;

		}

		public void run() {
			System.out.println("Thread start " + getName());
			Address target;
			numSendMsg =0;
			for (int i = 0; i < num_msgsPerThreads; i++) {
				numSendMsg = i;
				//if(is_warmUp){
					while ((numSendMsg - msgReceived) > load){
							try {
								Thread.sleep(0,1);
							} catch (InterruptedException e1) {
								e1.printStackTrace();
							}
					}
			//	}
				try {
					MessageId messageId = new MessageId(local_addr,
							local.getAndIncrement(), System.currentTimeMillis());

					ZabHeader hdrReq = new ZabHeader(ZabHeader.REQUEST,
							messageId);
					target = Util.pickRandomElement(zabBox);
					Message msg = new Message(target, payload);
					msg.putHeader(ID, hdrReq);
				//	System.out.println("sender " + this.getName()+ " Sending " + i + " out of " + num_msgsPerThreads);
					channel.send(msg);
         // if(!is_warmUp){
					//	sendTime = min + rand.nextInt((max - min) + 1);
					//	Thread.sleep(sendTime);
					//}					

				} catch (Exception e) {
				}
			}
		}
	}
}

