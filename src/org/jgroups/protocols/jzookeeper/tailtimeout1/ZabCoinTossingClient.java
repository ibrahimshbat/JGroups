package org.jgroups.protocols.jzookeeper.tailtimeout1;

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
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.protocols.jzookeeper.MessageId;
import org.jgroups.protocols.jzookeeper.ZabCoinTossingHeader;
import org.jgroups.util.Util;

public class ZabCoinTossingClient extends ReceiverAdapter {
	private String props;
	private static String ProtocotName = "ZabCoinTossing";
	private JChannel channel;
	private Address local_addr = null;
	final AtomicInteger actually_sent = new AtomicInteger(0); 
	private CyclicBarrier barrier;
	private AtomicLong local = new AtomicLong(0);
	private byte[] payload;
	private long numsMsg = 0;
	private long num_msgsPerThreads;
	private Sender sender;
	private volatile long  msgReceived = 0;
	private List<Address> zabBox = new ArrayList<Address>();
	private List<Long> latencies = new ArrayList<Long>();
	private View view;
	private static Calendar cal = Calendar.getInstance();
	private  short ID = ClassConfigurator
			.getProtocolId(ZabCoinTossing.class);
	private static int load = 1;
	private static int count = 0;
	private long numSendMsg=0;
    private static boolean is_warmUp = false;
    private long warmUpRequests = 0;
    private int sendTime = 0;
    private String channelName;
    private ZabCoinTossingTest zabCoinTossingTest= new ZabCoinTossingTest();
    
	private static Random rand = new Random();
 	private static int min = 0, max = 0;


	public ZabCoinTossingClient(List<Address> zabbox, CyclicBarrier barrier, long numsMsg, AtomicLong local,
			byte[] payload, String ProtocotName, long num_msgsPerThreads, String propsFile, int load, 
			long warmUpRequests, int sendTime, String channelName, ZabCoinTossingTest zabCoinTossingTest ) {
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
		this.channelName = channelName;
		this.sendTime=sendTime;
		this.min = sendTime - ((int)(sendTime * 0.25));
		this.max = sendTime + ((int)(sendTime * 0.25));
		this.zabCoinTossingTest = zabCoinTossingTest;
		this.ID = ClassConfigurator
			.getProtocolId(ZabCoinTossing.class);
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
		channel.connect(channelName);
		local_addr = channel.getAddress();
		//JmxConfigurator.registerChannel(channel, Util.getMBeanServer(),
				//"jgroups", "ZABCluster", true);
		Address coord = channel.getView().getMembers().get(0);

	}

	public void sendMessages(long numMsgs) {
		msgReceived=0;
		this.sender = new Sender(this.barrier, this.local,
				this.payload, numMsgs, load, sendTime);
		System.out.println("Start sending "+ sender.getName());
		sender.start();
	}

	private String getCurrentTimeStamp() {
		long timestamp = new Date().getTime();
		cal.setTimeInMillis(timestamp);
		String timeString = new SimpleDateFormat("HH:mm:ss:SSS").format(cal
				.getTime());

		return timeString;
	}

	public void receive(Message msg) {
		synchronized (this) {
			final ZabCoinTossingHeader testHeader = (ZabCoinTossingHeader) msg.getHeader(ID);
			MessageId message = testHeader.getMessageId();
			if (testHeader.getType() != ZabCoinTossingHeader.START_SENDING) {
				if (is_warmUp){
					msgReceived++;
					if(msgReceived>=warmUpRequests){
						try {
							zabCoinTossingTest.finishedWarmupTest();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
				else if(testHeader.getType()==ZabCoinTossingHeader.RESPONSE){
					msgReceived++;
					if(msgReceived>=num_msgsPerThreads)
						zabCoinTossingTest.finishedRealTest();
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
			//this.min = sendTime - ((int)(sendTime * 0.25));
			//this.max = sendTime + ((int)(sendTime * 0.25));
		}

		public void run() {
			System.out.println("Thread start " + getName());
//			try {
//	            barrier.await();
//	        }
//	        catch(Exception e) {
//	            e.printStackTrace();
//	            return;
//	        }
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
				//}
				try {
					MessageId messageId = new MessageId(local_addr,
							local.getAndIncrement(), System.currentTimeMillis());

					ZabCoinTossingHeader hdrReq = new ZabCoinTossingHeader(ZabCoinTossingHeader.REQUEST,
							messageId);
					target = Util.pickRandomElement(zabBox);
					Message msg = new Message(target, payload);
					msg.putHeader(ID, hdrReq);
					System.out.println("sender " + this.getName()+ " Sending " + i + " out of " + num_msgsPerThreads);
					channel.send(msg);
         // if(!is_warmUp){
						//	stt=System.currentTimeMillis();
						//	System.out.println("sendTime="+sendTime);
//						sendTime = min + rand.nextInt((max - min) + 1);
//						Thread.sleep(sendTime);
						//System.out.println("sendTime="+sendTime);

						//	et=System.currentTimeMillis();
					//System.out.println("Send a Message ("+i+")"+this.getName());
					//}				
					//isSend = true;
					//while (isSend){
						//wait until notify
					//}

				} catch (Exception e) {
				}
			}
		}
	}
}
