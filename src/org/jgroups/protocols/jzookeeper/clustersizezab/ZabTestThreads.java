package org.jgroups.protocols.jzookeeper.clustersizezab;

import org.jgroups.*;

import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.Version;
import org.jgroups.View;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.MethodLookup;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Buffer;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;

/*
 * Automated Test
 */
public class ZabTestThreads extends ReceiverAdapter {
	private List<String> zabboxInit = new ArrayList<String>();
	private String propsFile = "conf/Zab.xml";
	private static String ProtocotName = "Zab";
	private JChannel channel;
	private Address local_addr = null;
	private List<Address> zabBox = new ArrayList<Address>();
	private List<Address> clients = new ArrayList<Address>();
	private View view;
	private static long num_msgs = 100000;
	private static long num_msgsPerThreads = 10000;
	private static int msg_size = 20000;
	private static int numOfClients = 10;
	private static int num_threads = 10;
	private ZabClient[] clientThreads;
	private final byte[] payload = new byte[msg_size];
	private AtomicLong localSequence = new AtomicLong();
	private short ID;
	private RpcDispatcher disp;

	private String outputDir;
	private static int numsThreadFinished = 0;
	private static int load = 1;
	private static boolean is_warmUp = false;
	private long numsOfWarmUpPerThread = 0;
	private int countTempRecieved = 0;
	private int sTime = 0;
 	private String initiator;
	private int clusterSize = 3;
	private String channelName;

	private static final Method[] METHODS = new Method[3];

	private static final short SETUPCLIENTS = 0;
	private static final short RESET = 1;
	private static final short STARTREALTEST = 2;

	static {
		try {
			METHODS[SETUPCLIENTS] = ZabTestThreads.class
					.getMethod("setupClientThreads");
			METHODS[RESET] = Zab.class
					.getMethod("reset");
			METHODS[STARTREALTEST] = ZabTestThreads.class
					.getMethod("startRealTest");
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public ZabTestThreads() {
	}

	public ZabTestThreads(String[] zabHosts, String protocolName, String props,
			int totalNum_msgs, int totalPerThreads, int num_threads,
			int msg_size, String outputDir, int numOfClients, int load,
			int numsOfWarmUpPerThread, int sendTime,  String initiator, 
			int clusterSize, String channelName) {
		this.zabboxInit = Arrays.asList(zabHosts);
		this.ProtocotName = protocolName;
		this.propsFile = props;
		this.num_msgs = totalNum_msgs;
		this.num_msgsPerThreads = totalPerThreads;
		this.num_threads = num_threads;
		this.msg_size = msg_size;
		this.outputDir = outputDir;
		this.numOfClients = numOfClients;
		this.load = load;
		this.numsOfWarmUpPerThread = numsOfWarmUpPerThread;
		this.sTime = sendTime;
		this.initiator = initiator;
		this.clusterSize = clusterSize;
		this.channelName = channelName;
		this.ID = ClassConfigurator.getProtocolId(Zab.class);
	}

	public void viewAccepted(View new_view) {
		System.out.println("** view: " + new_view);
		view = new_view;
		List<Address> mbrs = new_view.getMembers();
		if (mbrs.size() == clusterSize) {
			zabBox.addAll(new_view.getMembers());
		}

		if (mbrs.size() > clusterSize && zabBox.isEmpty()) {
			for (int i = 0; i < clusterSize; i++) {
				zabBox.add(mbrs.get(i));
			}
		}
		local_addr = channel.getAddress();

	}

	void stop() {
		if (disp != null)
			disp.stop();
		Util.close(channel);
	}

	public void setup() throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append("\n\n----------------------- ZABPerf -----------------------\n");
		sb.append("Date: ").append(new Date()).append('\n');
		sb.append("Run by: ").append(System.getProperty("user.name"))
				.append("\n");
		sb.append("JGroups version: ").append(Version.description).append('\n');
		System.out.println(sb);
		channel = new JChannel(propsFile);
		channel.setReceiver(this);
		disp = new RpcDispatcher(channel, null, this, this);
		disp.setMethodLookup(new MethodLookup() {
			public Method findMethod(short id) {
				return METHODS[id];
			}
		});
		disp.setRequestMarshaller(new CustomMarshaller());
		channel.connect(channelName);
		JmxConfigurator.registerChannel(channel, Util.getMBeanServer(),
				"jgroups", channelName, true);
		Address coord = channel.getView().getMembers().get(0);
		if (channel.getAddress().toString().contains(initiator)){
			 System.out.println("I am initiator");
			 setupClients();		 
		}
	}

	// Invoke by Initiator
	public void setupClients() {
		removeZabMembers(view.getMembers()); 
		for (Address client : clients) {
			try {
				System.out.println("RPC TO "+client);
				long time1 =System.currentTimeMillis();
				 RequestOptions options=new RequestOptions(ResponseMode.GET_ALL, 40000);
			     options.setFlags(Message.Flag.OOB, Message.Flag.DONT_BUNDLE, Message.NO_FC);
			     RspList<Object> responses=disp.callRemoteMethod(client, new MethodCall(SETUPCLIENTS), options);
				long time2 =System.currentTimeMillis();   
				//.sleep(40000);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		warmup();
	}

	private void removeZabMembers(List<Address> addresses) {
		if (zabBox == null)
			return;
		clients = new ArrayList<Address>(addresses);
		for (Address zabHostName : zabBox) {
			if (clients.contains(zabHostName)) {
				clients.remove(zabHostName);
			}
		}

	}

	public void setupClientThreads() {

		final CyclicBarrier barrier = new CyclicBarrier(num_threads + 1);
		clientThreads = new ZabClient[num_threads];
		for (int i = 0; i < clientThreads.length; i++) {
			clientThreads[i] = new ZabClient(zabBox, barrier, num_msgs,
					localSequence, payload, ProtocotName, num_msgsPerThreads,
					propsFile, load, numsOfWarmUpPerThread, sTime, channelName, this);
		}
		
		System.out.println("Start create Threads");
		for (int i = 0; i < clientThreads.length; i++) {
			try {
				clientThreads[i].start();
				Thread.sleep(1000);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		// }
		// Send local address to leader
		MessageId mid = new MessageId(local_addr,
				localSequence.incrementAndGet());
		ZabHeader startHeader = new ZabHeader(ZabHeader.SENDMYADDRESS, -6, mid);
		Message msg = new Message(null).putHeader(ID, startHeader);
		msg.src(channel.getAddress());
		msg.setObject("SENDMYADDRESS");
		try {
			channel.send(msg);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void sendStartSign() throws Exception {
		MessageId mid = new MessageId(local_addr,
				localSequence.incrementAndGet());
		ZabHeader startHeader = new ZabHeader(ZabHeader.START_SENDING, -5, mid);
		Message msg = new Message(null).putHeader(ID, startHeader);
		msg.setObject("req");
		channel.send(msg);
	}

	public void resetProtocol() throws Exception {
		
		numsThreadFinished = 0;
		is_warmUp = false;
		MessageId mid = new MessageId(local_addr,
				localSequence.incrementAndGet());
		ZabHeader resetProtocol = new ZabHeader(ZabHeader.RESET, -4, mid);
		Message msg = new Message(null).putHeader(ID, resetProtocol);
		channel.send(msg);
		Thread.sleep(10);
		for (Address client : clients) {
			try {
				 RequestOptions options=new RequestOptions(ResponseMode.GET_NONE, 0);
			     options.setFlags(Message.Flag.OOB, Message.Flag.DONT_BUNDLE, Message.NO_FC);
			     RspList<Object> responses=disp.callRemoteMethod(client, new MethodCall(STARTREALTEST), options);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void callRemotePrintStats() throws Exception {
		MessageId mid = new MessageId(local_addr,
				localSequence.incrementAndGet());
		ZabHeader stats = new ZabHeader(ZabHeader.STATS, -3, mid);
		Message msg = new Message(null).putHeader(ID, stats);
		channel.send(msg);

	}

	public void calculateABMessage() throws Exception {
		MessageId mid = new MessageId(local_addr,
				localSequence.incrementAndGet());
		ZabHeader stats = new ZabHeader(ZabHeader.COUNTMESSAGE, -2, mid);
		Message msg = new Message(null).putHeader(ID, stats);
		channel.send(msg);

	}

	public void sendTestRequest() throws Exception {
		int numSendMsg = 0;
		for (int i = 0; i < 1000; i++) {
			numSendMsg = i;
			while ((numSendMsg - countTempRecieved) > load) {
				try {
					Thread.sleep(0, 1);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}
			MessageId mid = new MessageId(local_addr,
					localSequence.incrementAndGet());
		}

	}

	public void receive(Message msg) {
		final ZabHeader testHeader = (ZabHeader) msg.getHeader(ID);
		if (testHeader.getType() == ZabHeader.STARTREALTEST) {
			startRealTest();
		}
	}

	public void sendMyAddressToLeader() {
		MessageId mid = new MessageId(local_addr,
				localSequence.incrementAndGet());
		ZabHeader startHeader = new ZabHeader(ZabHeader.SENDMYADDRESS, -6, mid);
		Message msg = new Message(null).putHeader(ID, startHeader);
		msg.src(channel.getAddress());
		msg.setObject("SENDMYADDRESS");
		try {
			channel.send(msg);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void warmup() {
		for (int i = 0; i < clientThreads.length; i++) {
			clientThreads[i].init();
		}
		
		numsThreadFinished = 0;
		is_warmUp = true;
		System.out.println("Warm up starts");
		for (int i = 0; i < clientThreads.length; i++) {
			clientThreads[i].setWarmUp(true);
			clientThreads[i].sendMessages(numsOfWarmUpPerThread);
		}
	}

	public void sendFinishedNotifcation() {
		MessageId mid = new MessageId(local_addr,
				localSequence.incrementAndGet());
		ZabHeader finishedHeader = new ZabHeader(ZabHeader.CLIENTFINISHED, -11,
				mid);
		Message msg = new Message(null).putHeader(ID, finishedHeader);
		msg.src(channel.getAddress());
		msg.setObject("CLIENTFINISHED");
		try {
			channel.send(msg);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public synchronized void finishedWarmupTest() throws Exception {
		numsThreadFinished++;
		if (numsThreadFinished >= num_threads) {
			System.out
					.println("Finished warm up----------------------------->>>");
			numsThreadFinished = 0;
			resetProtocol();
		}
	}

	public void startRealTest() {
		System.out.println("Real test starts");

		for (int i = 0; i < clientThreads.length; i++) {
			clientThreads[i].init();
		}

		for (int i = 0; i < clientThreads.length; i++) {
			clientThreads[i].setWarmUp(false);
			clientThreads[i].sendMessages(num_msgsPerThreads);
		}
	}

	public synchronized void finishedRealTest() {

		numsThreadFinished++;
		if (numsThreadFinished >= num_threads) {
			System.out.println("Finished");
			sendFinishedNotifcation();
		}

	}

	public static void main(String[] args) {
		String propsFile = "conf/Zab.xml";
		String name = "Zab";
		String outputDir = "/home/pg/p13/a6915654/" + name + "/";
		String[] zabboxInits = new String[3];
		int msgSize = 1000;
		int load = 1;
		int numsThreads = 10;
		int numberOfMessages = 100000; // #Msgs to be executed by this node
		int totalMessages = 1000000; // #Msgs to be sent by the whole cluster
		int numOfClients = 10;
		int numWarmUp = 10000;
		int sendTime = 10;
		String initiator="";
		int cSize = 10;
		String channelName = "ZabT";

		for (int i = 0; i < args.length; i++) {

			if ("-warmup".equals(args[i])) {
				numWarmUp = Integer.parseInt(args[++i]);
				System.out.println(numWarmUp);
				continue;
			}

			if ("-load".equals(args[i])) {
				load = Integer.parseInt(args[++i]);
				System.out.println(load);
				continue;
			}

			if ("-config".equals(args[i])) {
				propsFile = args[++i];
				continue;
			}
			if ("-hosts".equals(args[i])) {
				zabboxInits = args[++i].split(",");
			}
			if ("-name".equals(args[i])) {
				name = args[++i];
				continue;
			}
			if ("-tmessages".equals(args[i])) {
				totalMessages = Integer.parseInt(args[++i]);
				continue;
			}
			if ("-nmessages".equals(args[i])) {
				numberOfMessages = Integer.parseInt(args[++i]);
				continue;
			}
			if ("-threads".equals(args[i])) {
				numsThreads = Integer.parseInt(args[++i]);
				continue;
			}
			if ("-msgSize".equals(args[i])) {
				msgSize = Integer.parseInt(args[++i]);
				continue;
			}
			if ("-outputDir".equals(args[i])) {
				outputDir = args[++i];
				continue;
			}
			if ("-numClients".equals(args[i])) {
				numOfClients = Integer.parseInt(args[++i]);
				continue;
			}
			if ("-sTime".equals(args[i])) {
				sendTime = Integer.parseInt(args[++i]);
				continue;
			}
			if ("-init".equals(args[i])) {
				initiator = args[++i];
				continue;
			}
			if ("-clusterSize".equals(args[i])) {
				cSize = Integer.parseInt(args[++i]);
				continue;
			}
			if ("-channel".equals(args[i])) {
				channelName = args[++i];
				continue;
			}
			

		}
		System.out.println("propsFile " + propsFile + "/" + "zabboxInits "
				+ zabboxInits + "/" + "name " + name + "/" + "totalMessages "
				+ totalMessages + "/" + "numberOfMessages " + numberOfMessages
				+ "/" + "numsThreads " + numsThreads + "/" + "msgSize"
				+ msgSize + "/" + "outputDir " + outputDir + "/"
				+ "numOfClients " + numOfClients + "/" + "load " + load + "/"
				+ "numWarmUp " + numWarmUp + "/" + "sendTime " + sendTime + "/" + 
				"Initiator " + initiator+"ChannelName " + channelName);
		try {

			final ZabTestThreads test = new ZabTestThreads(zabboxInits, name,
					propsFile, totalMessages, numberOfMessages, numsThreads,
					msgSize, outputDir, numOfClients, load, numWarmUp, sendTime, 
					initiator, cSize, channelName);

			test.setup();

		} catch (Exception e) {
			e.printStackTrace();

		}
	}

	public void loop() {
		int c;
		System.out.println("Zab members are " + zabBox);
		;
		final String INPUT = "[1] Send start request to all clients \n[2] Reset the protocol \n[3] print Throughput and Min/Avg/Max latency \n[4] Change number  of message\n"
				+ "[4] calculate AB Message \n[5] Print Zab meberes \n"
				+ "[6] measure JGroups latency\n" + "";

		while (true) {
			try {
				c = Util.keyPress(String.format(INPUT));
				switch (c) {
				case '1':
					System.out.println("Start lenght ----->"
							+ clientThreads.length);
					for (int i = 0; i < clientThreads.length; i++) {
						clientThreads[i].init();
					}
					System.out.println("Start Test ----->");
					warmup();
					break;
				case '2':
					resetProtocol();
					break;
				case '3':
					callRemotePrintStats();
					break;
				case '4':
					calculateABMessage();
					break;
				case '5':
					sendMyAddressToLeader();
					break;
				case '6':
					try {
					} catch (Exception e) {
					}
					break;
				}
			} catch (Throwable t) {
				t.printStackTrace();
			}
		}

	}
    static class CustomMarshaller implements RpcDispatcher.Marshaller {

        public Buffer objectToBuffer(Object obj) throws Exception {
            MethodCall call=(MethodCall)obj;
            ByteBuffer buf;
            switch(call.getId()) {
                case SETUPCLIENTS:
                	   buf=ByteBuffer.allocate(Global.BYTE_SIZE);
                       buf.put((byte)call.getId());
                       return new Buffer(buf.array());
                case RESET:
	             	   buf=ByteBuffer.allocate(Global.BYTE_SIZE);
	                    buf.put((byte)call.getId());
	                    return new Buffer(buf.array());
                case STARTREALTEST:
	             	   buf=ByteBuffer.allocate(Global.BYTE_SIZE);
	                    buf.put((byte)call.getId());
	                    return new Buffer(buf.array());
                default:
                    throw new IllegalStateException("method " + call.getMethod() + " not known");
            }
        }



        public Object objectFromBuffer(byte[] buffer, int offset, int length) throws Exception {
            ByteBuffer buf=ByteBuffer.wrap(buffer, offset, length);

            byte type=buf.get();
            switch(type) {
                case SETUPCLIENTS:
                	 return new MethodCall(type);
                case RESET:
               	 	return new MethodCall(type);
                case STARTREALTEST:
               	 	return new MethodCall(type);
                default:
                    throw new IllegalStateException("type " + type + " not known");
            }
        }
    }


}
