package org.jgroups.protocols.jzookeeper;

import org.jgroups.*;

import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.jgroups.protocols.jzookeeper.ZabCoinTossingClient.Sender;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.Version;
import org.jgroups.View;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

public class ZabCoinTossingTest extends ReceiverAdapter {
	private List<String> zabboxInit = new ArrayList<String>();
	private String propsFile = "conf/ZabCoinTossing.xml";
	private static String ProtocotName = "ZabCoinTossing";
	private JChannel channel;
	private Address local_addr = null;
	private List<Address> zabBox = new ArrayList<Address>();
	private View view;
	private static long num_msgs = 100000;
	private static long num_msgsPerThreads = 10000;
	private static int msg_size = 20000;
	private static int numOfClients = 10;
	private static int num_threads = 10;
	private long log_interval = num_msgs / 10; 
	private long receive_log_interval = Math.max(1, num_msgs / 10);
	private ZabCoinTossingClient[] clientThreads=null;
	private final byte[] payload = new byte[msg_size];
	private AtomicLong localSequence = new AtomicLong(); 
	private long incMainRequest = 0;
	private short ID;
	
    private String outputDir;
    private static PrintWriter outFile;
    private static int numsThreadFinished = 0;
    private static int avgTimeElpased = 0;
    private static long avgRecievedOps = 0;
    private static int load = 1;
    private static boolean is_warmUp = false;
    private long numsOfWarmUpPerThread = 0;
    private int countTempRecieved = 0;
    private int countRecieved = 0;
    private Scanner wait = new Scanner(System.in);


	public ZabCoinTossingTest(String [] zabHosts, String protocolName, String props,
						 int totalNum_msgs, int totalPerThreads, int num_threads,
						 int msg_size, String outputDir, int numOfClients, int load, int numsOfWarmUpPerThread){
		this.zabboxInit =  Arrays.asList(zabHosts);
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
		this.ID = ClassConfigurator
			.getProtocolId(ZabCoinTossing.class);
	}

	public ZabCoinTossingTest() {
	}

	public void viewAccepted(View new_view) {
		System.out.println("** view: " + new_view);
		view = new_view;
		List<Address> mbrs = new_view.getMembers();
		if (mbrs.size() == 3) {
			zabBox.addAll(new_view.getMembers());
		}

		if (mbrs.size() > 3 && zabBox.isEmpty()) {
			for (int i = 0; i < 3; i++) {
				zabBox.add(mbrs.get(i));
			}
		}
		local_addr=channel.getAddress();
		
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
		channel.connect("ZABCluster");
		JmxConfigurator.registerChannel(channel, Util.getMBeanServer(),
				"jgroups", "ZABCluster", true);
		Address coord = channel.getView().getMembers().get(0);
	}
	
	public void setupClientThreads(){

		final CyclicBarrier barrier = new CyclicBarrier(num_threads + 1);
		clientThreads = new ZabCoinTossingClient[num_threads];
		System.out.println("after clientThreads = new ZabCoinTossingClient[num_threads];");
		if (!zabboxInit.contains(local_addr.toString().split("-")[0])) {
			for (int i = 0; i < clientThreads.length; i++) {
				clientThreads[i] = new ZabCoinTossingClient(zabBox, barrier, num_msgs,
						localSequence, payload, ProtocotName, num_msgsPerThreads, propsFile, load, numsOfWarmUpPerThread, this);
			}
		}
		System.out.println("after for (int i = 0; i < clientThreads.length; i++)");
		if ((view != null) && zabBox.size() != 0
				&& view.getMembers().size() > 3 && !zabBox.contains(local_addr)) {
			int noUsed = wait.nextInt();
			for (int i = 0; i < clientThreads.length; i++) {
				try {
					clientThreads[i].start();
					Thread.sleep(1000);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}
		// Send local address to leader
		MessageId mid = new MessageId(local_addr,
				localSequence.incrementAndGet());
		ZabCoinTossingHeader startHeader = new ZabCoinTossingHeader(ZabCoinTossingHeader.SENDMYADDRESS, -6, mid);
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
		//System.out.println("inside sendStartSign");
		MessageId mid = new MessageId(local_addr,
				localSequence.incrementAndGet());
		ZabCoinTossingHeader startHeader = new ZabCoinTossingHeader(ZabCoinTossingHeader.START_SENDING, -5, mid);
		Message msg = new Message(null).putHeader(ID, startHeader);
		msg.setObject("req");
		channel.send(msg);
	}
	
	public void resetProtocol() throws Exception {
		numsThreadFinished=0;
		avgTimeElpased =0;
		avgRecievedOps=0;
		is_warmUp =false;
		this.outFile = new PrintWriter(new BufferedWriter(new FileWriter
				(outputDir+InetAddress.getLocalHost().getHostName()+".log",true)));
		MessageId mid = new MessageId(local_addr,
				localSequence.incrementAndGet());
		ZabCoinTossingHeader resetProtocol = new ZabCoinTossingHeader(ZabCoinTossingHeader.RESET, -4,  mid);
		Message msg = new Message(null).putHeader(ID, resetProtocol);
		channel.send(msg);
	}
	
	public void callRemotePrintStats() throws Exception{
		MessageId mid = new MessageId(local_addr,
				localSequence.incrementAndGet());
		ZabCoinTossingHeader stats = new ZabCoinTossingHeader(ZabCoinTossingHeader.STATS, -3, mid);
		Message msg = new Message(null).putHeader(ID, stats);
		channel.send(msg);
		
	}
	
	public void calculateABMessage() throws Exception{
		MessageId mid = new MessageId(local_addr,
				localSequence.incrementAndGet());
		ZabCoinTossingHeader stats = new ZabCoinTossingHeader(ZabCoinTossingHeader.COUNTMESSAGE, -2, mid);
		Message msg = new Message(null).putHeader(ID, stats);
		channel.send(msg);
		
	}
	
	
	public void sendTestRequest() throws Exception {
		//System.out.println("inside sendStartSign");
		int numSendMsg=0;
		for (int i = 0; i < 1000; i++) {
			numSendMsg=i;
			while ((numSendMsg - countTempRecieved) > load){
				try {
					Thread.sleep(0,1);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}
			System.out.println(" Sending " + i + " out of " + 1000);

			MessageId mid = new MessageId(local_addr,
					localSequence.incrementAndGet());
			//ZabCoinTossingHeader startHeader = new ZabCoinTossingHeader(ZabCoinTossingHeader.TEMPSENT,i, mid);
			//Message msg = new Message(null).putHeader(ID, startHeader);
			//channel.send(msg);
		}
		
	}
	
	public void receive(Message msg) {
		final ZabCoinTossingHeader testHeader = (ZabCoinTossingHeader) msg.getHeader(ID);
		
			//if (testHeader.getType() == ZabCoinTossingHeader.START_SENDING) {
				//numsThreadFinished=0;
				//avgTimeElpased =0;
				//avgRecievedOps=0;
				//is_warmUp = true;
				//System.out.println("Warm up starts");

				//for (int i = 0; i < clientThreads.length; i++) {
				//	System.out.println("inside for i = "+i);
				//	clientThreads[i].setWarmUp(true);
				//	clientThreads[i].sendMessages(numsOfWarmUpPerThread);				
			  //  }
				//try {
				//	this.outFile = new PrintWriter(new BufferedWriter(new FileWriter
			//				(outputDir+InetAddress.getLocalHost().getHostName()+".log",true)));
			//	} catch (IOException e) {
					// TODO Auto-generated catch block
			//		e.printStackTrace();
			//	}
	       // }
		    if (testHeader.getType() == ZabCoinTossingHeader.STARTREALTEST){
				System.out.println("!!!Recieved STARTREALTEST from "+msg.getSrc());

				//synchronized(this){
					//countRecieved++;
					//if (countRecieved==zabBox.size())
						startRealTest();
			//	}
			}
			//else if (testHeader.getType() == ZabCoinTossingHeader.RESPONCETEMP){
				//countTempRecieved++;
			//}
	}
	
	public void sendMyAddressToLeader(){
		MessageId mid = new MessageId(local_addr,
				localSequence.incrementAndGet());
		ZabCoinTossingHeader startHeader = new ZabCoinTossingHeader(ZabCoinTossingHeader.SENDMYADDRESS, -6, mid);
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
	
		public void warmup(){
		numsThreadFinished=0;
		avgTimeElpased =0;
		avgRecievedOps=0;
		is_warmUp = true;
		System.out.println("Warm up starts");

		for (int i = 0; i < clientThreads.length; i++) {
			System.out.println("inside for i = "+i);
			clientThreads[i].setWarmUp(true);
			clientThreads[i].sendMessages(numsOfWarmUpPerThread);				
	    }
		try {
			this.outFile = new PrintWriter(new BufferedWriter(new FileWriter
					(outputDir+InetAddress.getLocalHost().getHostName()+".log",true)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
	
		public void sendFinishedNotifcation(){
			MessageId mid = new MessageId(local_addr,
					localSequence.incrementAndGet());
			ZabCoinTossingHeader finishedHeader = new ZabCoinTossingHeader(ZabCoinTossingHeader.CLIENTFINISHED, -11, mid);
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
	
	public synchronized void finishedSend() throws Exception{
		numsThreadFinished++;
		if (numsThreadFinished >= num_threads){
			System.out.println("Finished warm up----------------------------->>>");
			numsThreadFinished=0;
			resetProtocol();
	  }
	}
	
	public void startRealTest(){
		System.out.println("Real test starts");

		for (int i = 0; i < clientThreads.length; i++) {
			clientThreads[i].init();
		}

		for (int i = 0; i < clientThreads.length; i++) {
			System.out.println("inside for i = "+i);
			clientThreads[i].setWarmUp(false);
			clientThreads[i].sendMessages(num_msgsPerThreads);				
	    }
	}

	//public synchronized static void result(long numOpsRecieved, Sender sender,
			                            //  long timeElapsed, List<Long> latencies){
	public synchronized void finishedTest(){

		numsThreadFinished++;
		//numsThreadFinished++;
		if (numsThreadFinished >= num_threads){
			System.out.println("Finished");
			sendFinishedNotifcation();
		}
		
//		numsThreadFinished++;
//		avgTimeElpased +=timeElapsed;
//		avgRecievedOps+=numOpsRecieved;
//		if (numsThreadFinished==1){
//			outFile.println();
//			outFile.println();
//			outFile.println("Test "+ProtocotName + " /Load " + load + " /numMsgs For Each Thread " + num_msgsPerThreads+
//					" /numMsgs For Each Client " + num_msgs+ " /numsThreads "+num_threads+" /msg size " + msg_size+
//					" /numOfClients All Cluster " + numOfClients+" /numMsgs For All Cluster "+ num_msgs*numOfClients);
//			outFile.println("------------------------ Result --------------------------");
//		}
//		// print Min, Avg, and Max latency
//		long min = Long.MAX_VALUE, avg =0, max = Long.MIN_VALUE;
//		for (long lat : latencies){
//			if (lat < min){
//				min = lat;
//			}
//			if (lat > max){
//				max = lat;
//			}
//			avg+=lat;			
//		}
//		outFile.println("Sender " + sender.getName()+ " Finished "+
//				numOpsRecieved + " Throughput per sender "+(numOpsRecieved/TimeUnit.MILLISECONDS.toSeconds(timeElapsed))+" ops/sec"
//				+" /Latency-----> Min = " + min + " /Avg = "+ (avg/latencies.size())+
//		        " /Max = " +max);
//		if (numsThreadFinished >= num_threads){
//			avgTimeElpased/=numsThreadFinished;
//			outFile.println("Throughput Per Client " +(avgRecievedOps/TimeUnit.MILLISECONDS.toSeconds(avgTimeElpased))+" ops/sec");
//			outFile.println("Throughput All Cluster " +((avgRecievedOps*numOfClients)/TimeUnit.MILLISECONDS.toSeconds(avgTimeElpased))
//					+" ops/sec");
//		    outFile.println("Test Generated at "+ new Date()+ " Lasted for " + TimeUnit.MILLISECONDS.toSeconds(avgTimeElpased)); 
//			System.out.println("File closed" +"############################################"); 
//			outFile.close();	
//		}

}

	public static void main(String[] args) {
		String propsFile = "conf/ZabCoinTossing.xml";
		String name ="ZabCoinTossing";
		String outputDir= "/home/pg/p13/a6915654/"+name+"/";
		String [] zabboxInits= new String[3];
        int msgSize = 1000;
        int load = 1;
        int numsThreads= 10;
        int numberOfMessages= 100000; // #Msgs to be executed by this node
        int totalMessages= 1000000; // #Msgs to be sent by the whole cluster
        int numOfClients= 10; 
        int numWarmUp= 10000; 


        for (int i = 0; i < args.length; i++) {

             	 
        	if("-warmup".equals(args[i])) {
                numWarmUp = Integer.parseInt(args[++i]);
                System.out.println(numWarmUp);
                continue;
            }
        	
        	if("-load".equals(args[i])) {
                load = Integer.parseInt(args[++i]);
                System.out.println(load);
                continue;
            }
        	
        	if("-config".equals(args[i])) {
                propsFile = args[++i];
                continue;
            }
        	if ("-hosts".equals(args[i])){
				zabboxInits = args[++i].split(","); 
		    }
        	 if("-name".equals(args[i])) {
                name = args[++i];
                continue;
            }
        	 if("-tmessages".equals(args[i])){
                totalMessages = Integer.parseInt(args[++i]);
                continue;
            }
        	 if("-nmessages".equals(args[i])) {
                numberOfMessages = Integer.parseInt(args[++i]);
                continue;
            }
        	 if("-threads".equals(args[i])) {
            	numsThreads = Integer.parseInt(args[++i]);
                continue;
            }
        	 if("-msgSize".equals(args[i])) {
            	msgSize = Integer.parseInt(args[++i]);
                continue;
            }
        	 if("-outputDir".equals(args[i])) {
            	outputDir = args[++i];
                continue;
            }
        	 if("-numClients".equals(args[i])) {
        		 numOfClients = Integer.parseInt(args[++i]);
                 continue;
             }
        	 
        }
        System.out.println("propsFile "+propsFile+ "/" + "zabboxInits "+ zabboxInits+ "/"+"name "+name+
				"/"+"totalMessages "+totalMessages+"/"+"numberOfMessages "+numberOfMessages+"/"+"numsThreads "+
				numsThreads+"/"+"msgSize" +msgSize+"/"+"outputDir "+outputDir+"/"+"numOfClients "+numOfClients+
				"/"+"load "+load+"/"+"numWarmUp "+numWarmUp);
		try {
			
			final ZabCoinTossingTest test = new ZabCoinTossingTest(zabboxInits, name, propsFile, totalMessages,
															numberOfMessages, numsThreads, msgSize, 
															outputDir, numOfClients, load, numWarmUp);
			
			test.setup();
			test.setupClientThreads();
			test.loop();

	} catch (Exception e) {
		e.printStackTrace();
	}

	}
	
	public void loop() {
		int c;
		System.out.println("ZabCoinTossing members are "+ zabBox);;
		final String INPUT = "[1] Send start request to all clients \n[2] Reset the protocol \n[3] print Throughput and Min/Avg/Max latency \n[4] Change number  of message\n"
				+ "[4] calculate AB Message \n[5] Print Zab meberes \n"
				+ "[6] measure JGroups latency\n"+ "";

		while (true) {
			try {
				c = Util.keyPress(String.format(INPUT));
				switch (c) {
				case '1':
        System.out.println("Start lenght ----->" + clientThreads.length);
					for (int i = 0; i < clientThreads.length; i++) {
						clientThreads[i].init();
					}
					System.out.println("Start Test ----->");
					warmup();
					//sendStartSign();
					break;
				case '2':
					resetProtocol();
					// System.out.println("view: " + channel.getView() +
					// " (local address=" + channel.getAddress() + ")");
					break;
				case '3':
					 callRemotePrintStats();
					//System.out.println("Enter number of message");
					// num_msgs = read.nextLong();
					break;
				case '4':
					calculateABMessage();
					//System.out.println("Enter Size of message");
					//msg_size = read.nextInt();
					break;
				case '5':
					sendMyAddressToLeader();
					break;
				case '6':
					
					// ProtocolStack stack=channel.getProtocolStack();
					// String cluster_name=channel.getClusterName();
					try {
						// JmxConfigurator.unregisterChannel(channel,
						// Util.getMBeanServer(), "jgroups", "ChatCluster");
					} catch (Exception e) {
					}
					// stack.stopStack(cluster_name);
					// stack.destroy();
					break;
				}
			} catch (Throwable t) {
				t.printStackTrace();
			}
		}

	}

}
