package org.jgroups.protocols.jzookeeper.ZabCTvsABCast;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.jzookeeper.ZabCoinTossing;
import org.jgroups.protocols.jzookeeper.ZabCoinTossingHeader;
import org.jgroups.util.Util;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A test class to send abcasts between multiple nodes. This test assumes infinite clients in an AbaaS scenario (i.e
 * the ARP is always full).
 *
 * @author Ryan Emerson
 * @since 4.0
 */
public class InfiniteClients extends ReceiverAdapter {
    public static void main(String[] args) throws Exception{
    	String propsFile = "conf/ZabCoinTossing.xml";
		String outputDir = "";
		int msgSize = 1000;
		int numberOfMessages = 100000; // #Msgs to be executed by this node
		int totalMessages = 1000000; // #Msgs to be sent by the whole cluster
		int numOfNodes = 3;
		int numWarmUp = 10000;
		String initiator = "";
	    int numberOfNodes = -1;


		for (int i = 0; i < args.length; i++) {

			if ("-config".equals(args[i])) {
				propsFile = args[++i];
				continue;
			}
			if ("-warmup".equals(args[i])) {
				numWarmUp = Integer.parseInt(args[++i]);
				System.out.println(numWarmUp);
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
			if ("-msgSize".equals(args[i])) {
				msgSize = Integer.parseInt(args[++i]);
				continue;
			}
			if("-nodes".equals(args[i])) {
                numberOfNodes = Integer.parseInt(args[++i]);
                continue;
            }
			if ("-outputDir".equals(args[i])) {
				outputDir = args[++i];
				continue;
			}
			if ("-initiator".equals(args[i])) {
				initiator = args[++i];
				continue;
			}
        }

        new InfiniteClients(propsFile, numWarmUp, totalMessages, numberOfMessages, msgSize,
        		numberOfNodes, outputDir, initiator).run();
    }

    public static final AtomicInteger msgsReceived = new AtomicInteger();
    private final String PROPERTIES_FILE;
    private final int NUMBER_MESSAGES_TO_SEND;
    private int TOTAL_NUMBER_OF_MESSAGES;
    private final int NUMBER_MESSAGES_PER_FILE = 1000;
    private final int NUM_OF_WARMUP_REQUESTS;
    private final int MSGSIZE;
    private final int NUM_OF_NODES;
    private final int LATENCY_INTERVAL = 5000;
    private final String INITIATOR;
    private final String PATH = "/work/a6915654/ZabCT/";
    private final String OUTDIR;
    private final List<ZabCoinTossingHeader> deliveredMessages = new ArrayList<ZabCoinTossingHeader>();
    private final short id;
    private final short idTest = 1026;
    private ExecutorService outputThread = Executors.newSingleThreadExecutor();
    private JChannel channel;
    private int count = 1;
    private boolean startSending = false;
    private int completeMsgsReceived = 0;
    private long startTime;
    private boolean allMessagesReceived = false;
    private Future lastOutputFuture;
    int testCount = 0;

    private Map<Address, ZabCoinTossingHeader> msgRecord = new HashMap<Address, ZabCoinTossingHeader>();

    public InfiniteClients(String propsFile, int numberOfReqWarmup, int totalMessages, int numberOfMessages,
    		int msgSize, int numOfNodes, String ourDir, String initiator) {
        PROPERTIES_FILE = propsFile;
        NUM_OF_WARMUP_REQUESTS = numberOfReqWarmup;
        TOTAL_NUMBER_OF_MESSAGES = totalMessages;
        NUMBER_MESSAGES_TO_SEND = numberOfMessages;
        MSGSIZE = msgSize;
        NUM_OF_NODES = numOfNodes;
        OUTDIR = ourDir;
        INITIATOR = initiator;
		this.id = ClassConfigurator.getProtocolId(ZabCoinTossing.class);

    }

    public void run() throws Exception {
        System.out.println(PROPERTIES_FILE + " | Total Cluster Msgs " + TOTAL_NUMBER_OF_MESSAGES +
                " | Msgs to send " + NUMBER_MESSAGES_TO_SEND + " | Initiator " + INITIATOR);
        channel = new JChannel(PROPERTIES_FILE);
        channel.setReceiver(this);
        channel.connect("uperfBox");

        ClassConfigurator.add(idTest, TestHeader.class); // Add Header to magic map without adding to file
        System.out.println("Channel Created | lc := " + channel.getAddress());
        System.out.println("Number of message to send := " + NUMBER_MESSAGES_TO_SEND);

        Util.sleep(1000 * 30);
        int sentMessages = 0;
        startTime = System.nanoTime();

        if (channel.getAddress().toString().contains(INITIATOR))
            sendStartMessage(channel);

        while (true) {
            if (startSending) {
                final Message message = new Message(channel.getAddress(), sentMessages);
                message.putHeader(idTest, TestHeader.createTestMsg(sentMessages + 1));
                message.setBuffer(new byte[1000]);
                message.setSrc(channel.getAddress());
                channel.send(message);
                sentMessages++;
                if (sentMessages == NUMBER_MESSAGES_TO_SEND)
                    break;
            } else {
                Util.sleep(1);
            }
        }
        System.out.println("Sending finished! | Time Taken := " + TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS));

        while (!allMessagesReceived || channel.getView().size() != completeMsgsReceived || (lastOutputFuture == null || !lastOutputFuture.isDone()))
            Util.sleep(100);

        System.out.println("Test Finished");
        System.exit(0);
    }

    public void sendStartMessage(JChannel channel) throws Exception {
        sendStatusMessage(channel, new TestHeader(TestHeader.START_SENDING));
    }

    private void sendCompleteMessage(JChannel channel) throws Exception {
        sendStatusMessage(channel, new TestHeader(TestHeader.SENDING_COMPLETE));
    }

    private void sendStatusMessage(JChannel channel, TestHeader header) throws Exception {
//        Message message = new Message(new AnycastAddress(channel.getView().getMembers()));
        Message message = new Message(null);
        message.putHeader(idTest, header);
        message.setFlag(Message.Flag.NO_TOTAL_ORDER);
        channel.send(message);
    }
    
    private void printStats(JChannel channel) throws Exception{
    	Message message = new Message(null);
        message.putHeader(idTest, TestHeader.createStatsMsg(-2));
        channel.send(message);   	
    }
    public void receive(Message msg) {
        long arrivalTime = System.nanoTime();
        long timeTaken = -1;

        final TestHeader testHeader = (TestHeader) msg.getHeader(idTest);
        if (testHeader != null) {
            byte type = testHeader.type;
            if (type == TestHeader.START_SENDING && !startSending) {
                startSending = true;
                System.out.println("Start sending msgs ------");
                return;
            } else if (type == TestHeader.SENDING_COMPLETE) {
                completeMsgsReceived++;

                if (msg.src() != null)
                    System.out.println("Complete message received from " + msg.src());

                int numberOfNodes = channel.getView().getMembers().size();
                if (completeMsgsReceived ==  numberOfNodes) {
                    System.out.println("Cluster finished! | Time Taken := " + TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS));
                    try {
						printStats(channel);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
                }
                return;
            }
            timeTaken = arrivalTime - testHeader.timestamp;
        }

        synchronized (deliveredMessages) {
            ZabCoinTossingHeader headerCT = (ZabCoinTossingHeader)msg.getHeader(id);
            System.out.println(headerCT);
            testCount++;
            System.out.println(" $$ testCount $$ " + testCount);
            deliveredMessages.add(headerCT);

            if (deliveredMessages.size() % NUMBER_MESSAGES_PER_FILE == 0) {
                final List<ZabCoinTossingHeader> outputHeaders = new ArrayList<ZabCoinTossingHeader>(deliveredMessages);
                deliveredMessages.clear();
                lastOutputFuture = outputThread.submit(new Runnable() {
                    @Override
                    public void run() {
                        writeHeadersToFile(outputHeaders);
                    }
                });
            }

            if (msg.src().equals(channel.getAddress()) && testHeader != null && testHeader.seq % LATENCY_INTERVAL == 0) {
                final long tt = timeTaken;
                lastOutputFuture = outputThread.submit(new Runnable() {
                    @Override
                    public void run() {
                        writeLatencyToFile(testHeader.seq, tt);
                    }
                });
            }

            if (msgsReceived.incrementAndGet() == TOTAL_NUMBER_OF_MESSAGES) {
                try {
                    sendCompleteMessage(channel);
                    if (!deliveredMessages.isEmpty())
                        writeHeadersToFile(new ArrayList<ZabCoinTossingHeader>(deliveredMessages));
                } catch (Exception e) {
                }
            }
        }
    }

    public void viewAccepted(View view) {
        LogFactory.getLog(ZabCoinTossing.class).warn("New View := " + view + " | "  + " | Channel View := " + channel.getViewAsString());
    }

    private PrintWriter getPrintWriter(String path) {
        try {
            new File(PATH).mkdirs();
            return new PrintWriter(new BufferedWriter(new FileWriter(path, true)), true);
        } catch (Exception e) {
            System.out.println("Error: " + e);
            return null;
        }
    }

    private void writeHeadersToFile(List<ZabCoinTossingHeader> headersCT) {
        PrintWriter out = getPrintWriter(PATH + "DeliveredMessages" + channel.getAddress() + "-" + count + ".csv");
        for (ZabCoinTossingHeader headerCT : headersCT)
               out.println(headerCT);

        out.flush();

        int totalNumberOfRounds = (int) Math.round((TOTAL_NUMBER_OF_MESSAGES) / (double) NUMBER_MESSAGES_PER_FILE);
        if (count == totalNumberOfRounds) {
            allMessagesReceived = true;
            System.out.println("&&&&&&&& Final Count := " + count + " | totalNumberOfRounds := " + totalNumberOfRounds +
                    " | numberOfMessagesToSend := " + NUMBER_MESSAGES_TO_SEND + " | NUMBER_MESSAGES_PER_FILE := " + NUMBER_MESSAGES_PER_FILE);
        }
        System.out.println("Count == " + count + " | numberOfRounds := " + totalNumberOfRounds);
        count++;
    }

    private void writeLatencyToFile(int count, long timeTaken) {
        PrintWriter out = getPrintWriter(PATH + "Latencies" + channel.getAddress() + ".csv");
        out.println(count + "," + timeTaken);
    }
    
    

    public static class TestHeader extends Header {
        public static final byte START_SENDING = 1;
        public static final byte SENDING_COMPLETE = 2;
        public static final byte TEST_MSG = 3;
        public static final byte STATS = 4;

        private byte type;
        private int seq = -1;
        private long timestamp = -1;

        public TestHeader() {}

        public TestHeader(byte type) {
            this.type = type;
        }

        public TestHeader(byte type, int seq, long timestamp) {
            this.type = type;
            this.seq = seq;
            this.timestamp = timestamp;
        }

        public static TestHeader createTestMsg(int seq) {
            return new TestHeader(TEST_MSG, seq, System.nanoTime());
        }
        
        public static TestHeader createStatsMsg(int seq) {
            return new TestHeader(STATS, seq, System.nanoTime());
        }
        
        

        public byte getType() {
			return type;
		}

		public void setType(byte type) {
			this.type = type;
		}

		public int getSeq() {
			return seq;
		}

		public void setSeq(int seq) {
			this.seq = seq;
		}

		public long getTimestamp() {
			return timestamp;
		}

		public void setTimestamp(long timestamp) {
			this.timestamp = timestamp;
		}

		@Override
        public int size() {
            return Global.BYTE_SIZE;
        }

        @Override
        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            out.writeInt(seq);
            out.writeLong(timestamp);
        }

        @Override
        public void readFrom(DataInput in) throws Exception {
            type = in.readByte();
            seq = in.readInt();
            timestamp = in.readLong();
        }

        @Override
        public String toString() {
            return "TestHeader{" +
                    "type=" + type +
                    ", seq=" + seq +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }
}