package org.jgroups.protocols.jzookeeper;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.jgroups.Address;
import org.jgroups.AnycastAddress;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

/*
 * It is orignal protocol of Apache Zookeeper. Also it has features of testing throuhput, latency (in Nano), ant etc. 
 * When using testing, it provides warm up test before starting real test.
 */

public class Zab_00 extends Protocol {
	private AtomicLong local = new AtomicLong(0);// , actually_sent;
	private final AtomicLong                  zxid=new AtomicLong(0);
    private ExecutorService                     executor;
    private Address                           local_addr;
    private volatile Address                  leader;
    private int QUEUE_CAPACITY = 500;
    private volatile View                     view;
    private volatile boolean                  is_leader=false;
    private List<Address>                       zabMembers = Collections.synchronizedList(new ArrayList<Address>());
	private long                                lastZxidProposed=0, lastZxidCommitted=0;
    private final Set<MessageId>                requestQueue =Collections.synchronizedSet(new HashSet<MessageId>());
	private Map<Long, ZabHeader> queuedCommitMessage = new HashMap<Long, ZabHeader>();
	private List<Integer> latencies = new ArrayList<Integer>();
	private List<Integer> avgLatencies = new ArrayList<Integer>();
	private List<String> avgLatenciesTimer = new ArrayList<String>();

    private final Map<Long, ZabHeader> queuedProposalMessage = Collections.synchronizedMap(new HashMap<Long, ZabHeader>());
    private final LinkedBlockingQueue<ZabHeader> queuedMessages =
	        new LinkedBlockingQueue<ZabHeader>();
	private ConcurrentMap<Long, Proposal> outstandingProposals = new ConcurrentHashMap<Long, Proposal>();
    private final Map<MessageId, Message> messageStore = Collections.synchronizedMap(new HashMap<MessageId, Message>());
    private Calendar cal = Calendar.getInstance();
    private int index=-1;
    private volatile boolean                  running=true;
    private long startThroughputTime = 0;
    private long endThroughputTime = 0;
    private volatile boolean startThroughput = false;
    private static PrintWriter outFile;
    private final static String outDir="/home/pg/p13/a6915654/ZAB/";
	private  AtomicInteger                  numReqDeviverd=new AtomicInteger(0);
	private  AtomicInteger                  numRequest=new AtomicInteger(0);
	private long rateInterval = 10000;
	private long rateCount = 0;
	private long currentCpuTime = 0, rateCountTime = 0, lastTimer = 0, lastCpuTime = 0;
	private int lastArrayIndex = 0, lastArrayIndexUsingTime = 0 ;
	private long timeInterval = 500;
	private int lastFinished = 0;
	private Timer timer;
	private AtomicLong countMessageLeader = new AtomicLong(0);
	private long countMessageFollower = 0;
	private long countTotalMessagesFollowers = 0;
	private AtomicLong warmUpRequest = new AtomicLong(0);
	private static long warmUp = 10000;
    private int largeLatCount = 0;
    private List<String> largeLatencies = new ArrayList<String>();
    private List<String> largeLatenciesNano = new ArrayList<String>();
    private boolean is_warmUp=true;
    
    public Zab_00(){
    	
    }
    
    @ManagedAttribute
    public boolean isleaderinator() {return is_leader;}
    public Address getleaderinator() {return leader;}
    public Address getLocalAddress() {return local_addr;}

    @ManagedOperation
    public String printStats() {
        return dumpStats().toString();
    }

    @Override
    public void start() throws Exception {
        super.start();
        running=true;    
	    executor = Executors.newSingleThreadExecutor();
	    executor.execute(new FollowerMessageHandler(this.id));
	    log.setLevel("trace");
	    this.outFile = new PrintWriter(new BufferedWriter(new FileWriter
				(outDir+InetAddress.getLocalHost().getHostName()+"Zab.log",true)));
	    
    }


    public void reset(Address client) {
    	zxid.set(0); lastZxidProposed=0;  	
    	lastZxidCommitted=0; requestQueue.clear();        
    	queuedCommitMessage.clear(); queuedProposalMessage.clear();         
        queuedMessages.clear(); outstandingProposals.clear();       
        messageStore.clear(); latencies.clear();      
        avgLatencies.clear(); avgLatenciesTimer.clear();       
        numReqDeviverd= new AtomicInteger(0); numRequest= new AtomicInteger(0);      
        startThroughputTime = 0; endThroughputTime = 0;
        startThroughput = false; currentCpuTime = 0;       
        rateCountTime = 0; lastTimer = 0;      
        lastCpuTime = 0; lastArrayIndex = 0;    	
    	lastArrayIndexUsingTime = 0 ; lastFinished = 0;    	
    	countMessageLeader = new AtomicLong(0);//	 timer.cancel();    	
    	largeLatCount=0; countMessageFollower=0;    	
    	countTotalMessagesFollowers=0; rateCount=0;
    	is_warmUp = false;
    	try {
			this.outFile = new PrintWriter(new BufferedWriter(new FileWriter
					(outDir+InetAddress.getLocalHost().getHostName()+"Zab.log",true)));
    	}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	log.info("Reset done");
    	MessageId messageId = new MessageId(local_addr,
				-10, System.currentTimeMillis());
    	ZabHeader startTest = new ZabHeader(ZabHeader.STARTREALTEST, messageId);
        Message confirmClient=new Message(client).putHeader(this.id, startTest);
		down_prot.down(new Event(Event.MSG, confirmClient));     
	    
    }
    @Override
    public void stop() {
        running=false;
        executor.shutdown();
        super.stop();
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
            	Message m = (Message) evt.getArg();
                handleClientRequest(evt, m);
                return null; // don't pass down
            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
        }
        return down_prot.down(evt);
    }

    public Object up(Event evt) {
        Message msg = null;
        ZabHeader hdr;

        switch(evt.getType()) {
            case Event.MSG:
                msg=(Message)evt.getArg();
                hdr=(ZabHeader)msg.getHeader(this.id);
                if(hdr == null){
                    break; // pass up
                }
                switch(hdr.getType()) {                
                   case ZabHeader.START_SENDING:
                	   if(!zabMembers.contains(local_addr))
                		   return up_prot.up(new Event(Event.MSG, msg));
                	   break;
                	case ZabHeader.REQUEST:        
  //              		if (!is_leader && !startThroughput){
//                			startThroughput = true;
//                			startThroughputTime = System.currentTimeMillis();
//                		      timer = new Timer();
//                			  timer.schedule(new ResubmitTimer(), timeInterval, timeInterval);
	                	    //numRequest.incrementAndGet();
                    		//queuedMessages.add(hdr);
                		//}
	                		forwardToLeader(msg);                		
                		break;
                	case ZabHeader.RESET:
                		 reset(msg.getSrc());
  	                	break;
                    case ZabHeader.FORWARD:   
//                    	if (warmUpRequest.get() <= warmUp){
//                    		warmUpRequest.incrementAndGet();
//                    		queuedMessages.add(hdr);
//                		}
                		// if (warmUpRequest.get() > warmUp && !startThroughput){
//                    	`startThroughputTime = System.currentTimeMillis();
//                		   // timer = new Timer();
//                			//timer.schedule(new ResubmitTimer(), timeInterval, timeInterval);
	                	    //numRequest.incrementAndGet();
                    		//queuedMessages.add(hdr);
                		//}
                		//else if (startThroughput){
	              	    //numRequest.incrementAndGet();
                   		queuedMessages.add(hdr);

                		//}
//                    	if(!is_leader) {
//                			if(log.isErrorEnabled())
//                				//log.error("[" + local_addr + "] "+ ": non-Leader; dropping FORWARD request from " + msg.getSrc());
//                			break;
//                		 }         
                    	
                	    //hdr.getMessageId().setStartTime(System.currentTimeMillis());
                		//queuedMessages.add(hdr);
                		break;
                    case ZabHeader.PROPOSAL:
                    	if (!is_leader){
	                   		if (!is_warmUp && !startThroughput){
	                			startThroughput = true;
	                			startThroughputTime = System.currentTimeMillis();
	                		    //timer = new Timer();
	                			//timer.schedule(new ResubmitTimer(), timeInterval, timeInterval);
		 
	                		}
	                   		//hdr.getMessageId().setStartTime(System.currentTimeMillis());
	            			sendACK(msg, hdr);
	            		}
	                   	break;          		
                    case ZabHeader.ACK:
                		if (is_leader){
                			processACK(msg, msg.getSrc());
                		}
                		break;
                    case ZabHeader.COMMIT:
                    		deliver(hdr.getZxid());
                		 break;
                    case ZabHeader.STATS:
                    	//if(is_leader)
                    		printMZabStats();
                    	break;
                    case ZabHeader.COUNTMESSAGE:
                    		sendTotalABMwssages(hdr);  
                    		log.info("Yes, I recieved count request");
                    	break;
                    case ZabHeader.STARTREALTEST:
                    	if(!zabMembers.contains(local_addr))
                			return up_prot.up(new Event(Event.MSG, msg));
                    case ZabHeader.RESPONSE:
                    	handleOrderingResponse(hdr);
                    	
            }                
                return null;
              case Event.VIEW_CHANGE:
              handleViewChange((View)evt.getArg());
              break;

        }

        return up_prot.up(evt);
    }

    public void up(MessageBatch batch) {
        for(Message msg: batch) {
            if(msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER) || msg.isFlagSet(Message.Flag.OOB) || msg.getHeader(id) == null)
                continue;
            batch.remove(msg);

            try {
                up(new Event(Event.MSG, msg));
            }
            catch(Throwable t) {
                log.error("failed passing up message", t);
            }
        }

        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    /* --------------------------------- Private Methods ----------------------------------- */

    
	private void handleClientRequest(Event event, Message message) {
		ZabHeader clientHeader = null;
        Address destination = message.getDest();
        Address zabDestination = null;
        //Store put here, and Forward write to Zab box to obtain otdering
        if (destination != null && destination instanceof AnycastAddress && !message.isFlagSet(Message.Flag.NO_TOTAL_ORDER)) {
			log.info("-------> AnycastAddress Send to Zab boxes message " + message);
        	MessageId messageId = new MessageId(local_addr,
			local.getAndIncrement(), System.currentTimeMillis());
        	messageStore.put(messageId, message);
			log.info("handleClientRequest---> " + messageId);
			log.info("handleClientRequest---> " + messageId.getId());
			down_prot.up(new Event(Event.MSG, message));

        	ZabHeader hdrReq = new ZabHeader(ZabHeader.REQUEST,
			messageId);
        	++index;
        	if (index > 2)
        		index = 0;
        	zabDestination = zabMembers.get(index);
        	Message requestMessage = new Message(zabDestination).putHeader(
			this.id, hdrReq);
        	int diff = 1000 - hdrReq.size();
        	 if (diff > 0)
        	message.setBuffer(new byte[diff]); // Necessary to ensure that each msgs size is 1kb, necessary for accurate network measurements
        	down_prot.down(new Event(Event.MSG, requestMessage));
        }
        //Forward start call to all destinations to start the benchmark
        else if (destination != null && !(destination instanceof AnycastAddress)) {
			log.info("-------> Send to client only");
			log.info("Start    ---> " + message);

			down_prot.down(new Event(Event.MSG, message));
		}


	}
    
    private void handleViewChange(View v) {
        List<Address> mbrs=v.getMembers();
        leader=mbrs.get(0);
        if (leader.equals(local_addr)){
        	is_leader = true;
        }
        if (mbrs.size() == 3){
        	zabMembers.addAll(v.getMembers());        	
        }
        if (mbrs.size() > 3 && zabMembers.isEmpty()){
        	for (int i = 0; i < 3; i++) {
        		zabMembers.add(mbrs.get(i));
			}

        }
        if(mbrs.isEmpty()) return;

        if(view == null || view.compareTo(v) < 0)
            view=v;
        else
            return;
    }

    private long getNewZxid(){
    	return zxid.incrementAndGet();
    }

    private void forwardToLeader(Message msg) {
    	ZabHeader hdrReq = (ZabHeader) msg.getHeader(this.id);
    	log.info("message id = "+hdrReq.getMessageId());
 	   requestQueue.add(hdrReq.getMessageId());
    	if (!is_warmUp && is_leader && !startThroughput){
			startThroughput = true;
			startThroughputTime = System.currentTimeMillis();
		      //timer = new Timer();
			  //timer.schedule(new ResubmitTimer(), timeInterval, timeInterval);
		}
	   
	   if (is_leader){
		   hdrReq.getMessageId().setStartTime(System.nanoTime());
		   queuedMessages.add((ZabHeader)msg.getHeader(this.id));
       }	   
	   else{
		   hdrReq.getMessageId().setStartTime(System.nanoTime());
		   forward(msg);
	   }
            
           
   }

    
    private void forward(Message msg) {
        Address target=leader;
 	    ZabHeader hdrReq = (ZabHeader) msg.getHeader(this.id);
        if(target == null)
            return;
 
	    try {
	        ZabHeader hdr=new ZabHeader(ZabHeader.FORWARD, hdrReq.getMessageId());
	        Message forward_msg=new Message(target).putHeader(this.id,hdr);
	        down_prot.down(new Event(Event.MSG, forward_msg));
	     }
	    catch(Exception ex) {
	      log.error("failed forwarding message to " + msg, ex);
	    }
      
    }
    
    private void sendACK(Message msg, ZabHeader hdrAck){
   		numRequest.incrementAndGet();
    	if (msg == null )
    		return;
    	
    	//ZABHeader hdr = (ZABHeader) msg.getHeader(this.id);
    	if (hdrAck == null)
    		return;
    	
    	//if (hdr.getZxid() != lastZxidProposed + 1){
//            log.info("Got zxid 0x"
//                    + Long.toHexString(hdr.getZxid())
//                    + " expected 0x"
//                    + Long.toHexString(lastZxidProposed + 1));
        //}
    	
    	lastZxidProposed = hdrAck.getZxid();
    	queuedProposalMessage.put(hdrAck.getZxid(), hdrAck);
		ZabHeader hdrACK = new ZabHeader(ZabHeader.ACK, hdrAck.getZxid(), hdrAck.getMessageId());
		Message ACKMessage = new Message(leader).putHeader(this.id, hdrACK);
		if(!is_warmUp)
			countMessageFollower++;
		try{
    		down_prot.down(new Event(Event.MSG, ACKMessage));   
         }catch(Exception ex) {
    		log.error("failed sending ACK message to Leader");
    	} 
		//deliver(hdrAck.getZxid());
		
		
    }
    
    
    private synchronized void processACK(Message msgACK, Address sender){
	   
    	ZabHeader hdr = (ZabHeader) msgACK.getHeader(this.id);	
    	long ackZxid = hdr.getZxid();
		if (lastZxidCommitted >= ackZxid) {
            return;
        }
        Proposal p = outstandingProposals.get(ackZxid);
        if (p == null) {
            return;
        }		
		p.AckCount++;
		//if (log.isDebugEnabled()) {
//            log.debug("Count for zxid: " +
//                    Long.toHexString(ackZxid)+" = "+ p.getAckCount());
        //}
		
		if(isQuorum(p.getAckCount())){ 	
 		   // log.info("Commiting processACK >>>>>>>>>>>>>"+ ackZxid);
			if(ackZxid == lastZxidCommitted+1){
	            outstandingProposals.remove(ackZxid);
	            commit(ackZxid);	
			}
			else
				System.out.println(">>> Can't commit >>>>>>>>>");
		}
			
			
		}
		
    private void commit(long zxidd){
	    ZabHeader hdrOrg = queuedProposalMessage.get(zxidd);
   	    //log.info("Czxid = "+ hdrOrg.getZxid() + " " + getCurrentTimeStamp());
	    synchronized(this){
 	       lastZxidCommitted = zxidd;
 	   }

		   if (hdrOrg == null){
			   log.info("??????????????????????????? Header is null (commit)"+ hdrOrg + " for zxid "+zxidd);
			   return;
		   }

	       ZabHeader hdrCommit = new ZabHeader(ZabHeader.COMMIT, zxidd);
	       Message commitMessage = new Message().putHeader(this.id, hdrCommit);	       
           for (Address address : zabMembers) {
        	   //if(!address.equals(leader) && startThroughput)
        	   if(!address.equals(leader))
             	  	countMessageLeader.incrementAndGet();
	              Message cpy = commitMessage.copy();
	              cpy.setDest(address);
	   		      //log.info("[" + local_addr + "] "+ "YYYYYYYY sending comit message zxid to = "+zxidd+":"+address);
	              down_prot.down(new Event(Event.MSG, cpy));   
              
        	   
           }
           //deliver(zxidd);
		   // log.info("Commiting commit >>>>>>>>>>>>>"+ zxidd);


	    }
		
    private void deliver(long dZxid){
		//ZABHeader hdrOrginal = null;
		//ZABHeader hdr = (ZABHeader) toDeliver.getHeader(this.id);
		//long zxid = hdr.getZxid();

//		log.info("[" + local_addr + "] "
//				+ " recieved commit message (deliver) for zxid="
//				+ hdr.getZxid() + " " + getCurrentTimeStamp());
		//log.info("Dzxid = "+ hdr.getZxid() + " " + getCurrentTimeStamp());
		ZabHeader hdrOrginal = queuedProposalMessage.remove(dZxid);
		if (hdrOrginal == null) {
			 if (log.isInfoEnabled())
				 log.info("$$$$$$$$$$$$$$$$$$$$$ Header is null (deliver)"
					+ hdrOrginal + " for zxid " + dZxid);
			return;
		}
//		log.info("!!!!!!!!!!!! check if (requestQueue.contains(hdrOrginal.getMessageId() (deliver)"
//				+ requestQueue.contains(hdrOrginal.getMessageId()));
		//numReqDeviverd.incrementAndGet();

		queuedCommitMessage.put(dZxid, hdrOrginal);
		//synchronized(latencies){
			if(!is_warmUp){
				numReqDeviverd.incrementAndGet();
				endThroughputTime = System.currentTimeMillis();

				//endThroughputTime = System.currentTimeMillis();
				//long startTime  = hdrOrginal.getMessageId().getStartTime();
				//latencies.add((int)(System.currentTimeMillis() - startTime));
//				rateCount++;
//			if (rateCount == rateInterval){
//				new StatsThread().start();;
//				rateCount=0;
//			}
				//if (numReqDeviverd.get()>=1000000){
				//	timer.cancel();
				//}
			}
	//	}
		
		if (log.isInfoEnabled())
			log.info("queuedCommitMessage size = " + queuedCommitMessage.size() + " zxid "+dZxid);
		if (requestQueue.contains(hdrOrginal.getMessageId())) {
			//if(!is_warmUp){
			long startTime  = hdrOrginal.getMessageId().getStartTime();
			latencies.add((int)(System.nanoTime() - startTime));
			log.info("chckkkkkkkkkkkkk "+("deliver messgae is:= "+hdrOrginal.getMessageId()));

			//}
			//log.info("I am the zab request receiver, "+ hdr.getZxid());
					//+ hdrOrginal.getMessageId().getAddress());
			ZabHeader hdrResponse = new ZabHeader(ZabHeader.RESPONSE, dZxid,
					hdrOrginal.getMessageId());
			Message msgResponse = new Message(hdrOrginal.getMessageId()
					.getOriginator()).putHeader(this.id, hdrResponse);
			down_prot.down(new Event(Event.MSG, msgResponse));

		}

	}
		
		
		
    private void handleOrderingResponse(ZabHeader hdrResponse) {
	        Message message = messageStore.get(hdrResponse.getMessageId());
	        log.info("Check return RPC before ########################### "+ message);
	        message.setDest(local_addr);
	        message.src(null);
	        log.info("Check return after ########################### "+ message);
	        //message.putHeader(this.id, hdrResponse);
	        down_prot.down(new Event(Event.MSG, message));

	    }
	    	
		private boolean isQuorum(int majority){
	    	return majority >= ((zabMembers.size()/2) + 1)? true : false;
	    }
		
		private String getCurrentTimeStamp(){
			long timestamp = new Date().getTime();  
			cal.setTimeInMillis(timestamp);
			String timeString =
				   new SimpleDateFormat("HH:mm:ss:SSS").format(cal.getTime());

			return timeString;
		}
		
		private void sendTotalABMwssages(ZabHeader CarryCountMessageLeader){
			if(!is_leader){
			    ZabHeader followerMsgCount = new ZabHeader(ZabHeader.COUNTMESSAGE, countMessageFollower);
		   	    Message requestMessage = new Message(leader).putHeader(this.id, followerMsgCount);
		        down_prot.down(new Event(Event.MSG, requestMessage)); 
			}
			else{
				long followerMsg = CarryCountMessageLeader.getZxid();
				countTotalMessagesFollowers += followerMsg;
			}
				
		}
		
		
		private void printMZabStats(){	
			// print Min, Avg, and Max latency
			List<Long> latAvg = new ArrayList<Long>();
			int count =0;
			long avgTemp =0;
			long min = Long.MAX_VALUE, avg =0, max = Long.MIN_VALUE;
			for (long lat : latencies){
				if (lat < min){
					min = lat;
				}
				if (lat > max){
					max = lat;
				}
				avg+=lat;
				avgTemp+=lat;
				count++;
				if(count>10000){
					latAvg.add(avgTemp/count);
					count=0;
					avgTemp=0;
				}
				
			}

			outFile.println("Number of Request Recieved = "+(numRequest));
			outFile.println("Number of Request Deliever = " + numReqDeviverd);
			outFile.println("Total ZAB Messages = " + (countMessageLeader.get() + countTotalMessagesFollowers));
			outFile.println("Throughput = " + (numReqDeviverd.get()/(TimeUnit.MILLISECONDS.toSeconds(endThroughputTime-startThroughputTime))));
			outFile.println("Large Latencies count " + largeLatCount);	
			outFile.println("Large Latencies " + largeLatencies);	
			outFile.println("Latency /Min= " + min + " /Avg= "+ (avg/latencies.size())+
			        " /Max= " +max);	
			outFile.println("Latency size= " + latencies.size());
			outFile.println("Latency average rate with interval 100000 = " + 
			        avgLatencies);
			outFile.println("Latency average rate with interval 200 MillSec = " + 
			        avgLatenciesTimer);
			outFile.println("Latency average rate with No intervel = " + 
					latAvg + " numbers avg = " + latAvg.size());
			
				
			 
			Collections.sort(latencies);
			
			int x50=0, x100=0, x150=0, x200=0, x250=0, x300=0, x350=0,x400=0, x450=0, x500=0,
					x550=0, x600=0, x650=0, x700=0, xLager700=0, x10=0, x20=0,x30=0,x40=0,x55=0;
			
			for (Integer l:latencies){
				if (l<11)
					x10++;
				else if(l>10 && l<21)
					x20++;
				else if(l>20 && l<31)
					x30++;
				else if(l>30 && l<41)
					x40++;
				else if(l>40 && l<51)
					x50++;
				else if(l>50 && l<101)
					x100++;
				else if(l>100 && l<151)
					x150++;
				else if(l>150 && l<201)
					x200++;
				else if(l>200 && l<251)
					x250++;
				else if(l>250 && l<301)
					x300++;
				else if(l>300 && l<351)
					x350++;
				else if(l>350 && l<401)
					x400++;
				else if(l>400 && l<451)
					x450++;
				else if(l>450 && l<501)
					x500++;
				else if(l>500 && l<551)
					x550++;
				else if(l>560 && l<601)
					x600++;
				else if(l>600 && l<651)
					x650++;
				else if(l>650 && l<701)
					x700++;
				else
					xLager700++;
			}
				//outFile.println("Distribution contains latencies form (0-50) " + x50);
				outFile.println("Distribution contains latencies form (0-10) " + x10);
				outFile.println("Distribution contains latencies form (11-20) " + x20);
				outFile.println("Distribution contains latencies form (21-30) " + x30);
				outFile.println("Distribution contains latencies form (31-40) " + x40);
				outFile.println("Distribution contains latencies form (41-50) " + x55);
			    outFile.println("Distribution contains latencies form (51-100) " + x100);
			    outFile.println("Distribution contains latencies form (101-150) " + x150);
			    outFile.println("Distribution contains latencies form (151-200) " + x200);
			    outFile.println("Distribution contains latencies form (201-250) " + x250);
			    outFile.println("Distribution contains latencies form (251-300) " + x300);
			    outFile.println("Distribution contains latencies form (301-350) " + x350);
			    outFile.println("Distribution contains latencies form (351-400) " + x400);
			    outFile.println("Distribution contains latencies form (401-450) " + x450);
			    outFile.println("Distribution contains latencies form (451-500) " + x500);
			    outFile.println("Distribution contains latencies form (501-550) " + x550);
			    outFile.println("Distribution contains latencies form (551-600) " + x600);
			    outFile.println("Distribution contains latencies form (601-650) " + x650);
			    outFile.println("Distribution contains latencies form (651-700) " + x700);
			    outFile.println("Distribution contains latencies form (  > 700) " + xLager700);
		
			
//			final int groupRange = 50;
//
//			ListMultimap<Integer, Integer> map = Multimaps.index(latencies, new Function<Integer, Integer>() {
//			    public Integer apply(Integer i) {
//			        //work out which group the value belongs in
//			        return (i / groupRange) + (i % groupRange == 0 ? 0 : 1);
//			    }
//			});
//			for (Integer key : map.keySet()) {
//			    List<Integer> value = map.get(key);
//			    outFile.println("Distribution contains " + value.size() + " items from " + value.get(0) + " to " + value.get(value.size() - 1));
//			}
			
			    outFile.println("Test Generated at "+ new Date() + " /Lasted for = "
					 	+ TimeUnit.MILLISECONDS.toSeconds((endThroughputTime-startThroughputTime)));	
			 outFile.println();	

			outFile.close();	
		
		}
		
/* ----------------------------- End of Private Methods -------------------------------- */
   

 final class FollowerMessageHandler implements Runnable {
    	
    	private short id;
    	public FollowerMessageHandler(short id){
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
        	ZabHeader hdrReq = null;
            while (running) {
            	
                	 try {
                		 hdrReq = queuedMessages.take();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
               
            	long new_zxid = getNewZxid();
          	    numRequest.incrementAndGet();

            	ZabHeader hdrProposal = new ZabHeader(ZabHeader.PROPOSAL, new_zxid, hdrReq.getMessageId());                
                Message ProposalMessage=new Message().putHeader(this.id, hdrProposal);

                ProposalMessage.setSrc(local_addr);
            	Proposal p = new Proposal();
            	p.setMessageId(hdrReq.getMessageId());
            	p.AckCount++;            	            	
            	outstandingProposals.put(new_zxid, p);
            	//hdrProposal.getMessageId().setStartTime(System.currentTimeMillis());
            	queuedProposalMessage.put(new_zxid, hdrProposal);
            	
            	          	
            	try{
       
                 	for (Address address : zabMembers) {
                        if(address.equals(leader))
                        	continue;
                        //if(startThroughput)
                		countMessageLeader.incrementAndGet();
                        Message cpy = ProposalMessage.copy();
                        cpy.setDest(address);
                		down_prot.down(new Event(Event.MSG, cpy));     
                    }
                 }catch(Exception ex) {
            		log.error("failed proposing message to members");
            	}    
            	
            }
            
        }

       
    }


	 public class StatsThread extends Thread{
		 public void run(){
			 int avg = 0, elementCount = 0;
			 for (int i =  lastArrayIndex; i < latencies.size(); i++){
				 avg+=latencies.get(i);
				 elementCount++;
			 }
			 
			 lastArrayIndex = latencies.size() - 1;
			 avg= avg/elementCount;
			 avgLatencies.add(avg);
		 }
	 }
	 
	 class ResubmitTimer extends TimerTask{

		@Override
		public void run() {
			int finished = numReqDeviverd.get();
			currentCpuTime = System.currentTimeMillis();
			
			//Find average latency
			 int avg = 0, elementCount = 0;
			 
			 //List<Integer> latCopy = new ArrayList<Integer>(latencies);
			 for (int i =  lastArrayIndexUsingTime; i < latencies.size(); i++){
				 if (latencies.get(i)>50){
					 largeLatCount++;
					 largeLatencies.add((currentCpuTime - startThroughputTime) + "/" + latencies.get(i));
				 }
				 avg+=latencies.get(i);
				 elementCount++;
			 }
			 
			 lastArrayIndexUsingTime = latencies.size()-1;
			 avg= avg/elementCount;
		 
			String mgsLat = (currentCpuTime - startThroughputTime) + "/" +
			                ((finished - lastFinished) + "/" + avg);// (TimeUnit.MILLISECONDS.toSeconds(currentCpuTime - lastCpuTime)));
			avgLatenciesTimer.add(mgsLat);
			lastFinished = finished;
			lastCpuTime = currentCpuTime;
		}
		 
	 }
}