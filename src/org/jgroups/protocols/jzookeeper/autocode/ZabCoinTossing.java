package org.jgroups.protocols.jzookeeper.autocode;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.Message.Flag;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.protocols.jzookeeper.MessageId;
import org.jgroups.protocols.jzookeeper.Proposal;
import org.jgroups.protocols.jzookeeper.ProtocolStats;
import org.jgroups.protocols.jzookeeper.ZUtil;
import org.jgroups.protocols.jzookeeper.ZabCoinTossingHeader;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

	
	/* 
	 * Zab_3 (main approach ) is the same implementation as Zab_2.
	 * Note that all the code and implementation are simaller to Zab_2, just we change probability 
	 * parameter in ZUtil class from 1.0 10 0.5.
	 * Also it has features of testing throughput, latency (in Nano), ant etc. 
	 * When using testing, it provides warm up test before starting real test.
	 */
	public class ZabCoinTossing extends Protocol {
		private final static String ProtocolName = "ZabCoinTossing";
		private final static int numberOfSenderInEachClient = 20;
		protected final AtomicLong        zxid=new AtomicLong(0);
		private ExecutorService executor1;
		private ExecutorService executor2;
		protected Address                           local_addr;
	    protected volatile Address                  leader;
	    protected volatile View                     view;
	    protected volatile boolean                  is_leader=false;
	    private List<Address> zabMembers = Collections.synchronizedList(new ArrayList<Address>());
		private long lastZxidCommitted=0;
	    private final Set<MessageId> requestQueue =Collections.synchronizedSet(new HashSet<MessageId>());
		private Map<Long, ZabCoinTossingHeader> queuedCommitMessage = new HashMap<Long, ZabCoinTossingHeader>();
	    private final LinkedBlockingQueue<ZabCoinTossingHeader> queuedMessages =
		        new LinkedBlockingQueue<ZabCoinTossingHeader>();
		private final LinkedBlockingQueue<ZabCoinTossingHeader> delivery = new LinkedBlockingQueue<ZabCoinTossingHeader>();

		private ConcurrentMap<Long, Proposal> outstandingProposals = new ConcurrentHashMap<Long, Proposal>();
	    private final Map<Long, ZabCoinTossingHeader> queuedProposalMessage = Collections.synchronizedMap(new HashMap<Long, ZabCoinTossingHeader>());
	    private final Map<MessageId, Message> messageStore = Collections.synchronizedMap(new HashMap<MessageId, Message>());
	    private final Map<Long, Integer> followerACKs = Collections.synchronizedMap(new HashMap<Long, Integer>());

	    protected volatile boolean                  running=true;
	    private int index=-1;
	    private boolean startThroughput = false;
		private final static String outDir = "/work/ZabCoinTossing/";
		private Timer timerThro = new Timer();
		private AtomicLong countMessageLeader = new AtomicLong(0);
		private long countMessageFollower = 0;

	    private List<String> largeLatencies = new ArrayList<String>();
		private volatile boolean makeAllFollowersAck=false;
		private List<Address>  clients = Collections.synchronizedList(new ArrayList<Address>());
		private ProtocolStats stats = new ProtocolStats();
		private int clientFinished = 0;
		private int numABRecieved = 0;
		@Property(name = "ZabCoinTossing_size", description = "It is ZabCoinTossing cluster size")
		private int clusterSize = 5;
		
		
		public ZabCoinTossing(){
	    	
	    }
	    
	    @ManagedAttribute
	    public boolean isleaderinator() {return is_leader;}
	    public Address getleaderinator() {return leader;}
	    public Address getLocalAddress() {return local_addr;}
	    
	    @Override
	    public void start() throws Exception {
	        super.start();
		    log.setLevel("trace");
		    running=true;        
			executor1 = Executors.newSingleThreadExecutor();
			executor1.execute(new FollowerMessageHandler(this.id));
	    }
	    
	    
	    public void reset() {
	    	zxid.set(0);     	
	    	lastZxidCommitted=0;         requestQueue.clear();
	    	queuedCommitMessage.clear();queuedProposalMessage.clear();        
	        queuedMessages.clear(); outstandingProposals.clear();       
	        messageStore.clear();     
	        largeLatencies.clear();   	
	 	
	        countMessageLeader = new AtomicLong(0);        
	        countMessageFollower = 0;        
	    	timerThro.schedule(new Throughput(), 1000, 1000);
			this.stats = new ProtocolStats(ProtocolName, clients.size(),
					numberOfSenderInEachClient, outDir, false);
			log.info("Reset done");
		    
	    }  
		   
	    @Override
	    public void stop() {
	        running=false;
			executor1.shutdown();
			executor2.shutdown();
			super.stop();
	    }

	    public Object down(Event evt) {
	        switch(evt.getType()) {
	            case Event.MSG:
	                Message msg=(Message)evt.getArg();
	                ZabCoinTossingHeader hdr=(ZabCoinTossingHeader) msg.getHeader(this.id);
	                if (hdr instanceof ZabCoinTossingHeader){
	    				handleClientRequest(msg);
	    				return null;
	    			}
	    			else{
	    				break;
	    			}
	            case Event.SET_LOCAL_ADDRESS:
	                local_addr=(Address)evt.getArg();
	                break;
	        }
	        return down_prot.down(evt);
	    }

	    public Object up(Event evt) {
	        Message msg = null;
	        ZabCoinTossingHeader hdr;

	        switch(evt.getType()) {
	            case Event.MSG:            	
	                msg=(Message)evt.getArg();
	                hdr=(ZabCoinTossingHeader)msg.getHeader(this.id);
	                if(hdr == null){
	                    break; // pass up
	                }
	                switch(hdr.getType()) {                
	                    case ZabCoinTossingHeader.RESET:
	  	                    reset();
		                	break;
	                	case ZabCoinTossingHeader.REQUEST:                			
	                		forwardToLeader(msg);
	                		break;
	                    case ZabCoinTossingHeader.FORWARD:
	                		queuedMessages.add(hdr);
	                		break;
	                    case ZabCoinTossingHeader.PROPOSAL:
	                         if (!is_leader) {
	        					if (!stats.isWarmup() && !startThroughput) {
	        						startThroughput = true;
	        						stats.setStartThroughputTime(System.currentTimeMillis());
	        						stats.setLastNumReqDeliveredBefore(0);
	        						stats.setLastThroughputTime(System.currentTimeMillis());
	        					}
	        					sendACK(msg, hdr);
	        				}
	        				break;          		
	                    case ZabCoinTossingHeader.ACK:
	                			processACK(msg);
	                		break;
	                    case ZabCoinTossingHeader.CLIENTFINISHED:
	        				clientFinished++;
	        				if(clientFinished==clients.size() && !is_leader)
	        					sendMyTotalBroadcast();
	        				break;
	                    case ZabCoinTossingHeader.COUNTMESSAGE:
	        				addTotalABMssages(hdr);
	        				log.info("Yes, I recieved count request");
	        				break;
	        			case ZabCoinTossingHeader.STATS:
	        				stats.printProtocolStats(is_leader);
	        				break;
	                    case ZabCoinTossingHeader.SENDMYADDRESS:
	                    	if (!zabMembers.contains(msg.getSrc())) {
	        					clients.add(msg.getSrc());
	        					System.out.println("Rceived clients address "
	        							+ msg.getSrc());
	        				}
	        				break;
	                    case ZabCoinTossingHeader.RESPONSE:
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

	    
	    private void handleClientRequest(Message message){
	    	ZabCoinTossingHeader clientHeader = ((ZabCoinTossingHeader) message.getHeader(this.id));

	        if (clientHeader != null
					&& clientHeader.getType() == ZabCoinTossingHeader.RESET) {

				for (Address server : zabMembers) {
					Message resetMessage = new Message(server).putHeader(this.id,
							clientHeader);
					resetMessage.setSrc(local_addr);
					//resetMessage.setFlag(Message.Flag.DONT_BUNDLE);
					down_prot.down(new Event(Event.MSG, resetMessage));
				}
			}
	    	
	    	
	    	else if (clientHeader != null
					&& clientHeader.getType() == ZabCoinTossingHeader.STATS) {

				for (Address server : zabMembers) {
					Message statsMessage = new Message(server).putHeader(this.id,
							clientHeader);
					//statsMessage.setFlag(Message.Flag.DONT_BUNDLE);
					down_prot.down(new Event(Event.MSG, statsMessage));
				}
			}

			else if (clientHeader != null
					&& clientHeader.getType() == ZabCoinTossingHeader.CLIENTFINISHED) {
				for (Address server : zabMembers) {
						Message countMessages = new Message(server).putHeader(
								this.id, clientHeader);
						//countMessages.setFlag(Message.Flag.DONT_BUNDLE);
						down_prot.down(new Event(Event.MSG, countMessages));
				}

			}
	    	
	    	else if(!clientHeader.getMessageId().equals(null) && clientHeader.getType() == ZabCoinTossingHeader.REQUEST){
		    	 Address destination = null;
		         messageStore.put(clientHeader.getMessageId(), message);
		        ZabCoinTossingHeader hdrReq=new ZabCoinTossingHeader(ZabCoinTossingHeader.REQUEST, clientHeader.getMessageId());  
		        ++index;
				if (index > (clusterSize-1))
					index = 0;
		        destination = zabMembers.get(index);
		        Message requestMessage = new Message(destination).putHeader(this.id, hdrReq);
		        requestMessage.setBuffer(new byte[1000]);
		        //requestMessage.setFlag(Message.Flag.DONT_BUNDLE);
		       down_prot.down(new Event(Event.MSG, requestMessage));    
	    	}
	    	
	    	else if (!clientHeader.getMessageId().equals(null)
					&& clientHeader.getType() == ZabCoinTossingHeader.SENDMYADDRESS) {
				log.info("ZabMemberSize = " + zabMembers.size());
				for (Address server : zabMembers) {
					log.info("server address = " + server);
					message.dest(server);
					message.src(message.getSrc());
					//message.setFlag(Message.Flag.DONT_BUNDLE);
					down_prot.down(new Event(Event.MSG, message));
				}
			}
	    
	    	
	    }
	    
	    private void handleViewChange(View v) {
	    	this.view = v;
	        List<Address> mbrs=v.getMembers();
	        leader=mbrs.get(0);
	        if (leader.equals(local_addr)){
	        	is_leader = true;
	        }
	        if (mbrs.size() == clusterSize) {
				zabMembers.addAll(v.getMembers());
				if (zabMembers.contains(local_addr)) {
					executor2 = Executors.newSingleThreadExecutor();
			        executor2.execute(new MessageHandler());
				}

			}
			if (mbrs.size() > clusterSize && zabMembers.isEmpty()) {
				for (int i = 0; i < clusterSize; i++) {
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
		   ZabCoinTossingHeader hdrReq = (ZabCoinTossingHeader) msg.getHeader(this.id);
		   requestQueue.add(hdrReq.getMessageId());
		   if (!stats.isWarmup() && is_leader && !startThroughput){
				startThroughput = true;
				stats.setStartThroughputTime(System.currentTimeMillis());
			}
//		   if (!stats.isWarmup()){
//	   			if (stats.getLastRecievedTime() ==0 ){
//	   				stats.setLastRecievedTime(System.nanoTime());
//	   			}
//	   			else{
//	   				long currentTime = System.nanoTime();
//	   				stats.addRecievedTime((currentTime - stats.getLastRecievedTime()));
//	   				stats.setLastRecievedTime(currentTime);
//	   			}
//   			}
		   if (is_leader){
			   hdrReq.getMessageId().setStartTime(System.nanoTime());
			   queuedMessages.add((ZabCoinTossingHeader)msg.getHeader(this.id));
	       }	   
		   else{
			   hdrReq.getMessageId().setStartTime(System.nanoTime());
			   forward(msg);
		   }
	            
	           
	   }

	    private synchronized void forward(Message msg) {
	        Address target=leader;
	 	    ZabCoinTossingHeader hdrReq = (ZabCoinTossingHeader) msg.getHeader(this.id);
	        if(target == null)
	            return;
		    try {
		        ZabCoinTossingHeader hdr=new ZabCoinTossingHeader(ZabCoinTossingHeader.FORWARD, hdrReq.getMessageId());
		        Message forward_msg=new Message(target).putHeader(this.id,hdr);
		        forward_msg.setBuffer(new byte[1000]);
		       // forward_msg.setFlag(Message.Flag.DONT_BUNDLE);
		        down_prot.down(new Event(Event.MSG, forward_msg));
		     }
		    catch(Exception ex) {
		      log.error("failed forwarding message to " + msg, ex);
		    }
	      
	    }
	    

	    private void sendACK(Message msg, ZabCoinTossingHeader hrdAck){
	    	if(!stats.isWarmup()){
				stats.incNumRequest();
			}
	    	Proposal p;
	    	if (msg == null )
	    		return;	    	
	    	if (hrdAck == null)
	    		return;
	    	
				p = new Proposal();
				p.AckCount++; // Ack from leader
				p.setZxid(hrdAck.getZxid());
				outstandingProposals.put(hrdAck.getZxid(), p);
				queuedProposalMessage.put(hrdAck.getZxid(), hrdAck);
			if (stats.isWarmup()){
				ZabCoinTossingHeader hdrACK = new ZabCoinTossingHeader(ZabCoinTossingHeader.ACK, hrdAck.getZxid());
				Message ackMessage = new Message().putHeader(this.id, hdrACK);
				//ackMessage.setFlag(Message.Flag.DONT_BUNDLE);
				try{
				for (Address address : zabMembers) {
	                Message cpy = ackMessage.copy();
	                cpy.setDest(address);
	        		down_prot.down(new Event(Event.MSG, cpy));     
	            }
	         }catch(Exception ex) {
	    		log.error("failed proposing message to members");
	    	}    
			}
			
			else if (ZUtil.SendAckOrNoSend() || makeAllFollowersAck) {

				ZabCoinTossingHeader hdrACK = new ZabCoinTossingHeader(ZabCoinTossingHeader.ACK, hrdAck.getZxid());
				Message ackMessage = new Message().putHeader(this.id, hdrACK);
				//ackMessage.setFlag(Message.Flag.DONT_BUNDLE);
				try{
				for (Address address : zabMembers) {
					if (address.equals(local_addr)) {
						processACK(ackMessage);
						continue;
					}
					else{
						countMessageFollower++;
						stats.incCountMessageFollower();
		                Message cpy = ackMessage.copy();
		                cpy.setDest(address);
		        		down_prot.down(new Event(Event.MSG, cpy));     
					}
	            }
	         }catch(Exception ex) {
	    		log.error("failed proposing message to members");
	    	}    
			}
			//else{
				//log.info("Not Sending ACK for " + hdr.getZxid()+" "+getCurrentTimeStamp()+ " " +getCurrentTimeStamp());
				//notACK.put(hdr.getZxid(), false);
			//}
		
	    	}
	    
	    
	    private synchronized void processACK(Message msgACK){
		    Proposal p = null;
	    	ZabCoinTossingHeader hdr = (ZabCoinTossingHeader) msgACK.getHeader(this.id);	
	    	long ackZxid = hdr.getZxid();
			if (lastZxidCommitted >= ackZxid) {
	            return;
	        }
			if (ackZxid > lastZxidCommitted && !outstandingProposals.containsKey(ackZxid)) {
				if (!followerACKs.containsKey(ackZxid))
					followerACKs.put(ackZxid,1);
				else{
					int count = followerACKs.get(ackZxid);
					followerACKs.put(ackZxid, count++);
	            //log.info("Received an ACK no ts proposal "+hdr.getZxid());
	            //log.info("Last lastZxidCommitted is "+ lastZxidCommitted);
	            //log.info("ACK from "+ msgACK.getSrc());
				//return;
				}
	        }
	        p = outstandingProposals.get(ackZxid);
	        if (p == null) {           
	            return;
	        }
			p.AckCount++;
			if (followerACKs.containsKey(ackZxid)){
				p.AckCount = p.AckCount + followerACKs.get(ackZxid);
				followerACKs.remove(ackZxid);
			}
			if (isQuorum(p.getAckCount())) {
				if (ackZxid == lastZxidCommitted+1){
					delivery.add(hdr);
					outstandingProposals.remove(ackZxid);	
					lastZxidCommitted = ackZxid;
				} else {
					long zxidCommiting = lastZxidCommitted +1;
					for (long z = zxidCommiting; z < ackZxid+1; z++){
						delivery.add(new ZabCoinTossingHeader(ZabCoinTossingHeader.DELIVER, z));
						outstandingProposals.remove(z);
						lastZxidCommitted = z;
					}
				}
		
			}
			

		}
	    
	    private void commit(long zxidd){
				

			    ZabCoinTossingHeader hdrOrginal = null;
			   hdrOrginal = queuedProposalMessage.get(zxidd);
			   if (hdrOrginal == null){
				  // log.info("#####commit hdrOrginal == null#####");
				   return;
			   }
	   	   
			   deliver(zxidd);

		    }
	    
			
	    private void deliver(long committedZxid){
		    	
		    	ZabCoinTossingHeader hdrOrginal = queuedProposalMessage.remove(committedZxid);

		    	queuedCommitMessage.put(committedZxid, hdrOrginal);
		    	if (!stats.isWarmup()) {
					stats.incnumReqDelivered();
					stats.setEndThroughputTime(System.currentTimeMillis());
				
					//999000
					if (stats.getnumReqDelivered() > 999000){
						makeAllFollowersAck=true;
					}
		    	}
				
		    	if(hdrOrginal==null)
		    		log.info("****hdrOrginal is null ****");
		      //	log.info("queuedCommitMessage size = " + queuedCommitMessage.size() + " zxid "+committedZxid);
		    	//log.info(queuedCommitMessage.size() + " zxid "+committedZxid);
			  	if (requestQueue.contains(hdrOrginal.getMessageId())){
		    		if (!stats.isWarmup()) {
		    			long startTime = hdrOrginal.getMessageId().getStartTime();
						long endTime = System.nanoTime();
						stats.addLatency((long) (endTime - startTime));
					}
			    	ZabCoinTossingHeader hdrResponse = new ZabCoinTossingHeader(ZabCoinTossingHeader.RESPONSE, committedZxid,  hdrOrginal.getMessageId());
			    	Message msgResponse = new Message(hdrOrginal.getMessageId().getOriginator()).putHeader(this.id, hdrResponse);
			    	//msgResponse.setFlag(Message.Flag.DONT_BUNDLE);
			    	down_prot.down(new Event(Event.MSG, msgResponse)); 
		       		requestQueue.remove(hdrOrginal.getMessageId());

		    	}
		    	
		    	   
		   }
			
			
	    private void handleOrderingResponse(ZabCoinTossingHeader hdrResponse) {
				
		        Message message = messageStore.get(hdrResponse.getMessageId());
		        message.putHeader(this.id, hdrResponse);
		        up_prot.up(new Event(Event.MSG, message));
				messageStore.remove(hdrResponse.getMessageId());


		 }
		    	

			private boolean isQuorum(int majority){
		    	return majority >= ((clusterSize/2) + 1)? true : false;
		    }
			
			private synchronized void addTotalABMssages(ZabCoinTossingHeader carryCountMessageLeader) {
				long followerMsg = carryCountMessageLeader.getZxid();
				stats.addCountTotalMessagesFollowers((int) followerMsg);
				numABRecieved++;
				if(numABRecieved==zabMembers.size()-1){
					ZabCoinTossingHeader headertStats = new ZabCoinTossingHeader(ZabCoinTossingHeader.STATS);
					for (Address zabServer:zabMembers){
						Message messageStats = new Message(zabServer).putHeader(this.id,
							headertStats);
						//messageStats.setFlag(Message.Flag.DONT_BUNDLE);
						down_prot.down(new Event(Event.MSG, messageStats));
					}
				}
			}

			private void sendMyTotalBroadcast(){
				ZabCoinTossingHeader followerMsgCount = new ZabCoinTossingHeader(
						ZabCoinTossingHeader.COUNTMESSAGE, countMessageFollower);
				Message requestMessage = new Message(leader).putHeader(this.id,
						followerMsgCount);
				//.setFlag(Message.Flag.DONT_BUNDLE);
				down_prot.down(new Event(Event.MSG, requestMessage));
			}
	    

	/* ----------------------------- End of Private Methods -------------------------------- */
	   

	 final class FollowerMessageHandler implements Runnable {
	    	
	    	private short id;
	    	public FollowerMessageHandler(short id){
	    		this.id = id;
	    	}
	    	
	    	@Override
	        public void run() {
	                handleRequests();         
	        }
	    	
	    	/**
	         * create a proposal and send it out to all the members
	         * 
	         * @param message
	         */
	    	
	        private void handleRequests() {
	        	ZabCoinTossingHeader hdrReq = null;
	        	//long currentTime = 0;
	            while (running) {
	            	            		
	                	 try {
	                		
	                		 hdrReq=queuedMessages.take();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
	                 
	            	
	                
	            	long new_zxid = getNewZxid();
	            	if (!stats.isWarmup()) {
						stats.incNumRequest();
					}

	            	ZabCoinTossingHeader hdrProposal = new ZabCoinTossingHeader(ZabCoinTossingHeader.PROPOSAL, new_zxid, hdrReq.getMessageId()); 
	                Message ProposalMessage=new Message().putHeader(this.id, hdrProposal);

	                ProposalMessage.setSrc(local_addr);
	                ProposalMessage.setBuffer(new byte[1000]);
	               // ProposalMessage.setFlag(Message.Flag.DONT_BUNDLE);
	            	Proposal p = new Proposal();
	            	p.setMessageId(hdrReq.getMessageId());
	            	p.setZxid(new_zxid);
	            	p.AckCount++;
	            	
	            	outstandingProposals.put(new_zxid, p);
	            	queuedProposalMessage.put(new_zxid, hdrProposal);
	            	
	            	try{
	            		

	                 	for (Address address : zabMembers) {
	                        if(address.equals(leader))
	                        	continue; 
	                        if (!stats.isWarmup()) {
								countMessageLeader.incrementAndGet();
								stats.incCountMessageLeader();
							}
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


	 final class MessageHandler implements Runnable {
	        @Override
	        public void run() {
	        	deliverMessages();

	        }

	        private void deliverMessages() {
				ZabCoinTossingHeader hdrDelivery= null;
	            while (true) {
	    				try {
	    					hdrDelivery = delivery.take();
	    				} catch (InterruptedException e) {
	    					e.printStackTrace();
	    				}          
	                    commit(hdrDelivery.getZxid());
	                        
	            }
	        }
	  
	    } 

	    class Throughput extends TimerTask {

			public Throughput() {

			}

			private long startTime = 0;
			private long currentTime = 0;
			private double currentThroughput = 0;
			private int finishedThroughput = 0;

			@Override
			public void run() {
					startTime = stats.getLastThroughputTime();
					currentTime = System.currentTimeMillis();
					finishedThroughput=stats.getnumReqDelivered();			
					currentThroughput = (((double)finishedThroughput - stats
							.getLastNumReqDeliveredBefore()) / ((double)(currentTime - startTime)/1000.0));
					stats.setLastNumReqDeliveredBefore(finishedThroughput);
					stats.setLastThroughputTime(currentTime);
					stats.addThroughput(convertLongToTimeFormat(currentTime),
						currentThroughput);
			}

			public String convertLongToTimeFormat(long time) {
				Date date = new Date(time);
				SimpleDateFormat longToTime = new SimpleDateFormat("HH:mm:ss.SSSZ");
				return longToTime.format(date);
			}
		}

	}
