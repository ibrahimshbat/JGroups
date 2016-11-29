package org.jgroups.protocols.jzookeeper.clustersize;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.jgroups.Address;
import org.jgroups.Message;

public class Proposal implements Comparable<Proposal>{
	
	public int AckCount;

	private long zxid = -1;
	
	private Message message=null;
	
	private MessageId messageId=null;
    
	private Address messageSrc;
	
    public Proposal(){
    }
    
	public Proposal(int count, HashSet<Long> ackSet) {
		this.AckCount = count;
		//.message = message;
	}

	public int getAckCount() {
		return AckCount;
	}

	public void setAckCount(int count) {
		this.AckCount = count;
	}

	public MessageId getMessageId() {
		return messageId;
	}

	public void setMessageId(MessageId messageId) {
		this.messageId = messageId;
	}
	
	public Message getMessage() {
		return message;
	}

	public void setMessage(Message message) {
		this.message = message;
	}

	public Address getMessageSrc() {
		return messageSrc;
	}
	public void setMessageSrc(Address messageSrc) {
		this.messageSrc = messageSrc;
	}
	
	public long getZxid() {
		return zxid;
	}
	public void setZxid(long zxid) {
		this.zxid = zxid;
	}
	@Override
	public int compareTo(Proposal o) {
		if (this.zxid > o.zxid)
			return 1;
		else if (this.zxid < o.zxid)
			return -1;
		else
			return 0;
	}
	@Override
	public String toString() {
		return "Proposal [zxid=" + zxid + "]";
	}
 

}
