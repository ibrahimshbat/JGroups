package org.jgroups.protocols.jzookeeper.clustersizeForPaper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.jgroups.Address;
import org.jgroups.Message;

public class Proposal implements Comparable<Proposal>{
	
	public int AckCount;
		
	private MessageOrderInfo messageOrderInfo=null;
	
	public Proposal() {

	}
	
	public Proposal(int count) {
		this.AckCount = count;
	}

	public int getAckCount() {
		return AckCount;
	}

	public void setAckCount(int count) {
		this.AckCount = count;
	}

	public MessageOrderInfo getMessageOrderInfo() {
		return messageOrderInfo;
	}
	public void setMessageOrderInfo(MessageOrderInfo messageOrderInfo) {
		this.messageOrderInfo = messageOrderInfo;
	}
	
	@Override
	public int compareTo(Proposal o) {
		if (this.messageOrderInfo.getOrdering() > o.getMessageOrderInfo().getOrdering())
			return 1;
		else if (this.messageOrderInfo.getOrdering() < o.getMessageOrderInfo().getOrdering())
			return -1;
		else
			return 0;
	}
	@Override
	public String toString() {
		return "Proposal info [messageOrderInfo=" + messageOrderInfo + "]";
	}
 

}
