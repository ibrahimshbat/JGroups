package org.jgroups.protocols.jzookeeper.zabCT_AdaptationUsingWriteRatioV1;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jgroups.Address;
import org.jgroups.Message;

public class Proposal implements Comparable<Proposal>{
	
	Set<Address> addressesACK ;
	
	private MessageOrderInfo messageOrderInfo=null;
	
	private boolean proposalRecieved=false;
	
	public Proposal() {
		this.addressesACK =  new HashSet<Address>();
	}
	

	public Set<Address> getAddressesACK() {
		return addressesACK;
	}


	public void setAddressesACK(Set<Address> addressesACK) {
		this.addressesACK = addressesACK;
	}
	
	public void addAddressesACK(Address addAck) {
		this.addressesACK.add(addAck);
	}

	public MessageOrderInfo getMessageOrderInfo() {
		return messageOrderInfo;
	}
	public void setMessageOrderInfo(MessageOrderInfo messageOrderInfo) {
		this.messageOrderInfo = messageOrderInfo;
	}
	
	public boolean isProposalRecieved() {
		return proposalRecieved;
	}

	public void setProposalRecieved(boolean proposalRecieved) {
		this.proposalRecieved = proposalRecieved;
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
