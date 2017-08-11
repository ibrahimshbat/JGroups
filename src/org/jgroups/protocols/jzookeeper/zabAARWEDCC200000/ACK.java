package org.jgroups.protocols.jzookeeper.zabAARWEDCC200000;

import org.jgroups.Address;

public class ACK implements Comparable<ACK>{

	private long zxid;
	private Address address;

	public ACK() {
		this.zxid =0;
	}
	
	public ACK(Address address, long zxid) {
		this.address = address;
		this.zxid = zxid;
	}


	public Address getAddress() {
		return address;
	}


	public void setAddress(Address address) {
		this.address = address;
	}


	public long getZxid() {
		return zxid;
	}


	public void setZxid(long zxid) {
		this.zxid = zxid;
	}


	@Override
	public int compareTo(ACK second) {
		if (this.zxid>second.zxid)
			return 1;
		else if(this.zxid<second.zxid)
			return -1;
		else
			return 0;
	}

	@Override
	public String toString() {
		return "ACK [address=" + address + ", zxid=" + zxid + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((address == null) ? 0 : address.hashCode());
		result = prime * result + (int) (zxid ^ (zxid >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof  ACK))
			return false;
		ACK other = (ACK) obj;
		if (address == null) {
			if (other.address != null)
				return false;
		} else if (!address.equals(other.address))
			return false;
		return true;
	}




}
