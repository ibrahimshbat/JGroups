package org.jgroups.protocols.jzookeeper.zabNRW;

import org.jgroups.Address;
import org.jgroups.util.Bits;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Util;

import java.io.*;

/**
 * The represents an unique identifier for the messages processed by the Total
 * Order Anycast protocol
 * <p/>
 * Note: it is similar to the ViewId (address + counter)
 *
 * @author Pedro Ruivo
 * @since 3.1
 */
public class MessageId implements Comparable<MessageId>, SizeStreamable {
	private Address originator = null;
	private long id = -1;
	private long startSend = 0;

	public MessageId() {
	}

	public MessageId(Address originator, long id) {
		this.originator = originator;
		this.id = id;
	}

	public MessageId(Address originator, long id, int threadId) {
		this.originator = originator;
		this.id = id;
	}

	public MessageId(Address address, long id, long startSend) {
		this.originator = address;
		this.id = id;
		this.startSend = startSend;
	}

	public Address getOriginator() {
		return originator;
	}

	public void setOriginator(Address originator) {
		this.originator = originator;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public long getStartTime() {
		return startSend;
	}

	public void setStartTime(long startSend) {
		this.startSend = startSend;
	}

	public Object clone() {
		try {
			MessageId dolly = (MessageId) super.clone();
			dolly.originator = originator;
			return dolly;
		} catch (CloneNotSupportedException e) {
			throw new IllegalStateException();
		}
	}

	@Override
	public void writeTo(DataOutput out) throws Exception {
		Util.writeAddress(originator, out);
		Bits.writeLong(id, out);
		Bits.writeLong(startSend, out);
	}

	@Override
	public void readFrom(DataInput in) throws Exception {
		originator = Util.readAddress(in);
		id = Bits.readLong(in);
		startSend = Bits.readLong(in);
	}

	@Override
	public int size() {
		return Bits.size(id) + Util.size(originator) + Bits.size(startSend);

	}

	@Override
	public int compareTo(MessageId other) {
		return id == other.id ? this.originator.compareTo(other.originator) : id < other.id ? -1 : 1;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		MessageId messageId = (MessageId) o;

		return id == messageId.id
				&& !(originator != null ? !originator.equals(messageId.originator) : messageId.originator != null);

	}

	@Override
	public int hashCode() {
		int result = originator != null ? originator.hashCode() : 0;
		result = 31 * result + (int) (id ^ (id >>> 32));
		return result;
	}

	@Override
	public String toString() {
		return "MessageId[" + originator + ":" + id + ":" + startSend+"]";
	}

}
