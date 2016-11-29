package org.jgroups.protocols.jzookeeper.testrw;

import java.io.DataInput;
import java.io.DataOutput;
import org.jgroups.util.Bits;
import org.jgroups.util.SizeStreamable;

public class Proposal implements Comparable<Proposal>, SizeStreamable {

	public int ackCount = 0;

	private long zxid = -1;

	private MessageId messageId = null;

	public Proposal() {
	}

	public Proposal(long zxid) {
		this.zxid = zxid;
	}

	public Proposal(long zxid, MessageId messageId) {
		this.zxid = zxid;
		this.messageId = messageId;
	}

	public int getackCount() {
		return ackCount;
	}

	public void setackCount(int count) {
		this.ackCount = count;
	}

	public MessageId getMessageId() {
		return messageId;
	}

	public void setMessageId(MessageId messageId) {
		this.messageId = messageId;
	}

	public long getZxid() {
		return zxid;
	}

	public void setZxid(long zxid) {
		this.zxid = zxid;
	}

	@Override
	public void writeTo(DataOutput out) throws Exception {
		writeMessageId(messageId, out);
		Bits.writeLong(zxid, out);
	}

	@Override
	public void readFrom(DataInput in) throws Exception {
		messageId = readMessageId(in);
		zxid = Bits.readLong(in);
	}

	private void writeMessageId(MessageId id, DataOutput out) throws Exception {
		if (id == null) {
			out.writeShort(-1);
		} else {
			out.writeShort(1);
			id.writeTo(out);
		}
	}

	private MessageId readMessageId(DataInput in) throws Exception {
		short length = in.readShort();
		if (length < 0) {
			return null;
		} else {
			MessageId id = new MessageId();
			id.readFrom(in);
			return id;
		}
	}

	@Override
	public int size() {
		return Bits.size(zxid) + (messageId != null ? messageId.size() : 0);
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
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		Proposal that = (Proposal) o;
		if (zxid != that.zxid)
			return false;
		if (messageId != null ? !messageId.equals(messageId) : that.messageId != null)
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		int result = (int) (zxid ^ (zxid >>> 32));
		result = 31 * result + (messageId != null ? messageId.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return "Proposal [zxid=" + zxid + ":" + "MessageId=" + messageId + "]";
	}

}
