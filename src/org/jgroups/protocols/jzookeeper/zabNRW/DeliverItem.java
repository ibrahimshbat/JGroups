package org.jgroups.protocols.jzookeeper.zabNRW;


public class DeliverItem  implements Comparable<DeliverItem> {
	private long zxid=0;
	private boolean isDelivered=false;
	
	
	public DeliverItem(long zxid) {
		this.zxid = zxid;
	}


	public DeliverItem(long zxid, boolean isDelivered) {
		this.zxid = zxid;
		this.isDelivered = isDelivered;
	}


	public long getZxid() {
		return zxid;
	}


	public void setZxid(long zxid) {
		this.zxid = zxid;
	}


	public boolean isDelivered() {
		return isDelivered;
	}


	public void setDelivered(boolean isDelivered) {
		this.isDelivered = isDelivered;
	}
	
	@Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DeliverItem that = (DeliverItem) o;

        if (zxid!=that.getZxid()) return false;

        return true;
    }

    @Override
    public int hashCode() {
    	Long zxidObj=this.zxid;
        return zxidObj.hashCode();
    }

    @Override
    public int compareTo(DeliverItem nextItem) {

        if (this.zxid==nextItem.getZxid())
            return 0;
        else if (this.zxid > nextItem.getZxid())
            return 1;
        else
            return -1;
    }
    

    @Override
    public String toString() {
        return "DeliverItem{" +
                "zxid=" + this.zxid +
                ", isDelivered=" + isDelivered +
                '}';
    }
	
	
	

}
