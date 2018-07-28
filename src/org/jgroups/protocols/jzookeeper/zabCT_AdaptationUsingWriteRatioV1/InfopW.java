package org.jgroups.protocols.jzookeeper.zabCT_AdaptationUsingWriteRatioV1;

public class InfopW implements Comparable{
	private double p=0.0;
	private double wPlus1=0.0;
	
	public InfopW(double p, double wPlus1) {
		this.p = p;
		this.wPlus1 = wPlus1;
	}

	public double getP() {
		return p;
	}

	public void setP(double p) {
		this.p = p;
	}

	public double getwPlus1() {
		return wPlus1;
	}

	public void setwPlus1(double wPlus1) {
		this.wPlus1 = wPlus1;
	}

	@Override
	public String toString() {
		return "InfopW [p=" + p + ", wPlus1=" + wPlus1 + "]";
	}

	@Override
	public int compareTo(Object o) {
		InfopW info = (InfopW) o;
		if(this.p>info.getP())
			return 1;
		if(this.p<info.getP())
			return -1;
		else
			return 0;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(p);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(wPlus1);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		InfopW other = (InfopW) obj;
		if (Double.doubleToLongBits(p) != Double.doubleToLongBits(other.p))
			return false;
		if (Double.doubleToLongBits(wPlus1) != Double.doubleToLongBits(other.wPlus1))
			return false;
		return true;
	}
	
	
	
	
}
