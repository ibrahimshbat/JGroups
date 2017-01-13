package org.jgroups.protocols.jzookeeper.zabCT_AdaptSwitching;

public class RangeHead {

	private int alpha=1;
	private int alphaPlusb=0;
	private int b=0;
	private double p=0;
	private double WPlus1=0;
	
	public RangeHead() {

	}
	public RangeHead(int alpha, int alphaPlusb) {
		this.alpha = alpha;
		this.alphaPlusb=alphaPlusb;
		this.b = this.alphaPlusb-this.alpha;
	}
	
	public int getAlpha() {
		return alpha;
	}
	public void setAlpha(int alpha) {
		this.alpha = alpha;
	}
	public int getB() {
		return b;
	}
	public void setB(int b) {
		this.b = b;
	}
	
	public int getAlphaPlusb() {
		return alphaPlusb;
	}
	public void setAlphaPlusb(int alphaPlusb) {
		this.alphaPlusb = alphaPlusb;
	}
	public double getP() {
		return p;
	}
	public void setP(double p) {
		this.p = p;
	}
	public double getWPlus1() {
		return WPlus1;
	}
	public void setWPlus1(double wPlus1) {
		WPlus1 = wPlus1;
	}
	@Override
	public String toString() {
		return "{"+ alpha + ", " + alphaPlusb + "}p="+p;
	}
	
}
