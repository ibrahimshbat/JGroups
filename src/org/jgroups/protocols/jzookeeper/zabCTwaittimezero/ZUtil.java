package org.jgroups.protocols.jzookeeper.zabCTwaittimezero;

import java.util.Random;

public class ZUtil {

	private double  p = 0.5;//0.634;//0.5;//0.6339745962155613;//1.0;//
	static double randomValue;
	static Random random = new Random();
	static boolean sendAck = false;



	public ZUtil(double p) {
		this.p = p;
	}



	public boolean SendAckOrNoSend(){


		randomValue=random.nextDouble();
		//TO check whether to send Ack or not
		return randomValue <= p? true : false;

	}



	public double getP() {
		return p;
	}



	public  void setP(double p) {
		this.p = p;
	}
	
	
}
