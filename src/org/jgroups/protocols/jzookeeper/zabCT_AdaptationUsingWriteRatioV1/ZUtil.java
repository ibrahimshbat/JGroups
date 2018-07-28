package org.jgroups.protocols.jzookeeper.zabCT_AdaptationUsingWriteRatioV1;

import java.util.Random;

import com.google.common.util.concurrent.AtomicDouble;

public class ZUtil {

	private AtomicDouble  p = new AtomicDouble(0.5);//0.634;//0.5;//0.6339745962155613;//1.0;//
	static double randomValue;
	static Random random = new Random();
	static boolean sendAck = false;



	public ZUtil(double p) {
		this.p.set(p);
	}


	public boolean SendAckOrNoSend(){
		randomValue=random.nextDouble();
		//TO check whether to send Ack or not
		return randomValue <= p.get()? true : false;

	}

	public double getP() {
		return p.get();
	}



	public  void setP(double p) {
		this.p.set(p);
	}
	
	
}
