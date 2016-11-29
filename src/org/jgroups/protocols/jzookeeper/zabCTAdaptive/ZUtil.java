package org.jgroups.protocols.jzookeeper.zabCTAdaptive;

import java.util.Random;

public class ZUtil {

	private final static byte zab=1;
	private final static byte zabCT=2;

	private byte protocolusing=0;
	public static double RANDOMP=0.0;
	private double  p = 1.0;//0.634;//0.5;//0.6339745962155613;//1.0;//
	private final static double  min = 0.1, max=1.0;
	static double randomValue;
	static Random random = new Random();
	static boolean sendAck = false;


	public ZUtil(double p) {
		this.p = p;
	}



	public synchronized boolean SendAckOrNoSend(double pValue){

		randomValue=random.nextDouble();
		//TO check whether to send Ack or not
		return randomValue <= pValue? true : false;

	}

	public byte forecast(){
		byte result = random.nextBoolean()?zab:zabCT;
		if (result==zabCT)
			chooseP();
		else
			RANDOMP=0.0;
		return result;
	}

	public void chooseP(){
		RANDOMP =  (random.nextInt((int)((max-min)*10+1))+min*10) / 10.0;
	}

	public double getP() {
		return p;
	}



	public  void setP(double p) {
		this.p = p;
	}


}
