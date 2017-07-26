package org.jgroups.protocols.jzookeeper.zabCT_AdaptationUsingWriteRatioSyncV2;

import java.text.DecimalFormat;
import java.util.SortedSet;
import java.util.TreeSet;

public class NoOptimal {

	public static void main(String[] args) {
		SortedSet<Double> pE1 = new TreeSet<Double>();
		ProtocolStats st = new ProtocolStats();
		int dlamda=0,N=3;
		double p1s=0.0,	E2=0.0;
		
		for (double i = 1; i <= 0.05; i-=0.05) {
			
			if(st.findWForRangep(i,N,0)<dlamda){
				pE1.add(i);
			}
		}
		System.out.println(pE1);
	}
	
	public static double findp(int c, int n, double c1, double c2, double P1, double P2, ProtocolStats st){
		final double ABS = 0.005;
		DecimalFormat roundValue = new DecimalFormat("#.000");
		double newp=P1-ABS, newW=0.0, p=0.000;
		while(newp>=P2){
			newp=Double.parseDouble(roundValue.format(newp));
			newW = st.findWForRangep(newp, n+1, c);
			System.out.println("W(P)="+newW+" P="+newp);

			if (newp<=c2 && newW<=c1){	
				p=newp;
				break;
			}
			newp= newp- 0.005;
			//System.out.println("check newp="+newp);
			//log.info("check newp="+newp);
		}
		return p;
	}

}
