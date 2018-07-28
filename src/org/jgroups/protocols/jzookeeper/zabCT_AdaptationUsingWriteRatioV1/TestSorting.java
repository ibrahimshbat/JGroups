package org.jgroups.protocols.jzookeeper.zabCT_AdaptationUsingWriteRatioV1;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TestSorting {

	public static void main(String[] args) {
		
		ACK a1 = new ACK();
		//a1.setAddress("dsfsaf1");
		a1.setZxid(50);
		ACK a2 = new ACK();
		//a2.setAddress("dsfsaf2");
		a2.setZxid(10);
		ACK a3 = new ACK();
		//a3.setAddress("dsfsaf3");
		a3.setZxid(10);
		ACK a4 = new ACK();
		//a4.setAddress("dsfsaf4");
		a4.setZxid(4);
		ACK a5 = new ACK();
		//a5.setAddress("dsfsaf5");
		a5.setZxid(5);
		ACK a6 = new ACK();
		//a6.setAddress("dsfsaf6");
		a6.setZxid(6);
		ACK a7 = new ACK();
		//a7.setAddress("dsfsaf7");
		a7.setZxid(7);

		List<ACK> processes = new ArrayList<ACK>();
		
		//a7.setZxid(0);
		processes.add(a1);
		processes.add(a2);
		processes.add(a3);
		processes.add(a4);
		processes.add(a5);
		processes.add(a6);
		processes.add(a7);
		Collections.sort(processes);
		System.out.println(processes);
		processes.remove(a1);
		System.out.println(processes);
		a1 = new ACK();
		//a1.setAddress("dsfsaf1");
		a1.setZxid(-1);
		processes.add(a1);
		Collections.sort(processes);

		System.out.println(processes);

		
	
		
	}

}
