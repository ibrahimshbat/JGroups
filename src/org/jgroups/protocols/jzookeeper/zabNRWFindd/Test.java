package org.jgroups.protocols.jzookeeper.zabNRWFindd;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SortedSet<Timeout> timeoutsA= Collections.synchronizedSortedSet(new TreeSet<Timeout>());
		//Map<Long, SortedSet<Timeout>> mapTimeouts = new HashMap<Long, SortedSet<Timeout>>();
		long counts=33;
		Timeout f1t11 = new Timeout(System.currentTimeMillis());
		f1t11.setZxid("123a");
		Timeout f1t212 = new Timeout(System.currentTimeMillis()+ (++counts));
		f1t212.setZxid("123b");
		timeoutsA.add(f1t11);
		timeoutsA.add(f1t212);
		System.out.println("Before timeouts----------> "+timeoutsA);
		Timeout f1t22 = timeoutsA.first();
		System.out.println("First "+f1t22);
		System.out.println("Remove "+timeoutsA.remove(f1t22));
		System.out.println("timeouts----------> "+timeoutsA);
		System.out.println("Removed Item----------> "+f1t22);
		
		SortedSet<Timeout> timeouts= Collections.synchronizedSortedSet(new TreeSet<Timeout>());
		Map<Long, SortedSet<Timeout>> mapTimeouts = new HashMap<Long, SortedSet<Timeout>>();
		long count=2;
		Timeout f1t1 = new Timeout(System.currentTimeMillis());
		f1t1.setZxid("123a");
		Timeout f1t2 = new Timeout(System.currentTimeMillis()+ (++count));
		f1t2.setZxid("123b");
		timeouts.add(f1t1);
		timeouts.add(f1t2);
		mapTimeouts.put((long)1, timeouts);
		SortedSet<Timeout> timeoutss = mapTimeouts.get((long)1);
		System.out.println("timeoutss "+timeoutss);
		Timeout f1t3 = new Timeout(System.currentTimeMillis()-50);
		f1t3.setZxid("123");
		timeoutss.add(f1t3);
		mapTimeouts.put((long)1, timeoutss);
		//SortedSet<Timeout> timeoutsss = Collections.synchronizedSortedSet(new TreeSet<Timeout>());
		//System.out.println("timeoutss after "+timeoutss);
		System.out.println(" Test mapTimeoutss "+mapTimeouts);
		//System.out.println("First-->"+mapTimeouts.get((long)1).first());
		//timeouts.clear();
		
		timeouts = Collections.synchronizedSortedSet(new TreeSet<Timeout>());
		Timeout f2t1 = new Timeout(System.currentTimeMillis()+(++count));
		f2t1.setZxid("123c");
		Timeout f2t2 = new Timeout(System.currentTimeMillis()+(++count));
		f2t2.setZxid("123d");
		timeouts.add(f2t1);
		timeouts.add(f2t2);
		mapTimeouts.put((long)2, timeouts);
		//timeouts.clear();
		
		timeouts = Collections.synchronizedSortedSet(new TreeSet<Timeout>());
		Timeout f3t1 = new Timeout(System.currentTimeMillis()+(++count));
		f3t1.setZxid("123e");
		Timeout f3t2 = new Timeout(System.currentTimeMillis()+(++count));
		f3t2.setZxid("123f");
		timeouts.add(f3t1);
		timeouts.add(f3t1);
		mapTimeouts.put((long)3, timeouts);
		//timeouts.clear();
		
		timeouts = Collections.synchronizedSortedSet(new TreeSet<Timeout>());
		Timeout f4t1 = new Timeout(System.currentTimeMillis()+(++count));
		f4t1.setZxid("123v");
		Timeout f4t2 = new Timeout(System.currentTimeMillis()+(++count));
		f4t2.setZxid("123n");
		timeouts.add(f4t1);
		timeouts.add(f4t2);
		mapTimeouts.put((long)4, timeouts);
		//timeouts.clear();
		
		System.out.println(mapTimeouts);

	}
	

}
