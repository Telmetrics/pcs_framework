package org.tm.messaging;

import java.util.Random;

import javax.jms.*;

import org.sipdev.framework.PCSFramework;
import org.sipdev.framework.PCSLog;

public class UnitTest {

	static PQueue pqueue;
	
    public static void main(String args[]) {
		
    	try {
		    PCSFramework framework = PCSFramework.getInstance("");
		    pqueue = framework.getPQueue();
		    
		    int numThreads = Integer.parseInt(args[0]);
		    int numMessages = Integer.parseInt(args[1]);
		    int callDurationRange = Integer.parseInt(args[2]);
		    
		    UnitTest ut = new UnitTest();
		    ut.start(numThreads, numMessages, callDurationRange);
    	}
    	catch(Exception e) {
    		e.printStackTrace(System.out);
    	}
    }
    
    public void start(int numThreads, int numMessages, int callDurationRange) {
    	try {
	    	System.out.println("starting number of sending threads");
	    	for (int i=1;i<=numThreads;i++) {
	    		new Thread(new CdrSender(i, numMessages, callDurationRange)).start();
	    	}
	    	int totalMessages = numThreads * numMessages;
		    while (true) {
		    	Thread.sleep(10000);
		    	long numPutItems = pqueue.getNumPutItems();
		    	long numRemovedItems = pqueue.getNumRemovedItems();
		    	System.out.println("stats:put=" + String.valueOf(numPutItems) + ",rem=" + String.valueOf(numRemovedItems));
		    	if (numRemovedItems == totalMessages)
		    		break;
		    }
		    pqueue.terminate();
		    Thread.sleep(60000);
    	}
    	catch(Exception e) {
    		System.out.println("exception: " + e.getMessage());
    	}
    	System.out.println("done");
    }
    
    public class MyMessage implements IMessage {
    	String s1;
    	long l1;
    	int i1;
    	String id;
    	
    	public MyMessage() {
    		id = "";
    		s1 = "string one";
    		l1 = 100;
    		i1 = 10;
    	}
    	
    	public void setId(String id) {
    		this.id = id;
    	}
    	public String getId() {
    		return id;
    	}
    	
    	public void marshall(MapMessage msg) throws Exception {
    		try {
    			msg.setString("id", id);
    			msg.setString("s1", s1);
    			msg.setLong("l1", l1);
    			msg.setInt("i1", i1);
    		}
    		catch (Exception e) {
    			throw e;
    		}
    	}
    	
    	public void unmarshall(Message msg) throws Exception {
    	}
    	
    	public String toString() {
    		String str = "";
    		str += "id=" + String.valueOf(id);
    		str += ",s1=" + s1;
    		str += ",l1=" + String.valueOf(l1);
    		str += ",i1=" + String.valueOf(i1);
    		str += ",";
    		return str;
    	}
    }
    
    public class CdrSender implements Runnable {
    	int threadId;
    	int numCdr;
    	int randomRange;
    	MyMessage msg;
    	Random randGen;
    	public CdrSender(int id, int numMessages, int callDurationRange) {
    		threadId = id;
    		numCdr = numMessages;
    		randomRange = callDurationRange;
    		msg = new MyMessage();
    		
    		randGen = new Random();
    	}
    	
    	public void run() {
    		try {
    			System.out.println("threadId=" + String.valueOf(threadId) + " starting");
    			int cnt = 0;
	    		while (cnt < numCdr) {
	    			msg.setId(String.valueOf(threadId) + "_" + String.valueOf(cnt));
	    			pqueue.PutItem(msg);
	    			cnt += 1;
	    			if (randomRange > 1)
	    				Thread.sleep((randGen.nextInt(randomRange) * 1000) + 7);
	    		}
	    		System.out.println("threadId=" + String.valueOf(threadId) + " finished");
    		}
    		catch(Exception e) {
    			System.out.println("threadId=" + String.valueOf(threadId) + " interrupted: " + e.getMessage());
    			e.printStackTrace(System.out);
    		}
    	}
    }
}
