package org.tm.messaging;

import javax.jms.*;

import org.sipdev.framework.PCSFramework;

public class UnitTestLineTest {

	static LineTestConsumer ltConsumer;
	
    public static void main(String args[]) {
    	UnitTestLineTest ut = new UnitTestLineTest();
    	ut.testIt();
    }
    
    public void testIt() {
    	try {
		    PCSFramework framework = PCSFramework.getInstance("");
		    ltConsumer = framework.getLineTestConsumer();
		    
		    MyMessage lTest = new MyMessage();
		    ltConsumer.GetLTestMessage(lTest);
    	}
    	catch(Exception e) {
    		e.printStackTrace(System.out);
    	}    	
    }

    public class MyMessage implements IMessage {
    	public MyMessage() {
    	}
    	
    	public void marshall(MapMessage msg) throws Exception {
    	}
    	
    	public void unmarshall(Message msg) throws Exception {
    		System.out.println("unmashall");
    	}
    	
    	public String toString() {
    		return "";
    	}
    }
}
