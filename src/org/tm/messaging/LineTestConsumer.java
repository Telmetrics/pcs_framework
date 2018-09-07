package org.tm.messaging;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.MapMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import java.util.*;
import java.io.*;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.apache.activemq.ActiveMQPrefetchPolicy;

public class LineTestConsumer {
	
	private static final int STATUS_INIT = 0;
	private static final int STATUS_RUNNING = 1;
	private static final int STATUS_STOP = 2;
	private static final int STATUS_ERROR = 3;
	
	private final Object syncLock = new Object();
	private final int resetLTestQueueCounter = 300;
	private int numberOfTimeoutTrials_LTest = 0;
	private int numberOfTimeoutTrials_LTestH = 0;
	private int numberOfTimeoutTrials_Cmd = 0;

	private String lTestQName = "LTEST";
    private Connection lTestConn = null;
    private Session lTestSession = null;
    private Destination lTestDestination = null;
    private MessageConsumer lTestConsumer = null;

	private String lTestHQName = "LTEST_H";
    private Connection lTestHConn = null;
    private Session lTestHSession = null;
    private Destination lTestHDestination = null;
    private MessageConsumer lTestHConsumer = null;
    
	private TopicConnectionFactory  topicConnectionFactory = null;
	private String cmdTopic = "LTEST_CMD_TOPIC";
    private TopicConnection cmdConn = null;
    private TopicSession cmdSession = null;
    private Topic cmdDestination = null;
    private TopicSubscriber cmdConsumer = null;
    
    // prefetch policy code setting
    //private ActiveMQPrefetchPolicy lTestPrefetchPolicy = null;

	private ActiveMQConnectionFactory connectionFactory = null;
	private String user = ActiveMQConnection.DEFAULT_USER;
    private String password = ActiveMQConnection.DEFAULT_PASSWORD;
    private String url = "";		//"tcp://172.20.40.190:61616";
    private boolean transacted = false;
    private int ackMode = Session.CLIENT_ACKNOWLEDGE;//Session.AUTO_ACKNOWLEDGE;

	private int status = STATUS_INIT;
	
	private List<MapMessage> retryMessages;
	
	private boolean stopFlag = false;
	private Thread cmdProcessor = null;
	private int processorStatus = STATUS_INIT;
	
	public LineTestConsumer(String msgBrokerUrl) {
		url = msgBrokerUrl;
		retryMessages = new ArrayList<MapMessage>();
	}
	
    public String getUrl() {
    	return url;
    }
    public void setUrl(String url) {
    	this.url = url;
    }
    
    private void cleanup() {
    	if (lTestSession != null) {
    		try {
    			lTestSession.close();
    		} catch(Exception ex) {}
    		lTestSession = null;
    	}
    	if (lTestConn != null) {
    		try {
    			lTestConn.stop();
    			lTestConn.close();
    		} catch(Exception ex) {}
    		lTestConn = null;
    	}    		
    	if (lTestHSession != null) {
    		try {
    			lTestHSession.close();
    		} catch(Exception ex) {}
    		lTestHSession = null;
    	}
    	if (lTestHConn != null) {
    		try {
    			lTestHConn.stop();
    			lTestHConn.close();
    		} catch(Exception ex) {}
    		lTestHConn = null;
    	}
    }
    
    private void cleanupCmdConnection() {
    	if (cmdSession != null) {
    		try {
    			cmdSession.close();
    		} catch(Exception ex) {}
    	}
    	if (cmdConn != null) {
    		try {
    			cmdConn.stop();
    			cmdConn.close();
    		} catch(Exception ex) {}
    		cmdConn = null;
    	}
    }
    
	private void setupConnection() throws Exception
	{
		try
		{
            // create LTEST message listener
            lTestConn = connectionFactory.createConnection();
            lTestSession = lTestConn.createSession(transacted, ackMode);
            lTestDestination = lTestSession.createQueue(lTestQName);
            lTestConsumer = lTestSession.createConsumer(lTestDestination);

            // create LTEST_H message listener
            lTestHConn = connectionFactory.createConnection();
            lTestHSession = lTestHConn.createSession(transacted, ackMode);
            lTestHDestination = lTestHSession.createQueue(lTestHQName);
            lTestHConsumer = lTestHSession.createConsumer(lTestHDestination);

        	numberOfTimeoutTrials_LTest = 0;
        	numberOfTimeoutTrials_LTestH = 0;

            lTestConn.start();
            lTestHConn.start();
            
            status = STATUS_RUNNING;
		}
		catch(Exception e) {
			throw e;
		}
	}
	
	private void setupCmdConnection() throws Exception
	{
		try
		{
            cmdConn = topicConnectionFactory.createTopicConnection();
            cmdSession = cmdConn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            cmdDestination = cmdSession.createTopic(cmdTopic);
            cmdConsumer = cmdSession.createSubscriber(cmdDestination);

            numberOfTimeoutTrials_Cmd = 0;
            
            cmdConn.start();
            
            processorStatus = STATUS_RUNNING;
		}
		catch(Exception e) {
			throw e;
		}
	}
	
    public int init() {
		try {
			connectionFactory = new ActiveMQConnectionFactory(user, password, url);
	    	topicConnectionFactory = new ActiveMQConnectionFactory(user, password, url);
	    	// prefetch policy code setting
            //lTestPrefetchPolicy = new ActiveMQPrefetchPolicy();
            //lTestPrefetchPolicy.setQueuePrefetch(1);
            //connectionFactory.setPrefetchPolicy(lTestPrefetchPolicy);
	    	cmdProcessor = new Thread(new CommandProcessor());
	    	cmdProcessor.start();
			return 0;
		}
		catch(Exception e) {
			//e.printStackTrace(System.out);
			return -1;
		}
    }
    
	public void terminate() {
		stopFlag = true;
		cleanup();
		cleanupCmdConnection();
	}
	
    private class CommandProcessor implements Runnable {
    	
    	public CommandProcessor() {
    		processorStatus = STATUS_INIT;
    	}

	    public void run()
	    {
	    	final int messageFetchTimeout = 10000;
	    	final int resetCmdQueueCounter = 18;
        	while(true) {
        		if (stopFlag)
        			break;
        		try {
        			Message msg = null;
	        		if (processorStatus != STATUS_RUNNING) {
	        			setupCmdConnection();
	        		}
	        		msg = cmdConsumer.receive(messageFetchTimeout);
	        		if (msg != null) {
	        			// processing received msg
	        			if (msg instanceof MapMessage) {
	        				MapMessage mapMsg = (MapMessage)msg;
	        				String cmd = mapMsg.getString("cmd");
	        				if (cmd.compareToIgnoreCase("stop") == 0) {
			        			synchronized(syncLock) {
			        				status = STATUS_STOP;
			        				cleanup();
			        			}
	        				}
	        				else if (cmd.compareToIgnoreCase("start") == 0) {
	        					synchronized(syncLock) {
	        						status = STATUS_INIT;
	        					}
	        				}
	        			}
	        			msg = null;
	        			numberOfTimeoutTrials_Cmd = 0;
	        		}
	        		else {
	        			numberOfTimeoutTrials_Cmd += 1;
	        			if (numberOfTimeoutTrials_Cmd == resetCmdQueueCounter) {
	        				cleanupCmdConnection();
	        				processorStatus = STATUS_STOP;
	        			}
	        		}
        		}
        		catch(Exception ex) {
        			cleanupCmdConnection();
        			try {
	        			synchronized(syncLock) {
	        				syncLock.wait(20000);
	        			}
        			}catch(Exception ex2){}
        			processorStatus = STATUS_ERROR;
        		}
        	}
	    }
    }
    
	//
	// Get next line test
	// if retryMessages has a retried message, return it
	// Otherwise, fetch a message from queue
	//
	public int GetLTestMessage(IMessage ltest) {
		final int messageFetchTimeout = 3;
		synchronized(syncLock) {
			try {
				if (retryMessages.size() > 0) {
					MapMessage mmsg = retryMessages.remove(0);
					ltest.unmarshall(mmsg);
					return 1;
				}
			}
			catch(Exception ex) {
				// log error
				return -2;
			}
			try {
				if (status == STATUS_STOP) {
					return 0;
				}
				Message msg = null;
				if (status != STATUS_RUNNING) {
					setupConnection();
				}
				// with NoWait, amq returns no message when many consumers are racing together to get avail msg
				//msg = lTestHConsumer.receiveNoWait();
				msg = lTestHConsumer.receive(messageFetchTimeout);
				if (msg == null) {
					numberOfTimeoutTrials_LTestH += 1;
					if (numberOfTimeoutTrials_LTestH == resetLTestQueueCounter) {
						status = STATUS_INIT;
						cleanup();
						return 0;
					}
					//msg = lTestConsumer.receiveNoWait();
					msg = lTestConsumer.receive(messageFetchTimeout);
					if (msg == null) {
						numberOfTimeoutTrials_LTest += 1;
						if (numberOfTimeoutTrials_LTest == resetLTestQueueCounter) {
							status = STATUS_INIT;
							cleanup();
						}
						return 0;
					}
					numberOfTimeoutTrials_LTest = 0;
					ltest.unmarshall(msg);
					//if (transacted)
					//	lTestSession.commit();
					//else if (ackMode == Session.CLIENT_ACKNOWLEDGE)
					//	msg.acknowledge();
				}
				else {
					numberOfTimeoutTrials_LTestH = 0;
					ltest.unmarshall(msg);
					//if (transacted)
					//	lTestHSession.commit();
					//else if (ackMode == Session.CLIENT_ACKNOWLEDGE)
					//	msg.acknowledge();
				}
				// remove this when transacted and ackMode are configurable and above commented codes are uncommented.
				msg.acknowledge();
			}
			catch(Exception ex) {
				//ex.printStackTrace(System.out);
				status = STATUS_ERROR;
				cleanup();
				return -1;
			}
		}
		return 1;
	}
	
	public void PutRetryLTestMessage(IMessage ltest) {
		synchronized(syncLock) {
			try {
				//if (status != STATUS_RUNNING) {
				//	setupConnection();
				//}
				//MapMessage mmsg = lTestSession.createMapMessage();
				MapMessage mmsg = new ActiveMQMapMessage();
				ltest.marshall(mmsg);
				retryMessages.add(mmsg);
			}
			catch(Exception ex) {
				// log error
			}
		}
	}
}
