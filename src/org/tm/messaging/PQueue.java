package org.tm.messaging;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Enumeration;
import java.util.List;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.MapMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.sipdev.framework.PCSFramework;
import org.sipdev.framework.PCSLog;

public class PQueue {

	// state of a runnable thread
	protected static final int STATUS_INIT = 0;
	protected static final int STATUS_RUNNING = 1;
	protected static final int STATUS_STOP = 2;
	protected static final int STATUS_ERROR = 3;
	protected static final int STATUS_DB_ERROR = 4;

	// messageid for being sent to which queue
	// message types for CDR queue
	private static final int TM_CDR_START = 10;
	private static final int TM_CDR_ROUTE = 11;
	private static final int TM_CDR_END = 12;	
	// message type for CDR_FILTERED queue
	private static final int TM_CDR_FILTERED = 20;
	
	
	private static final int DEST_CDR_FILTERED = 1;
	private static final int DEST_CDR_IN_LTEST = 2;
	private static final int DEST_CDR_OUT_LTEST = 3;
	private static final int DEST_CDR = 4;
	private static final int DEST_CDR_START = 5;

	private final int RECONNECT_TIME_DEFAULT = 10000;	// in milliseconds
	
	final int QUEUE_SIZE_DEFAULT = 4000;
	
	final int MAX_NUM_ITEMS_PER_FILE_DEFAULT = 1000; // rule: 1/4 of queue size
	
	final int MAX_AVAIL_BASKET_LIST = 7;		// number of ItemBasket objects preallocated (rule: MAX_AVAIL_BASKET_LIST * MAX_NUM_ITEMS_PER_FILE_DEFAULT > QUEUE)
	
	final int MAX_NUM_MESSAGE_FIELDS = 25;		// make sure cdr map message has less than
	
	final int MAX_NUM_DEFERRED_FILES = 10;

	final int LOADER_TIMEOUT_DEFAULT = 10000;	// in milliseconds
	final int PROCESSOR_TIMEOUT_DEFAULT = 5000;	// in milliseconds
	
	private final Object syncLock = new Object();
	private final Object loadedLock = new Object();
	private final Object deferredLock = new Object();
	
	private final Object processorLock = new Object();
		
	private boolean stopFlag = false;
	
	private List<ItemBasket> availBasketList;
	
	private MapMessage[] itemList;
	private int headIdx;
	private int tailIdx;
	
	private int queueSize = QUEUE_SIZE_DEFAULT;
	private int numItemsPerFile = MAX_NUM_ITEMS_PER_FILE_DEFAULT;
	
	private String itemFilePathDir = "/opt/pcs/tm";
	
	private ItemBasket mBasket = null;
	
	private List<ItemBasket> m_loadedBasketList;
	private List<ItemBasket> m_deferredBasketList;
	
	private int m_loaderWait;
	private boolean m_processorWait;
	
	// ActiveMQ general setting
	ActiveMQConnectionFactory connectionFactory = null;
	protected String user = ActiveMQConnection.DEFAULT_USER;
    protected String password = ActiveMQConnection.DEFAULT_PASSWORD;
    protected boolean transacted = true;
    protected int ackMode = Session.AUTO_ACKNOWLEDGE;
    private String messageBrokerUrl = "";

    // CDR message setting
    private Connection cdrConn = null;
    private Session cdrSession = null;
    private Destination cdrDestination = null;
    private MessageProducer cdrProducer = null;
    
    // CDR_START message setting
    private Connection cdrStartConn = null;
    private Session cdrStartSession = null;
    private Destination cdrStartDestination = null;
    private MessageProducer cdrStartProducer = null;

    // CDR_FILTERED message setting
    private Connection cdrFilteredConn = null;
    private Session cdrFilteredSession = null;
    private Destination cdrFilteredDestination = null;
    private MessageProducer cdrFilteredProducer = null;
    
    // CDR_IN_LTEST message setting
    private Connection cdrInLtestConn = null;
    private Session cdrInLtestSession = null;
    private Destination cdrInLtestDestination = null;
    private MessageProducer cdrInLtestProducer = null;

    // CDR_OUT_LTEST message setting
    private Connection cdrOutLtestConn = null;
    private Session cdrOutLtestSession = null;
    private Destination cdrOutLtestDestination = null;
    private MessageProducer cdrOutLtestProducer = null;

    // worker
    private Thread itemProcessor = null;
    private Thread itemLoader = null;
	private int processorStatus = STATUS_INIT;
	private int loaderStatus = STATUS_INIT;

    // stats info
    private long numPutItems;
    private long numRemovedItems;
    
    //
    // Constructor
    //
    public PQueue() {}
    
    public PQueue(String messageBrokerUrl) {
    	this.messageBrokerUrl = messageBrokerUrl;
    }

	public String getMessageBrokerUrl() {
    	return messageBrokerUrl;
    }
    public void setMessageBrokerUrl(String messageBrokerUrl) {
    	this.messageBrokerUrl = messageBrokerUrl;
    }
    
    public long getNumPutItems() {
    	return numPutItems;
    }
    public long getNumRemovedItems() {
    	return numRemovedItems;
    }

    public int init() {
		try {
			if (!validateDataDirectory()) {
				// log error
				return -2;
			}
			availBasketList = new ArrayList<ItemBasket>();
			m_loadedBasketList = new ArrayList<ItemBasket>();
			m_deferredBasketList = new ArrayList<ItemBasket>();
			
			itemList = new MapMessage[queueSize];
			headIdx = -1;
			tailIdx = -1;
			m_loaderWait = 0;
			m_processorWait = false;
			
			numPutItems = 0;
			numRemovedItems = 0;
			
			for(int i=0;i<queueSize;i++) {
				itemList[i] = (MapMessage)new ActiveMQMapMessage();
			}
			
			// loading data from the last run
			Reload();
			
			itemProcessor = new Thread(new ItemProcessor());
			itemLoader = new Thread(new ItemLoader());

			itemProcessor.start();
			itemLoader.start();
			
			return 0;
		}
		catch(Exception e) {
			//e.printStackTrace(System.out);
			return -1;
		}
	}
	
	public void terminate() {
		stopFlag = true;
	}

	//
	// outside threads use the PutItem method to feed the pqueue
	//
	// Return: 
	public int PutItem(IMessage item)
	{
		// enter critical section
		try {
			synchronized(syncLock) {
					
				if (mBasket != null && (mBasket.isHold()))
				{
					if (mBasket.isDeferred())
					{
						//
						// ongoing and deferred basket
						//
						if (!PutItemIntoBasket(mBasket,item))
						{
							PCSFramework.getInstance().getLogger().log(PCSLog.ERROR, "fail to log item (deferred) to file");
							return -1;
						}
						numPutItems += 1;
						return 0;
					}
		
					//
					// ongoing and loaded basket
					//
					if (_Insert(item))
					{
						numPutItems += 1;
						// item has been loaded in memory
						if (!PutMapMessageItemIntoBasket(mBasket,itemList[tailIdx]))
						{
							// log error
							PCSFramework.getInstance().getLogger().log(PCSLog.ERROR, "fail to log item (onhold)to file");
							return -1;
						}
						return 0;
					}
					// item has NOT been loaded in memory because queue is FULL
					mBasket.clearHold();
					mBasket = CreateNewBasket(ItemBasket.IB_DEFERRED|ItemBasket.IB_HOLD);				
					AppendDeferredBasket(mBasket);
					
					numPutItems += 1;
		
					if (!PutItemIntoBasket(mBasket,item))
					{
						// log error
						PCSFramework.getInstance().getLogger().log(PCSLog.ERROR, "fail to log item (full and deferred) to file");
						return -1;
					}
					return 0;
				}
		
				//
				// non-existing ongoing basket
				//
				numPutItems += 1;
				mBasket = CreateNewBasket(ItemBasket.IB_INIT);
				if (m_deferredBasketList.size() > 0)
				{
					mBasket.addStatusValue(ItemBasket.IB_DEFERRED|ItemBasket.IB_HOLD);
					AppendDeferredBasket(mBasket);
					if (!PutItemIntoBasket(mBasket,item))
					{
						// log error
						PCSFramework.getInstance().getLogger().log(PCSLog.ERROR, "fail to log item (non-existing and deferred) to file");
						return -1;
					}
				}
				else
				{
					if (_Insert(item))
					{
						// item has been loaded in memory
						mBasket.addStatusValue(ItemBasket.IB_LOADED|ItemBasket.IB_HOLD);
						AppendLoadedBasket(mBasket);
						if (!PutMapMessageItemIntoBasket(mBasket,itemList[tailIdx]))
						{
							// log error
							PCSFramework.getInstance().getLogger().log(PCSLog.ERROR, "fail to log item (non-existing and onhold) to file");
							return -1;
						}
					}
					else
					{
						mBasket.addStatusValue(ItemBasket.IB_DEFERRED|ItemBasket.IB_HOLD);
						AppendDeferredBasket(mBasket);
						if (!PutItemIntoBasket(mBasket,item))
						{
							// log error
							PCSFramework.getInstance().getLogger().log(PCSLog.ERROR, "fail to log item (full,onhold and deferred) to file");
							return -1;
						}
					}
				}
				return 0;
			}
		}
		catch(Exception e) {
			return -2;
		}
	}

	private void setupConnection() throws Exception
	{
		try
		{
			//System.out.println("connecting to activemq...");
            // create CDR connection, session and queue
            cdrConn = connectionFactory.createConnection();
            cdrSession = cdrConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            cdrDestination = cdrSession.createQueue("CDR");            
            cdrProducer = cdrSession.createProducer(cdrDestination);
            cdrProducer.setDeliveryMode(DeliveryMode.PERSISTENT);             
            cdrConn.start();

            // create CDR connection, session and queue
            cdrStartConn = connectionFactory.createConnection();
            cdrStartSession = cdrStartConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            cdrStartDestination = cdrStartSession.createQueue("CDR_START");            
            cdrStartProducer = cdrStartSession.createProducer(cdrStartDestination);
            cdrStartProducer.setDeliveryMode(DeliveryMode.PERSISTENT);             
            cdrStartConn.start();

            // create CDR_FILTERED connection, session and queue
            cdrFilteredConn = connectionFactory.createConnection();
            cdrFilteredSession = cdrFilteredConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            cdrFilteredDestination = cdrFilteredSession.createQueue("CDR_FILTERED");            
            cdrFilteredProducer = cdrFilteredSession.createProducer(cdrFilteredDestination);
            cdrFilteredProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
            cdrFilteredConn.start();
            
            // create CDR_IN_LTEST connection, session and queue
            cdrInLtestConn = connectionFactory.createConnection();
            cdrInLtestSession = cdrInLtestConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            cdrInLtestDestination = cdrInLtestSession.createQueue("CDR_IN_LTEST");            
            cdrInLtestProducer = cdrInLtestSession.createProducer(cdrInLtestDestination);
            cdrInLtestProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
            cdrInLtestConn.start();
            
            // create CDR_OUT_LTEST connection, session and queue
            cdrOutLtestConn = connectionFactory.createConnection();
            cdrOutLtestSession = cdrOutLtestConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            cdrOutLtestDestination = cdrOutLtestSession.createQueue("CDR_OUT_LTEST");            
            cdrOutLtestProducer = cdrOutLtestSession.createProducer(cdrOutLtestDestination);
            cdrOutLtestProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
            cdrOutLtestConn.start();
		}
		catch(Exception e) {
			throw e;
		}
	}
	
    private void cleanupConnection()
	{
        try {
        	if (cdrSession != null) {
        		cdrSession.close();
        		cdrSession = null;
        	}
        }
        catch (Exception e) {
        }
        try {
        	if (cdrConn != null) {
        		cdrConn.stop();
        		cdrConn.close();
        		cdrConn = null;
        	}
        }
        catch (Exception e) {
        }		
        try {
        	if (cdrStartSession != null) {
        		cdrStartSession.close();
        		cdrStartSession = null;
        	}
        }
        catch (Exception e) {
        }
        try {
        	if (cdrStartConn != null) {
        		cdrStartConn.stop();
        		cdrStartConn.close();
        		cdrStartConn = null;
        	}
        }
        catch (Exception e) {
        }		
        try {
        	if (cdrFilteredSession != null) {
        		cdrFilteredSession.close();
        		cdrFilteredSession = null;
        	}
        }
        catch (Exception e) {
        }
        try {
        	if (cdrFilteredConn != null) {
        		cdrFilteredConn.stop();
        		cdrFilteredConn.close();
        		cdrFilteredConn = null;
        	}
        }
        catch (Exception e) {
        }
        
        try {
        	if (cdrInLtestSession != null) {
        		cdrInLtestSession.close();
        		cdrInLtestSession = null;
        	}
        }
        catch (Exception e) {
        }
        try {
        	if (cdrInLtestConn != null) {
        		cdrInLtestConn.stop();
        		cdrInLtestConn.close();
        		cdrInLtestConn = null;
        	}
        }
        catch (Exception e) {
        }	
        
        try {
        	if (cdrOutLtestSession != null) {
        		cdrOutLtestSession.close();
        		cdrOutLtestSession = null;
        	}
        }
        catch (Exception e) {
        }
        try {
        	if (cdrOutLtestConn != null) {
        		cdrOutLtestConn.stop();
        		cdrOutLtestConn.close();
        		cdrOutLtestConn = null;
        	}
        }
        catch (Exception e) {
        }	
	}
    
    private class ItemLoader implements Runnable {
    	
    	public ItemLoader() {
    		loaderStatus = STATUS_INIT;
    	}

	    public void run()
	    {
	        try {
	        	loaderStatus = STATUS_RUNNING;

	        	int numItems;
	        	
	        	while(true) {
	        		if (stopFlag)
	        			break;

	        		// check if there's any deferred basket
	        		synchronized(deferredLock) {
	        			while (true) {
		        			if (m_deferredBasketList.size() > 0)
		        				break;
		        			// none of deferred basket
		        			m_loaderWait = 2;	// waiting for a deferred basket
		        			deferredLock.wait(LOADER_TIMEOUT_DEFAULT);
	        				if (stopFlag) {
	        					if (m_loaderWait > 0)
	        						m_loaderWait = 0;
	        					return;
	        				}	        				
	        			}
		        		m_loaderWait = 0;
	        		}

	        		if (stopFlag)
	        			break;
	        			        		
	        		// check if the queue has enough space
	        		synchronized(syncLock) {
	        			while (true) {
		        			numItems = _QSize();
	        				if (((MAX_NUM_ITEMS_PER_FILE_DEFAULT*2) + numItems) <= queueSize)
	        					break;
	        			
	        				m_loaderWait = 1;	// waiting for room
	        				syncLock.wait(LOADER_TIMEOUT_DEFAULT);
	        				if (stopFlag) {
	        					if (m_loaderWait > 0)
	        						m_loaderWait = 0;
	        					return;
	        				}
	        			}
	        			m_loaderWait = 0;
		        		_LoadDeferredBasket();
	        		}
	        	}

	        } catch (Exception e) {
	        	loaderStatus = STATUS_ERROR;
	        }
	    }
    }
    
    private class ItemProcessor implements Runnable {
    	
    	public ItemProcessor() {
    		processorStatus = STATUS_INIT;
    	}

	    public void run()
	    {
	        try {
	        	connectionFactory = new ActiveMQConnectionFactory(user, password, messageBrokerUrl);
	            
	        	processorStatus = STATUS_RUNNING;
	            
	            while (true)
	            {
	    			if (stopFlag)
	    				break;
	    			try {
	    				setupConnection();
	    				while (true) {
	            			if (stopFlag)
	            				break;
	            			MapMessage msg;
	            			synchronized(syncLock) {
	            				while (true) {
	            					msg = _GetItem();
	            					if (msg != null)
	            						break;
	            					m_processorWait = true;
	            					syncLock.wait(PROCESSOR_TIMEOUT_DEFAULT);
	            					if (stopFlag)
	            					{
	            						m_processorWait = false;
	            						break;
	            					}
	            				}
	            			}
	            			if (msg != null) {
	            				// process message
	            				switch(isCdrFilteredMessage(msg))
	            				{
	            					case DEST_CDR_START:
	            						cdrStartProducer.send(msg);
	            						break;
	            					case DEST_CDR:
	            						try
		            					{
			            					// We need to make this version of M2 work with 5.8 and 5.9 cdr service
			            					if (msg.getString("aid").startsWith("H") && msg.getLong("sid") != 0L) {
			            						msg.setInt("TMType", 20);
			            					}
		            					}
		            					catch(Exception e) {}
	            						cdrProducer.send(msg);
	            						break;
	            					case DEST_CDR_FILTERED:
	            						cdrFilteredProducer.send(msg);
	            						break;
	            					case DEST_CDR_IN_LTEST:
	            						cdrInLtestProducer.send(msg);
	            						break;
	            					case DEST_CDR_OUT_LTEST:
	            						cdrOutLtestProducer.send(msg);
	            						break;
	            				}
	            				
	            				boolean needLoaded = RemoveItemAndCheckLoadNeeded();
	            				if (needLoaded) {
	            					synchronized(deferredLock) {
	            						deferredLock.notifyAll();
	            					}
	            				}
	            			}
	    				}
		        	} catch (Exception e) {
		        		//e.printStackTrace(System.out);
		        		PCSFramework.getInstance().getLogger().log(PCSLog.ERROR, "activemq down?");
		                cleanupConnection();
		                PCSFramework.getInstance().getLogger().log(PCSLog.ERROR, "activemq cleanup done?");
		        	}
		        	//synchronized(processorLock) {
		        	//	if (processorStatus != STATUS_STOP) {
		        	//		processorLock.wait(RECONNECT_TIME_DEFAULT);
		        	//	}
		        	//	PCSFramework.getInstance().getLogger().log(PCSLog.ERROR, "processor wakes up");
		        	//}
		        	Thread.sleep(RECONNECT_TIME_DEFAULT);
	            }
	            cleanupConnection();
	        } catch (Exception e) {
	        	//e.printStackTrace(System.out);
	        	PCSFramework.getInstance().getLogger().log(PCSLog.ERROR, "generic exception (pqueue processor):" + e.getMessage());
	        	if (processorStatus == STATUS_RUNNING) {
	        		cleanupConnection();
	        	}
	        	processorStatus = STATUS_ERROR;
	        }
	    }
    }
    
    private int isCdrFilteredMessage(MapMessage msg) {
    	try 
    	{
			int msgType = msg.getInt("TMType");
    		if(msgType == TM_CDR_FILTERED)
    		{
    			if(msg.getString("aid").startsWith("T"))
    			{
    				//Out-bound Line Test
    				return DEST_CDR_OUT_LTEST;
    			}
    			else if(msg.getString("aid").startsWith("P"))
    			{
    				//In-bound Line Test
    				return DEST_CDR_IN_LTEST;
    			}
    			else
    			{
    				return DEST_CDR_FILTERED;
    			}
    		}
    		else if (msgType == TM_CDR_START) {
				return DEST_CDR_START;
    		}
			else {
				return DEST_CDR;
			}
    	}
    	catch(Exception e) {
    		return DEST_CDR_FILTERED;
    	}
    }
    
	private MapMessage _GetItem()
	{
		return (_QSize() > 0)? itemList[headIdx]:null;
	}

	private int _QSize()
	{
		if (headIdx == -1)
		{
			return 0;
		}
		if (tailIdx < headIdx)
		{
			// m_tail is infront of m_head, wrapped around
			return tailIdx + queueSize + 1 - headIdx;
		}
		// m_tail is behind m_head
		return tailIdx + 1 - headIdx;
	};

	private boolean validateDataDirectory()
	{
		try {
			File dir = new File(itemFilePathDir);
			if (!dir.exists()) {
				if (!dir.mkdirs())
					return false;
			}
			if (!dir.isDirectory())
				return false;
			if (!dir.canRead())
				return false;
			if (!dir.canWrite())
				return false;
			return true;
		}
		catch(Exception e) {
			// log error
			return false;
		}
	}
	
	//
	// Reload is executed when starting up PQueue
	// no need to use sync lock
	//
	private int Reload()
	{
		File dir = new File(itemFilePathDir);
		File[] files = dir.listFiles();
		
		if (files.length == 0)
			return 0;
		
		List<String> nameList = new ArrayList<String>();
		List<String> nameList2 = new ArrayList<String>();
		
		for(int i=0;i<files.length;i++)
		{
			String fileName = files[i].getName();
			if (fileName.length() > 4) {
				String ext = fileName.substring(fileName.length() - 4);
				if (ext.compareToIgnoreCase(".cdr") == 0) {
					nameList.add(fileName.substring(0, fileName.length() - 4));
				}
				else if (ext.compareToIgnoreCase(".sts") == 0) {
					nameList2.add(fileName.substring(0, fileName.length() - 4));
				}
			}
		}

		for (int i=0;i<nameList.size();i++) {
			String name = nameList.get(i);
			boolean found = false;
			int idx = 0;
			for (;idx<nameList2.size();idx++) {
				if (name.compareToIgnoreCase(nameList2.get(idx)) == 0) {
					found = true;
					break;
				}
			}
			if (found) {
				ItemBasket itemBasket = CreateBasket(name + ".cdr", ItemBasket.IB_LOADED);
				int ret = _LoadBasket(itemBasket);
				if (ret < 0) {
					// log error: data could be corrupted
				}
				else if (ret == 0) {
					// all items in the file have been processed
				}
				nameList2.remove(idx);
			}
			else
			{
				// put it into deferred list
				ItemBasket itemBasket = CreateBasket(name + ".cdr",ItemBasket.IB_DEFERRED);
				m_deferredBasketList.add(itemBasket);
			}
		}		
		return 0;
	}

	//
	// _LoadBasket reads items from file .cdr guided by file .sts. The method is used by Reload method
	// Return:	0 if all items have already processed
	//				1 if file is loaded successfully
	//				-1 if error
	//
	private int _LoadBasket(ItemBasket itemBasket)
	{
		int idx;

		idx = itemBasket.readItemStatusFile();
		if (idx <= 0)
		{
			// log error
			return -1;
		}
		
		itemBasket.closeFiles();

		itemBasket.setNumItems(idx);
		itemBasket.setNumItemStatus(idx);
		
		BufferedReader input = null;
		
		int numItems = 0;
		
		try {
			input = new BufferedReader(new FileReader(itemBasket.getItemFileName()));
			// skip all already processed lines
			while (numItems < idx) {
				String aLine = input.readLine();
				if (aLine == null)
					break;
				numItems += 1;
			}
			// read unprocessed lines
			while (true) {
				String aLine = input.readLine();
				if (aLine == null)
					break;
				// parse the line into map message
				_insertMessage(aLine, true);
				numItems += 1;				
			}
			input.close();
		}
		catch(Exception e) {
			// log error
		}
		if (numItems > idx) {
			itemBasket.setNumItems(numItems);
			itemBasket.setNumItemStatus(idx);
			AppendLoadedBasket(itemBasket);
			return 1;
		}
		itemBasket.destroy();
		return 0;
	}

	private ItemBasket CreateBasket(String cdrFileName, long status)
	{
		ItemBasket itemBasket;
		
		if (availBasketList.size() > 0) {
			itemBasket = availBasketList.remove(0);
		}
		else {
			itemBasket = new ItemBasket();
		}

		String fileName = itemFilePathDir + "/" + cdrFileName;
		itemBasket.setItemFileName(fileName);
		
		itemBasket.setItemStatusFileName(fileName.substring(0, fileName.length() - 3) + "sts");

		itemBasket.addStatusValue(status);

		return itemBasket;
	}

	private int _insertMessage(String aLine, boolean duplicateCheckOn)
	{
		try
		{
			String[] items = aLine.split(",", MAX_NUM_MESSAGE_FIELDS);
			
			if (headIdx == -1) {
				// queue is empty
				headIdx = tailIdx = 0;
			}
			else {
				tailIdx = (tailIdx + 1) % queueSize;
			}
			MapMessage msg = itemList[tailIdx];
			msg.clearBody();
						
			for (int i=0;i<items.length;i++) {
				int idx = items[i].indexOf('=');
				if (idx == -1)
					break;
				msg.setString(items[i].substring(0, idx), items[i].substring(idx+1));
			}
			if (duplicateCheckOn && !msg.itemExists("chkdup")) {
				msg.setInt("chkdup", 1);
			}
			return 0;
		}
		catch(Exception e) {
			// log error
			return -1;
		}
	}

	private void _LoadDeferredBasket()
	{
		ItemBasket itemBasket;
		
		synchronized(deferredLock) {
			if (m_deferredBasketList.size() == 0)
			{
				return;
			}
			itemBasket = m_deferredBasketList.get(0);
		}
		
		itemBasket.closeFiles();

		BufferedReader input = null;
		
		int numItems = 0;
		
		try {
			input = new BufferedReader(new FileReader(itemBasket.getItemFileName()));
			
			while (true) {
				String aLine = input.readLine();
				if (aLine == null)
					break;
				// parse the line into map message
				_insertMessage(aLine, false);
				numItems += 1;
			}
			input.close();
		}
		catch(Exception e) {
			// log error
		}
		
		if (numItems > 0) {		
			if (itemBasket.isHold())
				itemBasket.closeFiles();
			
			itemBasket.clearDeferred();
	
			itemBasket.addStatusValue(ItemBasket.IB_LOADED);
	
			if (itemBasket.getNumItems() != numItems)
				itemBasket.setNumItems(numItems);
	
			AppendLoadedBasket(itemBasket);
		}
		else {
			itemBasket.destroy();
		}

		synchronized(deferredLock) {
			m_deferredBasketList.remove(0);
		}		
	}

	private boolean PutItemIntoBasket(ItemBasket itemBasket, IMessage item)
	{
		// serialize msg to string
		String cdr = item.toString();
		boolean ret = itemBasket.updateItemFile(cdr);
		if (ret) {
			if (itemBasket.getNumItems() >= MAX_NUM_ITEMS_PER_FILE_DEFAULT) {
				itemBasket.setFull();
			}
		}
		return ret;
	}
	
	private boolean PutMapMessageItemIntoBasket(ItemBasket itemBasket, MapMessage msg)
	{
		// serialize msg to string
		String cdr = "";
		try {
			Enumeration iter = msg.getMapNames();
			while (iter.hasMoreElements()) {
				String fieldName = iter.nextElement().toString();
				cdr += fieldName + "=" + msg.getString(fieldName) + ",";
			}
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			return false;
		}
		boolean ret = itemBasket.updateItemFile(cdr);
		if (ret) {
			if (itemBasket.getNumItems() >= MAX_NUM_ITEMS_PER_FILE_DEFAULT) {
				itemBasket.setFull();
			}
		}
		return ret;
	}
	
	//
	// Return: true if the queue has room for loading deferred items if they exist
	//         false if no need to load.
	//
	private boolean RemoveItemAndCheckLoadNeeded()
	{
		boolean needLoaded = false;
		
		synchronized(syncLock) {
			
			if (_QSize() == 1) {
				headIdx = -1;
				tailIdx = -1;
			}
			else {
				headIdx = (headIdx + 1) % queueSize;
			}
			numRemovedItems += 1;
	
			//
			// if loader is sleeping, wake it up if queue has lots of rooms
			//
			if ((m_loaderWait == 1) && ((_QSize() + (numItemsPerFile*2)) < queueSize))
			{
				needLoaded = true;
			}
			//
			
			ItemBasket itemBasket = m_loadedBasketList.get(0);
			int numUnprocessed = itemBasket.numUnprocessedItems();
			if (numUnprocessed > 0) {
				if (!itemBasket.updateItemStatusFile()) {
					// log error
				}
				if (numUnprocessed > 1)
					return needLoaded;
			}
			
			itemBasket.destroy();
			if (availBasketList.size() < MAX_AVAIL_BASKET_LIST)
				availBasketList.add(itemBasket);
			m_loadedBasketList.remove(0);
			
			return needLoaded;
		}
	}


	private void AppendDeferredBasket(ItemBasket ib)
	{
		synchronized(deferredLock) {
			m_deferredBasketList.add(ib);
			if (m_loaderWait == 2)
				deferredLock.notifyAll();
		}
	}
	
	private void AppendLoadedBasket(ItemBasket ib)
	{
		synchronized(loadedLock) {
			m_loadedBasketList.add(ib);
		}
	}
	
	//private boolean _Insert(MapMessage item)
	private boolean _Insert(IMessage item)
	{
		if (headIdx == -1) {
			// queue is empty
			headIdx = tailIdx = 0;
		}
		else if (((tailIdx + 1) % queueSize) == headIdx) {
			// queue is full
			return false;
		}
		else {
			tailIdx = (tailIdx + 1) % queueSize;
		}
		try {
			MapMessage msg = itemList[tailIdx];
			msg.clearBody();
			item.marshall(msg);
		}
		catch(Exception e) {
			// log error: very bad case in marshall
		}
		// wake up processor if there're enough items for it to process
		if (m_processorWait)
		{
			syncLock.notifyAll();
		}
		return true;
	}
		
	private ItemBasket CreateNewBasket(long status)
	{
		ItemBasket itemBasket;
		if (availBasketList.size() > 0) {
			itemBasket = availBasketList.remove(0);
		}
		else {
			itemBasket = new ItemBasket();
		}

		long t = Calendar.getInstance().getTimeInMillis();
		
		String fileName = itemFilePathDir + "/cdr_" + String.valueOf(t);
		File f = new File(fileName + ".cdr");
		if (f.exists()) {
			int idx = 0;
			while (true) {
				String tmpFileName = fileName + "_" + String.valueOf(idx);
				f = new File(tmpFileName);
				if (!f.exists()) {
					fileName = tmpFileName;
					break;
				}
				idx += 1;
			}
		}
		itemBasket.setItemFileName(fileName + ".cdr");		
		itemBasket.setItemStatusFileName(fileName + ".sts");

		itemBasket.setStatus(status);
		
		return itemBasket;
	}

}
