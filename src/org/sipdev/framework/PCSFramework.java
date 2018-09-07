/*
 * Copyright (c) Pactolus Communications Software, 2006

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation
 files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, 
 publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the 
 Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING 
BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND 
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, 
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, 
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
$Id: PCSFramework.java 796 2007-01-19 16:21:57Z erobbins $
 */

package org.sipdev.framework;

import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.ClassPathResource;
import java.sql.Connection;
import java.sql.SQLException;

import org.tm.Config;
import org.tm.messaging.*;

/**
 * 
 * @author Ed Robbins
 *
 * This is the main class within the Pactolus Application Framework.  
 * <p>The default Pactolus framework leverages three (3) open source techologies:
 * <ul>
 * <li>Spring Framework
 * <li>Log4j Logging
 * <li>Commons DBCP Pooling
 * </ul>
 * 
 * <p>Of these 3 pieces only the Spring Framework is mandatory.  Both the Framework logging and database pooling
 * mechanism are written to interfaces which allows one to develop their own implementations.
 * 
 * <p>While the Spring Framework utilizes the IoC or DI paradigm, this framework does not strictly adhere to that model.  Instead it uses a 
 * mixture of DI and Dependency Pull to make available any resources needed within an application.  In essence this framework
 * becomes a generic object factory that allows components to be plugged into the framework.  Those components are then 
 * available to other components via the framework itself.
 * 
 * The default bean definition for the Pactolus framework is described in a file beans.xml, this file must be located within the classpath
 * of the application in order for the framework to process it.  
 * 
 *
 */
public class PCSFramework  {
  
  private static PCSFramework framework = null;
  //private XmlBeanFactory bf = null;
  GenericApplicationContext bf;
  private PCSLog logger = null;
  private final static String logIdentifier = "org.sipdev.framework.pcsframework";
  private String defaultPoolName = null;
  private String identifier = null;
  private static String syncObject = "SyncObject";
  private static ThreadLocal defaultPoolName_ = new ThreadLocal();
  private static ThreadLocal localResource_ = new ThreadLocal();
  private PQueue pqueue = null;
  private Config tmConfig = null;
  private LineTestConsumer lineTestConsumer = null;
  
  private PCSFramework() {
    this("beans.xml");
  } // private constructor
  
  private PCSFramework(String beanFile) {
	bf = new GenericApplicationContext();
	XmlBeanDefinitionReader xmlReader = new XmlBeanDefinitionReader(bf);
	xmlReader.loadBeanDefinitions(new ClassPathResource(beanFile));
	bf.refresh();
    logger = (PCSLog)bf.getBean("logger");
    logger.log(PCSLog.DEBUG,"Creating new instance of framework",logIdentifier);
    tmConfig = (Config)bf.getBean("TMComponentConfig");
    if (tmConfig != null) {
	    if (tmConfig.getMessagingLoad() == 1) {
		    pqueue = new PQueue(tmConfig.getMessageBrokerUrl());
		    try {
		    	int retValue = pqueue.init();
		    	if (retValue == -2) {
		    		logger.log(PCSLog.ERROR, "fail to initialize pqueue due to data directory validation");    		
		    	}
		    	else if (retValue != 0) {
		    		logger.log(PCSLog.ERROR, "fail to initialize pqueue due to system error");
		    	}
		    }
			catch(Exception ex) {
				logger.log(PCSLog.ERROR, "fail to load pqueue: ", ex);
			}
	    }
	    if (tmConfig.getLineTestLoad() == 1) {
	    	lineTestConsumer = new LineTestConsumer(tmConfig.getLineTestBrokerUrl());
	    	if (lineTestConsumer.init() != 0) {
	    		logger.log(PCSLog.ERROR, "fail to initialize line test consumer");
	    	}
	    }
    }
  } // private constructor 
  
  /**
   * Getter method to return the logger within the framework
   * 
   * @return <code>PCSLog</code>
   */
  public PCSLog getLogger() {
    return logger;
  } // getLogger
  
  /**
   * Getter method to return the persistent queue object PQueue  within the framework
   * 
   * @return <code>org.tm.messaging.PQueue</code>
   */
  public PQueue getPQueue() {
	  return pqueue;
  }
  
  /**
   * Getter method to return the line test consumer object within the framework
   * 
   * @return <code>org.tm.messaging.LineTestConsumer</code>
   */
  public LineTestConsumer getLineTestConsumer() {
	  return lineTestConsumer;
  }
  
  /**
   * Getter method to return the telmetrics Config object within the framework
   * 
   * @return <code>org.tm.Config</code>
   */
  public Config getTMConfig() {
	  return tmConfig;
  }
  
  /**
   * Method to return a connection without specifiying the poolName.  The method will use 
   * the default pool name which can be set via the <code>setDefaultPoolName</code> method.
   * @return Connection
   */
  public Connection getConnection() {
	  return getConnection((String)defaultPoolName_.get());
  } // getConnection
  
  /**
   * Method to return a connection from the named connection pool
   * 
   * @param poolName
   * @return <code>Connection</code> from the named connection pool or null if unavailable.
   */
  public Connection getConnection(String poolName) {
    Connection returnConnection = null;
    PCSManagedConnectionInterface pmci = null;
    logger.log(PCSLog.DEBUG,"Retrieving connection",logIdentifier);
    PCSPoolableDataSource dataSource = (PCSPoolableDataSource)bf.getBean(poolName);
    try {
      try { 
    	  pmci = (PCSManagedConnectionInterface)bf.getBean("connectionResource");
    	  pmci.setManagedConnection(dataSource.getConnection());
      } catch(Exception ex) {
    	  logger.log(PCSLog.DEBUG,"Managed connection is not defined, sending back underlying Connection.",logIdentifier);
    	  pmci = null;
      }
      if(null == pmci) {
        returnConnection =  dataSource.getConnection();
      } else {
    	  returnConnection = pmci;
      }
    } catch (SQLException sqlex) {
      logger.log(PCSLog.ERROR,"Problem getting connection",sqlex,logIdentifier);
      returnConnection = null;
    } catch (Exception ex) {
      logger.log(PCSLog.ERROR,"Caught exception in getConnection.",ex,logIdentifier);
      returnConnection = null;
    } // try catch
   return returnConnection;
  } // getConnection
  
 
  /**
   * Utility method to print pool stats for the given poolName
   * @param poolName
   */
  public void printPoolStats(String poolName) {
   
    PCSPoolableDataSource ds = (PCSPoolableDataSource)bf.getBean(poolName);
    if(ds != null) {
      ds.printStats();
    } else {
      logger.log(PCSLog.ERROR,"No Pool found by name: " + poolName,logIdentifier);
    } // if - else
  } // print pool stats
  
  /**
   * Method to retrieve a resource from the framework.  The resource is defined within your beans.xml file.
   * 
   * @param resourceName - The name of the resource
   * @return <code>Object</code> The resource or null if not found.
   */
  public Object getResource(String resourceName) {
    Object returnObject = null;
    logger.log(PCSLog.INFO,"Retrieving resource: " + logIdentifier);
    returnObject = bf.getBean(resourceName);
    return returnObject;
  } // getResource
  
  /**
   * This method implements the singleton pattern to return an instance of PCSFramework.
   * 
   * 
   * @return A new instance of <code>PCSFramework</code> if non-existed, otherwise the previously created instance.
   */
  public static PCSFramework getInstance() {
    
    if(framework == null) {
      synchronized (PCSFramework.class) {
        if(framework == null ) {
          framework = new PCSFramework();
        } // inner if
      } // synchronized
    } // if
    return framework;
  } // getInstance()
  
  /**
   * This method is used to retrieve an instance of the <code>PCSFramework</code>.  The method utilizes the singleton pattern
   * to return a single instance of the framework.  Passing a non null identifier will call <code>Thread.currentThread().setName()</code> 
   * to name the current thread.  This is particularly helpful when used within an app server running an XTML application.  You can pass
   * the session identifer into this method and all messages logged via log4j will be identified by the session id.  This is highly recommended
   * when trying to troubleshoot application problems on servers running many sessions.
   *  
   * @param identifier
   * @return An instance of the PCSFramework
   */
  public static PCSFramework getInstance(String identifier) {
    PCSFramework returnFramework = PCSFramework.getInstance();
    returnFramework.setIdentifier(identifier);
    return returnFramework;
  } // getInstance

    /**
     * This method is used to retrieve an instance of the <code>PCSFramework</code>.  The method utilizes the singleton pattern
     * to return a single instance of the framework.  Passing a non null identifier will call <code>Thread.currentThread().setName()</code> 
     * to name the current thread.  This is particularly helpful when used within an app server running an XTML application.  You can pass
     * the session identifer into this method and all messages logged via log4j will be identified by the session id.  This is highly recommended
     * when trying to troubleshoot application problems on servers running many sessions.  You may pass a null in for this value to use the default 
     * identifier.
     * 
     * The beanDefinitionFile parameter allows you to specify an alternate bean defintion file, otherwise the framework will default to beans.xml.
     *  
     * @param identifier
     * @param beanDefinitionFile
     * @return An instance of the PCSFramework
     */
    public static PCSFramework getInstance(String identifier,String beanDefinitionFile) {
	PCSFramework returnFramework = new PCSFramework(beanDefinitionFile);
	if(null != identifier) {
	    returnFramework.setIdentifier(identifier);
	}
	return returnFramework;
    }

  /**
   * Method used to set the identifier which is then used to set the name of the executing thread.
   * @param identifier
   */
  public void setIdentifier(String identifier) {
	  synchronized (syncObject) {
		  if (identifier != null) {
				Thread.currentThread().setName(identifier);
			} // if
		} // synchronized
	} // setIdentifier
  
  /**
	 * Method used to set the default pool name. This will allow defininng and
	 * setting a default pool name This
	 * will facilitate calling getConnection().
	 * 
	 * @param defaultPoolName
	 */
  public static void setDefaultPoolName(String defaultPoolName) {
	  defaultPoolName_.set(defaultPoolName);
  } // setDefaultPoolName
  
  /**
   * Method to allow the storage of a resource within a thread of execution for later use.
   * 
   * @param resource
   */
  public static void storeResource(Object resource) {
	  localResource_.set(resource);
  } // storeResource
  
  /**
   * Method to retrieve a stored resource.  
   * 
   * @return Object
   */
  public static Object retrieveStoredResource() {
	  return localResource_.get();
  } // retrieveStoredResource
  
  /**
   * Method to clean up a stored resource
   *
   */
  public static void removeStoredResource() {
	  localResource_.remove();
  }
} // class PCSFramework
/* $Id: PCSFramework.java 796 2007-01-19 16:21:57Z erobbins $ */
