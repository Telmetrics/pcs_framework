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
 */

package org.sipdev.framework;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * The <code>PCSLog4jLogger</code> is a concrete implementation of the <code>PCSLog</code> interface.  
 * There are five (5) levels of logging available:
 * 
 * <ul>
 * <li>DEBUG
 * <li>INFO
 * <li>WARN
 * <li>ERROR
 * <li>FATAL
 * </ul>
 * 
 * <p>These 5 levels directly relate to the named levels within the log4j logger and as such, this class is an adapter
 * to the log4j logger class.  By default it will configure itself using the log4j.properties file located in:
 * 
 *  /opt/pcs/config/log4j.properties
 * 
 *  A different file/locations can be specified by calling the setLog4jProperties method and passing in the appropriate value.
 *  
 /* $Id: PCSLog4jLogger.java 948 2007-06-26 19:57:20Z erobbins $ 
 *
 */


public class PCSLog4jLogger implements PCSLog {

  private final String FQN = "org.sipdev";
  private String log4jProperties = (ClassLoader.getSystemResource("log4j.properties") == null)? "":ClassLoader.getSystemResource("log4j.properties").toExternalForm();
  //private String log4jProperties = ClassLoader.getSystemResource("log4j.properties").toExternalForm();
  // original ClassLoader.getSystemResource("log4j.properties") returns null
  
  public PCSLog4jLogger() {
	  if (log4jProperties != null && log4jProperties.length() > 5)
		  PropertyConfigurator.configureAndWatch(log4jProperties.substring(5),30);	// Strip file:/ from the string and set 
																					// the thread sleep time to 30 seconds.
  }
  
  /**
   * @param level The level at which the message will be logged.
   * @param message The message to log
   */
  public void log(int level, Object message) {
    log(level,message,FQN);
  }

  /**
   * @param level The level at which the message will be logged.
   * @param message The message to log
   * @param th The exception to log
   */
  public void log(int level, Object message, Throwable th) {
    log(level,message,th,FQN);
  }

  /**
   * @param level The level at which the message will be logged.
   * @param message The message to log
   * @param identifier The identifier used to denote which logger to use.  See log4j.properties file
   */
  public void log(int level, Object message, String identifier) {
    switch(level) {
    case PCSLog.DEBUG:
      Logger.getLogger(identifier).debug(message);
      break;
    case PCSLog.INFO:
      Logger.getLogger(identifier).info(message);
      break;
    case PCSLog.WARN:
      Logger.getLogger(identifier).warn(message);
      break;
    case PCSLog.ERROR:
      Logger.getLogger(identifier).error(message);
      break;
    case PCSLog.FATAL:
      Logger.getLogger(identifier).fatal(message);
      break;
    default:
      Logger.getLogger(identifier).debug(message);
    
    } // switch
  } // log(level,message,identifier)

  /**
   * @param level The level at which the message will be logged.
   * @param message The message to log
   * @param th The exception to log
   * @param identifier The identifier used to denote which logger to use.  See log4j.properties file
   */
  public void log(int level, Object message, Throwable th, String identifier) {
   switch(level) {
     case PCSLog.DEBUG:
       Logger.getLogger(identifier).debug(message,th);
       break;
     case PCSLog.INFO:
       Logger.getLogger(identifier).info(message,th);
       break;
     case PCSLog.WARN:
       Logger.getLogger(identifier).warn(message,th);
       break;
     case PCSLog.ERROR:
       Logger.getLogger(identifier).error(message,th);
       break;
     case PCSLog.FATAL:
       Logger.getLogger(identifier).fatal(message,th);
       break;
     default:
       Logger.getLogger(identifier).debug(message,th);
         
   } // switch
    
  } // log(level,message,throwable,identifier)

  /**
   * 
   * @return A string representing the path to the configured log4j.properties file
   */
  public String getLog4jProperties() {
    return log4jProperties;
  }

  /**
   * 
   * @param log4jProperties 
   */
  public void setLog4jProperties(String p_log4jProperties) {
      if (p_log4jProperties != null && p_log4jProperties.length() > 5) {
	  if (ClassLoader.getSystemResource(p_log4jProperties) != null) {
	      this.log4jProperties = ClassLoader.getSystemResource(p_log4jProperties).toExternalForm();
	      PropertyConfigurator.configureAndWatch(p_log4jProperties.substring(5),30);      // Strip file:/ from the string and set
	      this.info("setting log4j properties to: " + this.log4jProperties);
	  } else {
              this.error("Failed setting log4j properties to: " + p_log4jProperties);
	  }

      }
  }

public void debug(Object message, Throwable th, String identifier) {
	this.log(DEBUG,message,th,identifier);
}

public void debug(Object message, String identifier) {
	this.log(DEBUG,message,identifier);
}

public void debug(Object message) {
	this.log(DEBUG,message);
}

public void debug(Object message, Throwable th) {
	this.log(DEBUG,message,th);
}

public void error(Object message, Throwable th, String identifier) {
	this.log(ERROR,message,th,identifier);
}

public void error(Object message, String identifier) {
	this.log(ERROR,message,identifier);
}

public void error(Object message) {
	this.log(ERROR,message);
}

public void error(Object message, Throwable th) {
	this.log(ERROR,message,th);
}

public void fatal(Object message, Throwable th, String identifier) {
	this.log(FATAL,message,th,identifier);
	
}

public void fatal(Object message, String identifier) {
	this.log(FATAL,message,identifier);
	
}

public void fatal(Object message) {
	this.log(FATAL,message);
	
}

public void fatal(Object message, Throwable th) {
	this.log(FATAL,message,th);
	
}

public void info(Object message, Throwable th, String identifier) {
	this.log(INFO,message,th,identifier);
	
}

public void info(Object message, String identifier) {
this.log(INFO,message,identifier);
}

public void info(Object message) {
this.log(INFO,message);
}

public void info(Object message, Throwable th) {
this.log(INFO,message,th);
}

public void warn(Object message, Throwable th, String identifier) {
	this.log(WARN,message,th,identifier);
}

public void warn(Object message, String identifier) {
	this.log(WARN,message,identifier);
}

public void warn(Object message) {
this.log(WARN,message);
}

public void warn(Object message, Throwable th) {
this.log(WARN,message,th);
}

} // PCSLog4jLogger
