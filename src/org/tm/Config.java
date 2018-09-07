package org.tm;

public class Config {

	private int messagingLoad;
	private String messageBrokerUrl;
	
	// line test members
	private int lineTestLoad;
	private String lineTestBrokerUrl;
	
	public Config() {
	    messagingLoad = 0;
	    messageBrokerUrl = "";
	}
	
	public int getMessagingLoad() {
		return this.messagingLoad;
	}
	
	public void setMessagingLoad(int messagingLoad) {
		this.messagingLoad = messagingLoad;
	}
	
	public String getMessageBrokerUrl() {
		return messageBrokerUrl;
	}
	
	public void setMessageBrokerUrl(String messageBrokerUrl) {
		this.messageBrokerUrl = messageBrokerUrl;
	}
	
	// line test config setting
	public int getLineTestLoad() {
		return this.lineTestLoad;
	}
	public void setLineTestLoad(int lineTestLoad) {
		this.lineTestLoad = lineTestLoad;
	}
	public String getLineTestBrokerUrl() {
		return this.lineTestBrokerUrl;
	}
	public void setLineTestBrokerUrl(String lineTestBrokerUrl) {
		this.lineTestBrokerUrl = lineTestBrokerUrl;
	}
}
