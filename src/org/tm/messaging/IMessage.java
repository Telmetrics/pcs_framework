package org.tm.messaging;

import javax.jms.MapMessage;
import javax.jms.Message;

public interface IMessage {
	public void marshall(MapMessage message) throws Exception;
	public void unmarshall(Message msg) throws Exception;
	public String toString();
}
