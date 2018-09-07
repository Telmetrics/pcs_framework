package org.tm.messaging;

import java.io.*;

public class ItemBasket {

	static public int IB_INIT = 0x00000000;
	static public int IB_HOLD = 0x00000001;
	static public int IB_LOADED	= 0x00000002;
	static public int IB_DEFERRED = 0x00000004;

	long m_status;
	int	m_numItems;
	int m_numItemStatus;
	
	String m_itemFileName;
	String m_itemStatusFileName;
	
	BufferedWriter m_itemWriter;
	BufferedWriter m_statusWriter;

	public ItemBasket() {
		m_status = IB_INIT;
		m_numItems = 0;
		m_numItemStatus = 0;
		m_itemFileName = "";
		m_itemStatusFileName = "";
		m_itemWriter = null;
		m_statusWriter = null;
	}
	
	public void destroy() {
		try {
			if (m_statusWriter != null) {
				m_statusWriter.close();
			}
		}
		catch(Exception e) {			
		}
		m_statusWriter = null;
		try {
			if (m_itemWriter != null) {
				m_itemWriter.close();
			}
		}
		catch(Exception e) {			
		}
		m_itemWriter = null;

		try {
			if (m_itemFileName.length() > 0) {
				File f = new File(m_itemFileName);
				f.delete();
			}
		}
		catch(Exception e) {}
		try {
			if (m_itemStatusFileName.length() > 0) {
				File f = new File(m_itemStatusFileName);
				f.delete();
			}
		}
		catch(Exception e) {}
		m_status = IB_INIT;
		m_numItems = 0;
		m_numItemStatus = 0;
		m_itemFileName = "";
		m_itemStatusFileName = "";
	}
	
	public String getItemFileName() {
		return m_itemFileName;
	}
	public void setItemFileName(String itemFileName) {
		m_itemFileName = itemFileName;
	}
	
	public String getItemStatusFileName() {
		return m_itemStatusFileName;
	}
	public void setItemStatusFileName(String itemStatusFileName) {
		m_itemStatusFileName = itemStatusFileName;
	}
	
	public int getNumItems() {
		return m_numItems;
	}
	public void setNumItems(int numItems) {
		m_numItems = numItems;
	}
	
	public int getNumItemStatus() {
		return m_numItemStatus;
	}
	public void setNumItemStatus(int numItemStatus) {
		m_numItemStatus = numItemStatus;
	}
	
	public long getStatus() {
		return m_status;
	}
	public void setStatus(long status) {
		m_status = status;
	}
	
	public boolean isHold() {
		return (m_status & IB_HOLD) == IB_HOLD;
	}
	public boolean isDeferred() {
		return (m_status & IB_DEFERRED) == IB_DEFERRED;
	}
	
	public void clearHold() {
		m_status &= ~IB_HOLD;
	}
	
	public void clearDeferred() {
		m_status &= ~IB_DEFERRED;
	}
	
	public void addStatusValue(long status) {
		m_status |= status;
	}
	
	public boolean hasUnprocessedItem() {
		return m_numItems > m_numItemStatus;
	}
	
	public int numUnprocessedItems() {
		return m_numItems - m_numItemStatus;
	}
	
	public void setFull() {
		try {
			if (m_itemWriter != null) {
				m_itemWriter.close();
			}
		}
		catch(Exception e) {			
		}
		m_itemWriter = null;
		m_status &= ~IB_HOLD;
	}
	
	public void closeFiles() {
		try {
			if (m_statusWriter != null) {
				m_statusWriter.close();
			}
		}
		catch(Exception e) {			
		}
		m_statusWriter = null;
		try {
			if (m_itemWriter != null) {
				m_itemWriter.close();
			}
		}
		catch(Exception e) {			
		}
		m_itemWriter = null;
	}
	
	public boolean updateItemStatusFile() {
		try {
			if (m_statusWriter == null && m_itemStatusFileName.length() == 0)
				return false;
	
			if (m_statusWriter == null)
			{
				m_statusWriter = new BufferedWriter(new FileWriter(m_itemStatusFileName));
			}
			m_statusWriter.write(String.valueOf(m_numItemStatus + 1));
			m_statusWriter.newLine();
			m_statusWriter.flush();
			m_numItemStatus += 1;
			return true;
		}
		catch(Exception e) {
			return false;
		}
	}
	
	public boolean updateItemFile(String data) {
		try {
			if (m_itemWriter == null && m_itemFileName.length() == 0)
				return false;
	
			if (m_itemWriter == null)
			{
				m_itemWriter = new BufferedWriter(new FileWriter(m_itemFileName));
			}
			m_itemWriter.write(data);
			m_itemWriter.newLine();
			m_itemWriter.flush();
			m_numItems += 1;
			return true;
		}
		catch(Exception e) {
			return false;
		}
	}

	public int readItemStatusFile() {
		String lastLine = "0";
		
		BufferedReader input = null;
		try {
			input = new BufferedReader(new FileReader(m_itemStatusFileName));
			
			while (true) {
				String aLine = input.readLine();
				if (aLine == null)
					break;
				// parse the line into map message
				lastLine = aLine;
			}
			input.close();
		}
		catch(Exception e) {
			// log error
			return 0;
		}
		try {
			return Integer.parseInt(lastLine);
		}
		catch(Exception e) {
			// log error
			return 0;
		}
	}
}
