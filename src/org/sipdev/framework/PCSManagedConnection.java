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
$Id: PCSManagedConnection.java 599 2006-12-22 21:30:57Z erobbins $
 */
package org.sipdev.framework;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLClientInfoException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLXML;

import java.util.Map;
import java.util.Properties;
import java.sql.Array;
import java.sql.Struct;

public class PCSManagedConnection implements PCSManagedConnectionInterface {
	private Connection managedConnection = null;
	private PCSLog logger = PCSFramework.getInstance().getLogger();
	private static final String logidentifier = "org.sipdev.framework.pcsmanagedconnection";
	private long id = System.currentTimeMillis();
	
	private PCSManagedConnection() {
		
	}
	public PCSManagedConnection(Connection conn) {
		logger.log(PCSLog.DEBUG, "Creating a managed connection",logidentifier);
		logger.log(PCSLog.DEBUG, "id: " + id,logidentifier);
		this.managedConnection = conn;
		
	}

    public Struct createStruct(String typeName, Object[] attributes) 
	throws SQLException {
	return managedConnection.createStruct(typeName, attributes);
    }

    public Array createArrayOf(String typeName, Object[] elements)
	throws SQLException {
        return managedConnection.createArrayOf(typeName, elements);
    }

    public Properties getClientInfo()
        throws SQLException {
        return managedConnection.getClientInfo();
    }

    public String getClientInfo(String name)
	throws SQLException {
	return managedConnection.getClientInfo(name);
    }

    public void setClientInfo(Properties properties)
	throws SQLClientInfoException {
	managedConnection.setClientInfo(properties);
    }

    public void setClientInfo(String name, String value)
	throws SQLClientInfoException {
	managedConnection.setClientInfo(name, value);
    }

    public boolean isValid(int timeout)
	throws SQLException{
	return managedConnection.isValid(timeout);
    }

    public SQLXML createSQLXML()
	throws SQLException{
	return managedConnection.createSQLXML();

    }

    public Clob createClob()
	throws SQLException{
	return managedConnection.createClob();
    }

    public Blob createBlob()
        throws SQLException{
        return managedConnection.createBlob();
    }

    public NClob createNClob()
        throws SQLException{
        return managedConnection.createNClob();
    }

    public boolean isWrapperFor(Class<?> iface)
        throws SQLException {
        return managedConnection.isWrapperFor(iface);
    }

    public <T> T unwrap(Class<T> iface)
        throws SQLException {
	return managedConnection.unwrap(iface);

    }
	public void clearWarnings() throws SQLException {
		managedConnection.clearWarnings();

	}

	public void close() throws SQLException {
		logger.log(PCSLog.DEBUG, "Closing id: " + id,logidentifier);
		managedConnection.close();

	}

	public void commit() throws SQLException {
		managedConnection.commit();

	}

	public Statement createStatement() throws SQLException {
		return managedConnection.createStatement();
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency)
			throws SQLException {
		
		return managedConnection.createStatement(resultSetType,resultSetConcurrency);
	}

	public Statement createStatement(int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		return managedConnection.createStatement(resultSetType,resultSetConcurrency,resultSetHoldability);
	}

	public boolean getAutoCommit() throws SQLException {
		
		return managedConnection.getAutoCommit();
	}

	public String getCatalog() throws SQLException {
		return managedConnection.getCatalog();
	}

	public int getHoldability() throws SQLException {
		return managedConnection.getHoldability();
	}

	public DatabaseMetaData getMetaData() throws SQLException {
		return managedConnection.getMetaData();
	}

	public int getTransactionIsolation() throws SQLException {
		return managedConnection.getTransactionIsolation();
	}

	public Map<String, Class<?>> getTypeMap() throws SQLException {
		return managedConnection.getTypeMap();
	}

	public SQLWarning getWarnings() throws SQLException {
		return managedConnection.getWarnings();
	}

	public boolean isClosed() throws SQLException {
		return managedConnection.isClosed();
	}

	public boolean isReadOnly() throws SQLException {
		return managedConnection.isReadOnly();
	}

	public String nativeSQL(String sql) throws SQLException {
		return managedConnection.nativeSQL(sql);
	}

	public CallableStatement prepareCall(String sql) throws SQLException {
		return managedConnection.prepareCall(sql);
	}

	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		return managedConnection.prepareCall(sql,resultSetType,resultSetConcurrency);
	}

	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		return managedConnection.prepareCall(sql,resultSetType,resultSetConcurrency,resultSetHoldability);
	}

	public PreparedStatement prepareStatement(String sql) throws SQLException {
		return managedConnection.prepareStatement(sql);
	}

	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
			throws SQLException {
		return managedConnection.prepareStatement(sql,autoGeneratedKeys);
	}

	public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
			throws SQLException {
		return managedConnection.prepareStatement(sql);
	}

	public PreparedStatement prepareStatement(String sql, String[] columnNames)
			throws SQLException {
		return managedConnection.prepareStatement(sql,columnNames);
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		return managedConnection.prepareStatement(sql,resultSetType,resultSetConcurrency);
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		return managedConnection.prepareStatement(sql,resultSetType,resultSetConcurrency,resultSetHoldability);
	}

	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		managedConnection.releaseSavepoint(savepoint);

	}

	public void rollback() throws SQLException {
		managedConnection.rollback();

	}

	public void rollback(Savepoint savepoint) throws SQLException {
		managedConnection.rollback(savepoint);

	}

	public void setAutoCommit(boolean autoCommit) throws SQLException {
		managedConnection.setAutoCommit(autoCommit);

	}

	public void setCatalog(String catalog) throws SQLException {
		managedConnection.setCatalog(catalog);

	}

	public void setHoldability(int holdability) throws SQLException {
		managedConnection.setHoldability(holdability);

	}

	public void setReadOnly(boolean readOnly) throws SQLException {
		managedConnection.setReadOnly(readOnly);

	}

	public Savepoint setSavepoint() throws SQLException {
		return managedConnection.setSavepoint();
	}

	public Savepoint setSavepoint(String name) throws SQLException {
		return managedConnection.setSavepoint(name);
	}

	public void setTransactionIsolation(int level) throws SQLException {
		managedConnection.setTransactionIsolation(level);

	}

	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		managedConnection.setTypeMap(map);

	}
	public Connection getManagedConnection() {
		return managedConnection;
	}
	public void setManagedConnection(Connection managedConnection) {
		logger.log(PCSLog.DEBUG, "Creating a managed connection",logidentifier);
		logger.log(PCSLog.DEBUG, "id: " + id,logidentifier);
		this.managedConnection = managedConnection;
	}

}
