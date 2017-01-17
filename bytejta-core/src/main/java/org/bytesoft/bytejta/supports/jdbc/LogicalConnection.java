/**
 * Copyright 2014-2016 yangming.liu<bytefox@126.com>.
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, see <http://www.gnu.org/licenses/>.
 */
package org.bytesoft.bytejta.supports.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogicalConnection implements Connection {
	static final Logger logger = LoggerFactory.getLogger(LogicalConnection.class);

	private boolean connectionClosed;
	private final LocalXAConnection managedConnection;
	private final Connection delegateConnection;

	public LogicalConnection(LocalXAConnection managedConnection, Connection connection) {
		this.managedConnection = managedConnection;
		this.delegateConnection = connection;
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.unwrap(iface);
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.isWrapperFor(iface);
	}

	public Statement createStatement() throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.createStatement();
	}

	public PreparedStatement prepareStatement(String sql) throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.prepareStatement(sql);
	}

	public CallableStatement prepareCall(String sql) throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.prepareCall(sql);
	}

	public String nativeSQL(String sql) throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.nativeSQL(sql);
	}

	public void setAutoCommit(boolean autoCommit) throws SQLException {
		this.validateConnectionStatus();
		delegateConnection.setAutoCommit(autoCommit);
	}

	public boolean getAutoCommit() throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.getAutoCommit();
	}

	public void commit() throws SQLException {
		// this.validateConnectionStatus();
		managedConnection.commitLocalTransaction();
	}

	public void rollback() throws SQLException {
		// this.validateConnectionStatus();
		managedConnection.rollbackLocalTransaction();
	}

	public synchronized void close() throws SQLException {
		if (this.connectionClosed) {
			logger.debug("Current connection has already been closed.");
		} else {
			this.connectionClosed = true;
			managedConnection.closeLogicalConnection();
		}
	}

	public boolean isClosed() throws SQLException {
		return this.connectionClosed;
	}

	public DatabaseMetaData getMetaData() throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.getMetaData();
	}

	public void setReadOnly(boolean readOnly) throws SQLException {
		this.validateConnectionStatus();
		delegateConnection.setReadOnly(readOnly);
	}

	public boolean isReadOnly() throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.isReadOnly();
	}

	public void setCatalog(String catalog) throws SQLException {
		this.validateConnectionStatus();
		delegateConnection.setCatalog(catalog);
	}

	public String getCatalog() throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.getCatalog();
	}

	public void setTransactionIsolation(int level) throws SQLException {
		this.validateConnectionStatus();
		delegateConnection.setTransactionIsolation(level);
	}

	public int getTransactionIsolation() throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.getTransactionIsolation();
	}

	public SQLWarning getWarnings() throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.getWarnings();
	}

	public void clearWarnings() throws SQLException {
		this.validateConnectionStatus();
		delegateConnection.clearWarnings();
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.createStatement(resultSetType, resultSetConcurrency);
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.prepareStatement(sql, resultSetType, resultSetConcurrency);
	}

	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.prepareCall(sql, resultSetType, resultSetConcurrency);
	}

	public Map<String, Class<?>> getTypeMap() throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.getTypeMap();
	}

	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		this.validateConnectionStatus();
		delegateConnection.setTypeMap(map);
	}

	public void setHoldability(int holdability) throws SQLException {
		this.validateConnectionStatus();
		delegateConnection.setHoldability(holdability);
	}

	public int getHoldability() throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.getHoldability();
	}

	public Savepoint setSavepoint() throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.setSavepoint();
	}

	public Savepoint setSavepoint(String name) throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.setSavepoint(name);
	}

	public void rollback(Savepoint savepoint) throws SQLException {
		this.validateConnectionStatus();
		delegateConnection.rollback(savepoint);
	}

	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		this.validateConnectionStatus();
		delegateConnection.releaseSavepoint(savepoint);
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.prepareStatement(sql, autoGeneratedKeys);
	}

	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.prepareStatement(sql, columnIndexes);
	}

	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.prepareStatement(sql, columnNames);
	}

	public Clob createClob() throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.createClob();
	}

	public Blob createBlob() throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.createBlob();
	}

	public NClob createNClob() throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.createNClob();
	}

	public SQLXML createSQLXML() throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.createSQLXML();
	}

	public boolean isValid(int timeout) throws SQLException {
		return delegateConnection.isValid(timeout);
	}

	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		try {
			this.validateConnectionStatus();
		} catch (SQLException ex) {
			throw new SQLClientInfoException(null, ex);
		}
		delegateConnection.setClientInfo(name, value);
	}

	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		try {
			this.validateConnectionStatus();
		} catch (SQLException ex) {
			throw new SQLClientInfoException(null, ex);
		}
		delegateConnection.setClientInfo(properties);
	}

	public String getClientInfo(String name) throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.getClientInfo(name);
	}

	public Properties getClientInfo() throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.getClientInfo();
	}

	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.createArrayOf(typeName, elements);
	}

	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.createStruct(typeName, attributes);
	}

	public void setSchema(String schema) throws SQLException {
		this.validateConnectionStatus();
		delegateConnection.setSchema(schema);
	}

	public String getSchema() throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.getSchema();
	}

	public void abort(Executor executor) throws SQLException {
		this.validateConnectionStatus();
		delegateConnection.abort(executor);
	}

	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		this.validateConnectionStatus();
		delegateConnection.setNetworkTimeout(executor, milliseconds);
	}

	public int getNetworkTimeout() throws SQLException {
		this.validateConnectionStatus();
		return delegateConnection.getNetworkTimeout();
	}

	private void validateConnectionStatus() throws SQLException {
		if (this.connectionClosed) {
			throw new SQLException("Connection is closed");
		}
	}

}
