/**
 * Copyright 2014-2018 yangming.liu<bytefox@126.com>.
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
package org.bytesoft.bytejta.supports.resource.jdbc;

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

import javax.transaction.Status;

import org.bytesoft.bytejta.TransactionBeanFactoryImpl;
import org.bytesoft.transaction.Transaction;
import org.bytesoft.transaction.TransactionBeanFactory;
import org.bytesoft.transaction.TransactionManager;

public class ConnectionImpl implements Connection {
	private Connection delegate;
	private XAConnectionImpl managedConnection;
	private boolean closed;

	public <T> T unwrap(Class<T> iface) throws SQLException {
		return delegate.unwrap(iface);
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return delegate.isWrapperFor(iface);
	}

	public Statement createStatement() throws SQLException {
		this.checkTransactionStatusIfNecessary();

		StatementImpl statement = new StatementImpl();
		statement.setConnection(this);
		statement.setDelegate(delegate.createStatement());
		return statement;
	}

	public PreparedStatement prepareStatement(String sql) throws SQLException {
		this.checkTransactionStatusIfNecessary();

		PreparedStatementImpl statement = new PreparedStatementImpl();
		statement.setConnection(this);
		statement.setDelegate(delegate.prepareStatement(sql));
		return statement;
	}

	public CallableStatement prepareCall(String sql) throws SQLException {
		this.checkTransactionStatusIfNecessary();

		CallableStatementImpl statement = new CallableStatementImpl();
		statement.setConnection(this);
		statement.setDelegate(delegate.prepareCall(sql));
		return statement;
	}

	public String nativeSQL(String sql) throws SQLException {
		return delegate.nativeSQL(sql);
	}

	public void setAutoCommit(boolean autoCommit) throws SQLException {
		delegate.setAutoCommit(autoCommit);
	}

	public boolean getAutoCommit() throws SQLException {
		return delegate.getAutoCommit();
	}

	public void commit() throws SQLException {
		delegate.commit();
	}

	public void rollback() throws SQLException {
		delegate.rollback();
	}

	public void close() throws SQLException {
		this.closed = true;
		delegate.close();
	}

	public boolean isClosed() throws SQLException {
		return this.closed ? true : delegate.isClosed();
	}

	public DatabaseMetaData getMetaData() throws SQLException {
		DatabaseMetaDataImpl metadata = new DatabaseMetaDataImpl();
		DatabaseMetaData delegateMetadata = delegate.getMetaData();
		metadata.setDelegate(delegateMetadata);
		return metadata;
	}

	public void setReadOnly(boolean readOnly) throws SQLException {
		delegate.setReadOnly(readOnly);
	}

	public boolean isReadOnly() throws SQLException {
		return delegate.isReadOnly();
	}

	public void setCatalog(String catalog) throws SQLException {
		delegate.setCatalog(catalog);
	}

	public String getCatalog() throws SQLException {
		return delegate.getCatalog();
	}

	public void setTransactionIsolation(int level) throws SQLException {
		delegate.setTransactionIsolation(level);
	}

	public int getTransactionIsolation() throws SQLException {
		return delegate.getTransactionIsolation();
	}

	public SQLWarning getWarnings() throws SQLException {
		return delegate.getWarnings();
	}

	public void clearWarnings() throws SQLException {
		delegate.clearWarnings();
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		this.checkTransactionStatusIfNecessary();

		StatementImpl statement = new StatementImpl();
		statement.setConnection(this);
		statement.setDelegate(delegate.createStatement(resultSetType, resultSetConcurrency));
		return statement;
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		this.checkTransactionStatusIfNecessary();

		PreparedStatementImpl statement = new PreparedStatementImpl();
		statement.setConnection(this);
		statement.setDelegate(delegate.prepareStatement(sql, resultSetType, resultSetConcurrency));
		return statement;
	}

	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		this.checkTransactionStatusIfNecessary();

		CallableStatementImpl statement = new CallableStatementImpl();
		statement.setConnection(this);
		statement.setDelegate(delegate.prepareCall(sql, resultSetType, resultSetConcurrency));
		return statement;
	}

	public Map<String, Class<?>> getTypeMap() throws SQLException {
		return delegate.getTypeMap();
	}

	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		delegate.setTypeMap(map);
	}

	public void setHoldability(int holdability) throws SQLException {
		delegate.setHoldability(holdability);
	}

	public int getHoldability() throws SQLException {
		return delegate.getHoldability();
	}

	public Savepoint setSavepoint() throws SQLException {
		return delegate.setSavepoint();
	}

	public Savepoint setSavepoint(String name) throws SQLException {
		return delegate.setSavepoint(name);
	}

	public void rollback(Savepoint savepoint) throws SQLException {
		delegate.rollback(savepoint);
	}

	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		delegate.releaseSavepoint(savepoint);
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		this.checkTransactionStatusIfNecessary();

		StatementImpl statement = new StatementImpl();
		statement.setConnection(this);
		statement.setDelegate(delegate.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability));
		return statement;
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		this.checkTransactionStatusIfNecessary();

		PreparedStatementImpl statement = new PreparedStatementImpl();
		statement.setConnection(this);
		statement.setDelegate(delegate.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability));
		return statement;
	}

	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		this.checkTransactionStatusIfNecessary();

		CallableStatementImpl statement = new CallableStatementImpl();
		statement.setConnection(this);
		statement.setDelegate(delegate.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability));
		return statement;
	}

	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		this.checkTransactionStatusIfNecessary();

		PreparedStatementImpl statement = new PreparedStatementImpl();
		statement.setConnection(this);
		statement.setDelegate(delegate.prepareStatement(sql, autoGeneratedKeys));
		return statement;
	}

	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		this.checkTransactionStatusIfNecessary();

		PreparedStatementImpl statement = new PreparedStatementImpl();
		statement.setConnection(this);
		statement.setDelegate(delegate.prepareStatement(sql, columnIndexes));
		return statement;
	}

	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		this.checkTransactionStatusIfNecessary();

		PreparedStatementImpl statement = new PreparedStatementImpl();
		statement.setConnection(this);
		statement.setDelegate(delegate.prepareStatement(sql, columnNames));
		return statement;
	}

	public Clob createClob() throws SQLException {
		return delegate.createClob();
	}

	public Blob createBlob() throws SQLException {
		return delegate.createBlob();
	}

	public NClob createNClob() throws SQLException {
		return delegate.createNClob();
	}

	public SQLXML createSQLXML() throws SQLException {
		return delegate.createSQLXML();
	}

	public boolean isValid(int timeout) throws SQLException {
		return this.closed ? false : delegate.isValid(timeout);
	}

	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		delegate.setClientInfo(name, value);
	}

	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		delegate.setClientInfo(properties);
	}

	public String getClientInfo(String name) throws SQLException {
		return delegate.getClientInfo(name);
	}

	public Properties getClientInfo() throws SQLException {
		return delegate.getClientInfo();
	}

	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		return delegate.createArrayOf(typeName, elements);
	}

	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		return delegate.createStruct(typeName, attributes);
	}

	public void setSchema(String schema) throws SQLException {
		delegate.setSchema(schema);
	}

	public String getSchema() throws SQLException {
		return delegate.getSchema();
	}

	public void abort(Executor executor) throws SQLException {
		delegate.abort(executor);
	}

	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		delegate.setNetworkTimeout(executor, milliseconds);
	}

	public int getNetworkTimeout() throws SQLException {
		return delegate.getNetworkTimeout();
	}

	public void checkTransactionStatusIfNecessary() throws SQLException {
		TransactionBeanFactory beanFactory = TransactionBeanFactoryImpl.getInstance();
		TransactionManager transactionManager = beanFactory.getTransactionManager();

		Transaction transaction = transactionManager.getTransactionQuietly();
		if (transaction == null) {
			return; // return quietly
		} // end-if (transaction == null)

		int transactionStatus = transaction.getTransactionStatus();
		if (Status.STATUS_ACTIVE != transactionStatus && Status.STATUS_MARKED_ROLLBACK != transactionStatus) {
			throw new SQLException("Operation is disabled during the inactive phase of the transaction!");
		} else if (transaction.isTiming() == false) {
			throw new SQLException("Operation is disabled during the inactive phase of the transaction!");
		}
	}

	public XAConnectionImpl getManagedConnection() {
		return managedConnection;
	}

	public void setManagedConnection(XAConnectionImpl managedConnection) {
		this.managedConnection = managedConnection;
	}

	public Connection getDelegate() {
		return delegate;
	}

	public void setDelegate(Connection delegate) {
		this.delegate = delegate;
	}
}
