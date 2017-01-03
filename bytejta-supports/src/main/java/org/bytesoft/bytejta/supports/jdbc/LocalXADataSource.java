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

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import javax.sql.DataSource;
import javax.sql.XADataSource;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;
import javax.transaction.xa.Xid;

import org.bytesoft.bytejta.supports.resource.LocalXAResourceDescriptor;
import org.bytesoft.transaction.Transaction;
import org.bytesoft.transaction.supports.TransactionListener;
import org.bytesoft.transaction.supports.TransactionListenerAdapter;
import org.bytesoft.transaction.xa.TransactionXid;
import org.springframework.beans.factory.BeanNameAware;

public class LocalXADataSource extends TransactionListenerAdapter
		implements XADataSource, DataSource, DataSourceHolder, BeanNameAware, TransactionListener {
	private PrintWriter logWriter;
	private int loginTimeout;

	private DataSource dataSource;
	private String beanName;
	private TransactionManager transactionManager;

	private final Map<Xid, LogicalConnection> connections = new ConcurrentHashMap<Xid, LogicalConnection>();

	public Connection getConnection() throws SQLException {
		TransactionXid transactionXid = null;
		try {
			Transaction transaction = (Transaction) this.transactionManager.getTransaction();
			if (transaction == null) {
				return this.dataSource.getConnection();
			}

			transaction.registerTransactionListener(this);

			transactionXid = transaction.getTransactionContext().getXid();

			LocalXAResourceDescriptor descriptor = null;
			LocalXAResource localRes = null;
			LogicalConnection connection = this.connections.get(transactionXid);
			if (connection == null) {
				LocalXAConnection xacon = this.getXAConnection();
				connection = xacon.getConnection();
				descriptor = xacon.getXAResource();
				localRes = (LocalXAResource) descriptor.getDelegate();
				this.connections.put(transactionXid, connection);
			}

			if (descriptor != null) {
				transaction.enlistResource(descriptor);
				connection.setCloseImmediately(localRes.hasParticipatedTx() == false);
			}

			return connection;
		} catch (SystemException ex) {
			throw new SQLException(ex);
		} catch (RollbackException ex) {
			throw new SQLException(ex);
		} catch (RuntimeException ex) {
			throw new SQLException(ex);
		}

	}

	public Connection getConnection(String username, String password) throws SQLException {
		TransactionXid transactionXid = null;
		try {
			Transaction transaction = (Transaction) this.transactionManager.getTransaction();
			if (transaction == null) {
				return this.dataSource.getConnection(username, password);
			}

			transaction.registerTransactionListener(this);

			transactionXid = transaction.getTransactionContext().getXid();

			LocalXAResourceDescriptor descriptor = null;
			LocalXAResource localRes = null;
			LogicalConnection connection = this.connections.get(transactionXid);
			if (connection == null) {
				LocalXAConnection xacon = this.getXAConnection(username, password);
				connection = xacon.getConnection();
				descriptor = xacon.getXAResource();
				localRes = (LocalXAResource) descriptor.getDelegate();
				this.connections.put(transactionXid, connection);
			}

			if (descriptor != null) {
				transaction.enlistResource(descriptor);
				connection.setCloseImmediately(localRes.hasParticipatedTx() == false);
			}

			return connection;
		} catch (SystemException ex) {
			throw new SQLException(ex);
		} catch (RollbackException ex) {
			throw new SQLException(ex);
		} catch (RuntimeException ex) {
			throw new SQLException(ex);
		}

	}

	public void onCommitStart(TransactionXid xid) {
		this.connections.remove(xid);
	}

	public void onRollbackStart(TransactionXid xid) {
		this.connections.remove(xid);
	}

	public boolean isWrapperFor(Class<?> iface) {
		if (iface == null) {
			return false;
		} else if (iface.isInstance(this)) {
			return true;
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	public <T> T unwrap(Class<T> iface) {
		if (iface == null) {
			return null;
		} else if (iface.isInstance(this)) {
			return (T) this;
		}
		return null;
	}

	public LocalXAConnection getXAConnection() throws SQLException {
		Connection connection = this.dataSource.getConnection();
		LocalXAConnection xacon = new LocalXAConnection(connection);
		xacon.setResourceId(this.beanName);
		return xacon;
	}

	public LocalXAConnection getXAConnection(String user, String passwd) throws SQLException {
		Connection connection = this.dataSource.getConnection(user, passwd);
		LocalXAConnection xacon = new LocalXAConnection(connection);
		xacon.setResourceId(this.beanName);
		return xacon;
	}

	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setBeanName(String name) {
		this.beanName = name;
	}

	public PrintWriter getLogWriter() {
		return logWriter;
	}

	public void setLogWriter(PrintWriter logWriter) {
		this.logWriter = logWriter;
	}

	public int getLoginTimeout() {
		return loginTimeout;
	}

	public void setLoginTimeout(int loginTimeout) {
		this.loginTimeout = loginTimeout;
	}

	public DataSource getDataSource() {
		return dataSource;
	}

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	public void setTransactionManager(TransactionManager transactionManager) {
		this.transactionManager = transactionManager;
	}

}
