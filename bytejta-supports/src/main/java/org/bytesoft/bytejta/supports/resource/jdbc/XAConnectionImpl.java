/**
 * Copyright 2014-2017 yangming.liu<bytefox@126.com>.
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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.StatementEvent;
import javax.sql.StatementEventListener;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;

import org.bytesoft.bytejta.supports.resource.CommonResourceDescriptor;
import org.bytesoft.transaction.supports.resource.XAResourceDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XAConnectionImpl implements XAConnection, ConnectionEventListener, StatementEventListener {
	static final Logger logger = LoggerFactory.getLogger(XAConnectionImpl.class);

	private final Set<ConnectionEventListener> connectionEventListeners = new HashSet<ConnectionEventListener>();
	private final Set<StatementEventListener> statementEventListeners = new HashSet<StatementEventListener>();

	private String identifier;
	private XAConnection delegate;
	private boolean closed;
	private XAResource xaResource;

	public void statementClosed(StatementEvent event) {
		Iterator<StatementEventListener> itr = this.statementEventListeners.iterator();
		while (itr.hasNext()) {
			StatementEventListener listener = itr.next();
			SQLException sqlException = event.getSQLException();
			PreparedStatement statement = event.getStatement();
			StatementEvent statementEvent = new StatementEvent(this, statement, sqlException);
			try {
				listener.statementClosed(statementEvent);
			} catch (RuntimeException error) {
				logger.warn("Error occurred!", error);
			}
		} // end-while (itr.hasNext())
	}

	public void statementErrorOccurred(StatementEvent event) {
		Iterator<StatementEventListener> itr = this.statementEventListeners.iterator();
		while (itr.hasNext()) {
			StatementEventListener listener = itr.next();
			SQLException sqlException = event.getSQLException();
			PreparedStatement statement = event.getStatement();
			StatementEvent statementEvent = new StatementEvent(this, statement, sqlException);
			try {
				listener.statementErrorOccurred(statementEvent);
			} catch (RuntimeException error) {
				logger.warn("Error occurred!", error);
			}
		} // end-while (itr.hasNext())
	}

	public void connectionClosed(ConnectionEvent event) {
		Iterator<ConnectionEventListener> itr = this.connectionEventListeners.iterator();
		while (itr.hasNext()) {
			ConnectionEventListener listener = itr.next();
			SQLException sqlException = event.getSQLException();
			ConnectionEvent connectionEvent = new ConnectionEvent(this, sqlException);
			try {
				listener.connectionClosed(connectionEvent);
			} catch (RuntimeException error) {
				logger.warn("Error occurred!", error);
			}
		} // end-while (itr.hasNext())
	}

	public void connectionErrorOccurred(ConnectionEvent event) {
		Iterator<ConnectionEventListener> itr = this.connectionEventListeners.iterator();
		while (itr.hasNext()) {
			ConnectionEventListener listener = itr.next();
			SQLException sqlException = event.getSQLException();
			ConnectionEvent connectionEvent = new ConnectionEvent(this, sqlException);
			try {
				listener.connectionErrorOccurred(connectionEvent);
			} catch (RuntimeException error) {
				logger.warn("Error occurred!", error);
			}
		} // end-while (itr.hasNext())
	}

	public Connection getConnection() throws SQLException {
		Connection delegateConnection = this.delegate.getConnection();

		ConnectionImpl connection = new ConnectionImpl();
		connection.setManagedConnection(this);
		connection.setDelegate(delegateConnection);

		return connection;
	}

	public void addConnectionEventListener(ConnectionEventListener listener) {
		this.connectionEventListeners.add(listener);
	}

	public void removeConnectionEventListener(ConnectionEventListener listener) {
		this.connectionEventListeners.remove(listener);
	}

	public void addStatementEventListener(StatementEventListener listener) {
		this.statementEventListeners.add(listener);
	}

	public void removeStatementEventListener(StatementEventListener listener) {
		this.statementEventListeners.remove(listener);
	}

	public XAResource getXAResource() throws SQLException {
		if (this.xaResource == null) {
			this.initXAResourceIfNecessary();
		} // end-if (this.xaResource == null)

		return this.xaResource;
	}

	private synchronized void initXAResourceIfNecessary() throws SQLException {
		if (this.xaResource == null) {
			XAResource delegateResource = this.delegate.getXAResource();
			if (XAResourceDescriptor.class.isInstance(delegateResource)) {
				this.xaResource = delegateResource;
			} else {
				CommonResourceDescriptor descriptor = new CommonResourceDescriptor();
				descriptor.setDelegate(delegateResource);
				descriptor.setIdentifier(this.identifier);
				this.xaResource = descriptor;
			}
		} // end-if (this.xaResource == null)
	}

	public void close() throws SQLException {
		if (this.closed == false) {
			this.delegate.close();
			this.closed = true;
		} // end-if (this.closed == false)
	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

	public XAConnection getDelegate() {
		return delegate;
	}

	public void setDelegate(XAConnection delegate) {
		this.delegate = delegate;
	}

}
