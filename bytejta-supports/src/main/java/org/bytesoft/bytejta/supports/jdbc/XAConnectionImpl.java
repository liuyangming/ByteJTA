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
package org.bytesoft.bytejta.supports.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.StatementEventListener;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;

import org.bytesoft.bytejta.supports.resource.CommonResourceDescriptor;
import org.bytesoft.transaction.supports.resource.XAResourceDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XAConnectionImpl implements XAConnection, ConnectionEventListener {
	static final Logger logger = LoggerFactory.getLogger(XAConnectionImpl.class);

	private final Set<ConnectionEventListener> listeners = new HashSet<ConnectionEventListener>();

	private String identifier;
	private XAConnection delegate;
	private boolean closed;

	public void connectionClosed(ConnectionEvent event) {
		for (Iterator<ConnectionEventListener> itr = this.listeners.iterator(); itr.hasNext();) {
			ConnectionEventListener listener = itr.next();
			try {
				listener.connectionClosed(new ConnectionEvent(this, event.getSQLException()));
			} catch (RuntimeException rex) {
				logger.warn(rex.getMessage(), rex);
			}
		} // end-for (Iterator<ConnectionEventListener> itr = this.listeners.iterator(); itr.hasNext();)

		this.firePhysicalConnectionClosed(event); // removeConnectionEventListener
	}

	public void connectionErrorOccurred(ConnectionEvent event) {
		for (Iterator<ConnectionEventListener> itr = this.listeners.iterator(); itr.hasNext();) {
			ConnectionEventListener listener = itr.next();
			try {
				listener.connectionErrorOccurred(new ConnectionEvent(this, event.getSQLException()));
			} catch (RuntimeException rex) {
				logger.warn(rex.getMessage(), rex);
			}
		} // end-for (Iterator<ConnectionEventListener> itr = this.listeners.iterator(); itr.hasNext();)

		this.firePhysicalConnectionClosed(event); // removeConnectionEventListener
	}

	private void firePhysicalConnectionClosed(ConnectionEvent event) {
		try {
			this.delegate.removeConnectionEventListener(this);

			this.close(); // close physical connection
		} catch (SQLException ex) {
			logger.warn("Failed to close XAConnection", ex);
		} catch (RuntimeException ex) {
			logger.warn("Failed to close XAConnection", ex);
		}
	}

	public Connection getConnection() throws SQLException {
		return this.delegate.getConnection();
	}

	public void addConnectionEventListener(ConnectionEventListener listener) {
		this.listeners.add(listener);
	}

	public void removeConnectionEventListener(ConnectionEventListener listener) {
		this.listeners.remove(listener);
	}

	public void addStatementEventListener(StatementEventListener listener) {
		this.delegate.addStatementEventListener(listener);
	}

	public void removeStatementEventListener(StatementEventListener listener) {
		this.delegate.removeStatementEventListener(listener);
	}

	public XAResource getXAResource() throws SQLException {
		XAResource xares = this.delegate.getXAResource();
		if (XAResourceDescriptor.class.isInstance(xares)) {
			return xares;
		}
		CommonResourceDescriptor descriptor = new CommonResourceDescriptor();
		descriptor.setDelegate(xares);
		descriptor.setIdentifier(this.identifier);
		return descriptor;
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
