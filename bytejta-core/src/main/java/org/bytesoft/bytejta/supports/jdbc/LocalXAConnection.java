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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.StatementEventListener;
import javax.sql.XAConnection;

import org.bytesoft.bytejta.supports.resource.LocalXAResourceDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalXAConnection implements XAConnection {
	static final Logger logger = LoggerFactory.getLogger(LocalXAConnection.class);

	private String resourceId;

	private final Connection connection;
	private final LocalXAResource xaResource = new LocalXAResource(this);
	private boolean underlyingConCloseRequired = false;
	private boolean physicalConnectionReleased = false;
	private int physicalConnectionSharingCount = 0;

	private final Set<ConnectionEventListener> listeners = new HashSet<ConnectionEventListener>();

	public LocalXAConnection(Connection connection) {
		this.connection = connection;
	}

	protected Connection getPhysicalConnection() {
		return this.connection;
	}

	public LogicalConnection getConnection() throws SQLException {
		if (this.physicalConnectionReleased) {
			throw new SQLException("LocalXAConnection has already been closed!");
		}

		LogicalConnection logicalConnection = new LogicalConnection(this, this.connection);

		this.physicalConnectionSharingCount++;
		this.underlyingConCloseRequired = false;

		return logicalConnection;
	}

	public void closeLogicalConnection() throws SQLException {
		this.physicalConnectionSharingCount--;

		if (this.physicalConnectionSharingCount == 0) {
			this.underlyingConCloseRequired = true;
		}

	}

	private void releaseConnection() {
		if (this.physicalConnectionReleased == false) {
			try {
				this.connection.close();
				this.fireConnectionClosed();
			} catch (SQLException ex) {
				logger.debug("Error occurred while closing connection!", ex);

				this.fireConnectionErrorOccurred();
			} catch (RuntimeException ex) {
				logger.debug("Error occurred while closing connection!", ex);

				this.fireConnectionErrorOccurred();
			} finally {
				this.physicalConnectionReleased = true;
			}
		}
	}

	public void commitLocalTransaction() throws SQLException {
		try {
			this.connection.commit();
		} catch (SQLException ex) {
			throw ex;
		} catch (RuntimeException ex) {
			throw new SQLException(ex);
		}
	}

	public void rollbackLocalTransaction() throws SQLException {
		try {
			this.connection.rollback();
		} catch (SQLException ex) {
			throw ex;
		} catch (RuntimeException ex) {
			throw new SQLException(ex);
		}
	}

	public void closeQuietly() {
		try {
			this.close();
		} catch (Exception ex) {
			logger.warn("Error occurred while closing physical connection.", ex);
		}
	}

	public void close() throws SQLException {
		if (this.underlyingConCloseRequired == false) {
			logger.warn("Illegal state: there is at least one connection that is not closed!");
		}

		this.releaseConnection();
	}

	private void fireConnectionClosed() {
		Iterator<ConnectionEventListener> itr = this.listeners.iterator();
		while (itr.hasNext()) {
			ConnectionEventListener listener = itr.next();
			try {
				listener.connectionClosed(new ConnectionEvent(this));
			} catch (Exception ex) {
				logger.debug(ex.getMessage(), ex);
			}
		}
	}

	private void fireConnectionErrorOccurred() {
		Iterator<ConnectionEventListener> itr = this.listeners.iterator();
		while (itr.hasNext()) {
			ConnectionEventListener listener = itr.next();
			try {
				listener.connectionErrorOccurred(new ConnectionEvent(this));
			} catch (Exception ex) {
				logger.debug(ex.getMessage(), ex);
			}
		}
	}

	public void addConnectionEventListener(ConnectionEventListener paramConnectionEventListener) {
		this.listeners.add(paramConnectionEventListener);
	}

	public void removeConnectionEventListener(ConnectionEventListener paramConnectionEventListener) {
		this.listeners.remove(paramConnectionEventListener);
	}

	public void addStatementEventListener(StatementEventListener paramStatementEventListener) {
	}

	public void removeStatementEventListener(StatementEventListener paramStatementEventListener) {
	}

	public LocalXAResourceDescriptor getXAResource(boolean loggingRequired) throws SQLException {
		LocalXAResourceDescriptor descriptor = new LocalXAResourceDescriptor();
		descriptor.setIdentifier(this.resourceId);
		descriptor.setDelegate(this.xaResource);
		descriptor.setLoggingRequired(loggingRequired);
		return descriptor;
	}

	public LocalXAResourceDescriptor getXAResource() throws SQLException {
		return this.getXAResource(true);
	}

	public String getResourceId() {
		return resourceId;
	}

	public void setResourceId(String resourceId) {
		this.resourceId = resourceId;
	}

}
