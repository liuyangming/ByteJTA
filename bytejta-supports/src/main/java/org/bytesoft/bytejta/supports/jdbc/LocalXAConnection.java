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

import javax.sql.ConnectionEventListener;
import javax.sql.StatementEventListener;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;

import org.bytesoft.bytejta.supports.resource.LocalXAResourceDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalXAConnection implements XAConnection {
	static final Logger logger = LoggerFactory.getLogger(LocalXAConnection.class);

	private String resourceId;

	private final Connection connection;
	private final LocalXAResource xaResource = new LocalXAResource();
	private boolean initialized = false;
	private boolean logicalConnectionReleased = false;
	private int pooledConnectionSharingCount = 0;

	private transient LocalXAResourceDescriptor descriptor;

	public LocalXAConnection(Connection connection) {
		this.connection = connection;
	}

	public Connection getConnection() throws SQLException {
		LogicalConnection logicalConnection = new LogicalConnection(this, this.connection);
		if (this.initialized) {
			this.pooledConnectionSharingCount++;
		} else {
			this.xaResource.setLocalTransaction(logicalConnection);
			this.initialized = true;
			this.logicalConnectionReleased = false;
		}
		return logicalConnection;
	}

	public void closeLogicalConnection() throws SQLException {
		if (this.pooledConnectionSharingCount > 0) {
			this.pooledConnectionSharingCount--;
		} else if (this.initialized) {
			if (this.logicalConnectionReleased) {
				throw new SQLException();
			} else {
				this.logicalConnectionReleased = true;
			}
		} else {
			throw new SQLException();
		}
	}

	public void commitLocalTransaction() throws SQLException {
		try {
			this.connection.commit();
		} catch (SQLException ex) {
			throw ex;
		} catch (RuntimeException ex) {
			throw new SQLException(ex);
		} finally {
			try {
				this.close();
			} catch (SQLException ex) {
				logger.debug(ex.getMessage());
			}
		}
	}

	public void rollbackLocalTransaction() throws SQLException {
		try {
			this.connection.rollback();
		} catch (SQLException ex) {
			throw ex;
		} catch (RuntimeException ex) {
			throw new SQLException(ex);
		} finally {
			try {
				this.close();
			} catch (SQLException ex) {
				logger.debug(ex.getMessage());
			}
		}
	}

	public void close() throws SQLException {
		try {
			this.connection.close();
		} finally {
			this.initialized = false;
		}
	}

	public void addConnectionEventListener(ConnectionEventListener paramConnectionEventListener) {
		// TODO this.pooledConnection.addConnectionEventListener(paramConnectionEventListener);
	}

	public void removeConnectionEventListener(ConnectionEventListener paramConnectionEventListener) {
		// TODO this.pooledConnection.removeConnectionEventListener(paramConnectionEventListener);
	}

	public void addStatementEventListener(StatementEventListener paramStatementEventListener) {
		// TODO this.pooledConnection.addStatementEventListener(paramStatementEventListener);
	}

	public void removeStatementEventListener(StatementEventListener paramStatementEventListener) {
		// TODO this.pooledConnection.removeStatementEventListener(paramStatementEventListener);
	}

	public XAResource getXAResource() throws SQLException {
		if (this.descriptor == null) {
			LocalXAResourceDescriptor xares = new LocalXAResourceDescriptor();
			xares.setIdentifier(this.resourceId);
			xares.setDelegate(this.xaResource);
			this.descriptor = xares;
		}
		return this.descriptor;
	}

	public String getResourceId() {
		return resourceId;
	}

	public void setResourceId(String resourceId) {
		this.resourceId = resourceId;
	}

}
