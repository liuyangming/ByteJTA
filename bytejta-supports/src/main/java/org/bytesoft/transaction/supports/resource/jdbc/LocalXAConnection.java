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
package org.bytesoft.transaction.supports.resource.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.ConnectionEventListener;
import javax.sql.StatementEventListener;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;

import org.bytesoft.bytejta.supports.resource.LocalXAResourceDescriptor;

public class LocalXAConnection implements XAConnection /* , ConnectionEventListener */ {
	private String identifier;
	private Connection connection;
	private XAResource xaResource;

	// public void connectionClosed(ConnectionEvent event) {}
	// public void connectionErrorOccurred(ConnectionEvent event) {}

	public Connection getConnection() throws SQLException {
		ConnectionImpl result = new ConnectionImpl();
		result.setDelegate(this.connection);
		return result;
	}

	public void close() throws SQLException {
		this.connection.close();
	}

	public void addConnectionEventListener(ConnectionEventListener listener) {
	}

	public void removeConnectionEventListener(ConnectionEventListener listener) {
	}

	public void addStatementEventListener(StatementEventListener listener) {
	}

	public void removeStatementEventListener(StatementEventListener listener) {
	}

	public XAResource getXAResource() throws SQLException {
		if (this.xaResource == null) {
			this.initXAResourceIfNecessary();
		}

		LocalXAResourceDescriptor descriptor = new LocalXAResourceDescriptor();
		descriptor.setIdentifier(this.identifier);
		descriptor.setDelegate(this.xaResource);

		return descriptor;
	}

	private synchronized void initXAResourceIfNecessary() {
		if (this.xaResource == null) {
			LocalXAResource xares = new LocalXAResource();
			xares.setPhysicalConnection(this.connection);
			this.xaResource = xares;
		}
	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}

}
