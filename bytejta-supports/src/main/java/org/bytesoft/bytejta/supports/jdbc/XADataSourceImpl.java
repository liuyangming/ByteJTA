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

import java.io.PrintWriter;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import javax.sql.XAConnection;
import javax.sql.XADataSource;

import org.springframework.beans.factory.BeanNameAware;

public class XADataSourceImpl implements XADataSource, BeanNameAware {
	private String identifier;
	private XADataSource xaDataSource;

	public PrintWriter getLogWriter() throws SQLException {
		return this.xaDataSource.getLogWriter();
	}

	public void setLogWriter(PrintWriter out) throws SQLException {
		this.xaDataSource.setLogWriter(out);
	}

	public void setLoginTimeout(int seconds) throws SQLException {
		this.xaDataSource.setLoginTimeout(seconds);
	}

	public int getLoginTimeout() throws SQLException {
		return this.xaDataSource.getLoginTimeout();
	}

	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return this.xaDataSource.getParentLogger();
	}

	public XAConnection getXAConnection() throws SQLException {
		XAConnectionImpl managed = new XAConnectionImpl();
		managed.setIdentifier(this.identifier);
		XAConnection delegate = this.xaDataSource.getXAConnection();
		managed.setDelegate(delegate);
		delegate.addConnectionEventListener(managed);
		return managed;
	}

	public XAConnection getXAConnection(String user, String password) throws SQLException {
		XAConnectionImpl managed = new XAConnectionImpl();
		managed.setIdentifier(this.identifier);
		XAConnection delegate = this.xaDataSource.getXAConnection(user, password);
		managed.setDelegate(delegate);
		delegate.addConnectionEventListener(managed);
		return managed;
	}

	public void setBeanName(String name) {
		this.setIdentifier(name);
	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

	public XADataSource getXaDataSource() {
		return xaDataSource;
	}

	public void setXaDataSource(XADataSource xaDataSource) {
		this.xaDataSource = xaDataSource;
	}

}
