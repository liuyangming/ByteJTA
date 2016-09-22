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
package org.bytesoft.bytejta.supports.resource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import javax.sql.DataSource;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.bytesoft.common.utils.ByteUtils;
import org.bytesoft.transaction.supports.resource.XAResourceDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalXAResourceDescriptor implements XAResourceDescriptor {
	static final Logger logger = LoggerFactory.getLogger(LocalXAResourceDescriptor.class);

	private String identifier;
	private XAResource delegate;

	private DataSource dataSource;

	public boolean isTransactionCommitted(Xid xid) throws IllegalStateException {
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			conn = this.dataSource.getConnection();
			stmt = conn.prepareStatement("select xid from bytejta where xid = ?");
			stmt.setString(1, ByteUtils.byteArrayToString(xid.getGlobalTransactionId()));
			rs = stmt.executeQuery();
			return rs.next();
		} catch (Exception ex) {
			logger.warn("Error occurred while recovering local-xa-resource.", ex);
			throw new IllegalStateException();
		} finally {
			this.closeQuietly(rs);
			this.closeQuietly(stmt);
			this.closeQuietly(conn);
		}
	}

	private void closeQuietly(ResultSet closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (Exception ex) {
				logger.debug("Error occurred while closing resource {}.", closeable);
			}
		}
	}

	private void closeQuietly(Statement closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (Exception ex) {
				logger.debug("Error occurred while closing resource {}.", closeable);
			}
		}
	}

	private void closeQuietly(Connection closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (Exception ex) {
				logger.debug("Error occurred while closing resource {}.", closeable);
			}
		}
	}

	public String toString() {
		return String.format("unknown-resource[%s]", this.delegate);
	}

	public void setTransactionTimeoutQuietly(int timeout) {
		try {
			this.delegate.setTransactionTimeout(timeout);
		} catch (Exception ex) {
			return;
		}
	}

	public void commit(Xid arg0, boolean arg1) throws XAException {
		if (this.delegate == null) {
			return;
		}
		delegate.commit(arg0, arg1);
	}

	public void recoveryCommit(Xid arg0, boolean arg1) throws XAException {
		if (this.delegate == null) {
			return;
		}
		delegate.commit(arg0, arg1);
	}

	public void end(Xid arg0, int arg1) throws XAException {
		if (this.delegate == null) {
			return;
		}
		delegate.end(arg0, arg1);
	}

	public void forget(Xid arg0) throws XAException {
		if (this.delegate == null) {
			return;
		}
		delegate.forget(arg0);
	}

	public int getTransactionTimeout() throws XAException {
		if (this.delegate == null) {
			return 0;
		}
		return delegate.getTransactionTimeout();
	}

	public boolean isSameRM(XAResource arg0) throws XAException {
		if (this.delegate == null) {
			return false;
		}
		return delegate.isSameRM(arg0);
	}

	public int prepare(Xid arg0) throws XAException {
		if (this.delegate == null) {
			return XAResource.XA_RDONLY;
		}
		return delegate.prepare(arg0);
	}

	public Xid[] recover(int arg0) throws XAException {
		if (this.delegate == null) {
			return new Xid[0];
		}
		return delegate.recover(arg0);
	}

	public void rollback(Xid arg0) throws XAException {
		if (this.delegate == null) {
			return;
		}
		delegate.rollback(arg0);
	}

	public void recoveryRollback(Xid arg0) throws XAException {
		if (this.delegate == null) {
			return;
		}
		delegate.rollback(arg0);
	}

	public boolean setTransactionTimeout(int arg0) throws XAException {
		if (this.delegate == null) {
			return false;
		}
		return delegate.setTransactionTimeout(arg0);
	}

	public void start(Xid arg0, int arg1) throws XAException {
		if (this.delegate == null) {
			return;
		}
		delegate.start(arg0, arg1);
	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

	public XAResource getDelegate() {
		return delegate;
	}

	public void setDelegate(XAResource delegate) {
		this.delegate = delegate;
	}

	public DataSource getDataSource() {
		return dataSource;
	}

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

}
