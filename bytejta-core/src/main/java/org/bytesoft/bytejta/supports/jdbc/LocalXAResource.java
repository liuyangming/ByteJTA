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
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.bytesoft.common.utils.ByteUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalXAResource implements XAResource {
	static final Logger logger = LoggerFactory.getLogger(LocalXAResource.class);

	private LocalXAConnection managedConnection;
	private Xid currentXid;
	private Xid suspendXid;
	private boolean suspendAutoCommit;
	private boolean originalAutoCommit;

	public LocalXAResource() {
	}

	public LocalXAResource(LocalXAConnection managedConnection) {
		this.managedConnection = managedConnection;
	}

	public void recoverable(Xid xid) throws XAException {
		String gxid = ByteUtils.byteArrayToString(xid.getGlobalTransactionId());
		String bxid = null;
		if (xid.getBranchQualifier() == null || xid.getBranchQualifier().length == 0) {
			bxid = gxid;
		} else {
			bxid = ByteUtils.byteArrayToString(xid.getBranchQualifier());
		}

		Connection connection = this.managedConnection.getPhysicalConnection();

		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = connection.prepareStatement("select gxid, bxid from bytejta where gxid = ? and bxid = ?");
			stmt.setString(1, gxid);
			stmt.setString(2, bxid);
			rs = stmt.executeQuery();
			if (rs.next() == false) {
				throw new XAException(XAException.XAER_NOTA);
			}
		} catch (Exception ex) {
			logger.warn("Error occurred while recovering local-xa-resource.", ex);
			throw new XAException(XAException.XAER_RMERR);
		} finally {
			this.closeQuietly(rs);
			this.closeQuietly(stmt);
		}
	}

	public synchronized void start(Xid xid, int flags) throws XAException {
		if (xid == null) {
			throw new XAException();
		} else if (flags == XAResource.TMRESUME && this.suspendXid != null) {
			if (this.suspendXid.equals(xid)) {
				this.suspendXid = null;
				this.currentXid = xid;
				this.originalAutoCommit = this.suspendAutoCommit;
				this.suspendAutoCommit = true;
				return;
			} else {
				throw new XAException();
			}
		} else if (flags == XAResource.TMJOIN) {
			if (this.currentXid == null) {
				throw new XAException();
			}
		} else if (flags != XAResource.TMNOFLAGS) {
			throw new XAException();
		} else if (this.currentXid != null) {
			throw new XAException();
		} else {
			Connection connection = this.managedConnection.getPhysicalConnection();

			try {
				originalAutoCommit = connection.getAutoCommit();
			} catch (Exception ignored) {
				originalAutoCommit = true;
			}

			try {
				connection.setAutoCommit(false);
			} catch (Exception ex) {
				XAException xae = new XAException();
				xae.initCause(ex);
				throw xae;
			}

			this.currentXid = xid;
		}
	}

	public synchronized void end(Xid xid, int flags) throws XAException {
		if (xid == null) {
			throw new XAException();
		} else if (this.currentXid == null) {
			throw new XAException();
		} else if (!this.currentXid.equals(xid)) {
			throw new XAException();
		} else if (flags == XAResource.TMSUSPEND) {
			this.suspendXid = xid;
			this.suspendAutoCommit = this.originalAutoCommit;
			this.currentXid = null;
			this.originalAutoCommit = true;
		} else if (flags == XAResource.TMSUCCESS) {
			String gxid = ByteUtils.byteArrayToString(xid.getGlobalTransactionId());
			String bxid = null;
			if (xid.getBranchQualifier() == null || xid.getBranchQualifier().length == 0) {
				bxid = gxid;
			} else {
				bxid = ByteUtils.byteArrayToString(xid.getBranchQualifier());
			}

			Connection connection = this.managedConnection.getPhysicalConnection();

			PreparedStatement stmt = null;
			try {
				stmt = connection.prepareStatement("insert into bytejta(gxid, bxid, ctime) values(?, ?, ?)");
				stmt.setString(1, gxid);
				stmt.setString(2, bxid);
				stmt.setLong(3, System.currentTimeMillis());
				int value = stmt.executeUpdate();
				if (value == 0) {
					throw new IllegalStateException(
							"The operation failed and the data was not written to the database!");
				}
			} catch (SQLException ex) {
				boolean tableExists = false;
				try {
					tableExists = this.isTableExists(connection);
				} catch (Exception sqlEx) {
					logger.error("Error occurred while ending local-xa-resource: {}", ex.getMessage());
					throw new XAException(XAException.XAER_RMFAIL);
				}

				if (tableExists) {
					logger.error("Error occurred while ending local-xa-resource: {}", ex.getMessage());
					throw new XAException(XAException.XAER_RMERR);
				} else {
					logger.debug("Error occurred while ending local-xa-resource: {}", ex.getMessage());
				}
			} catch (RuntimeException rex) {
				logger.error("Error occurred while ending local-xa-resource: {}", rex.getMessage());
				throw new XAException(XAException.XAER_RMERR);
			} finally {
				this.closeQuietly(stmt);
			}
		} else if (flags == XAResource.TMFAIL) {
			logger.debug("Error occurred while ending local-xa-resource.");
		} else {
			throw new XAException();
		}
	}

	public synchronized int prepare(Xid xid) {
		Connection connection = this.managedConnection.getPhysicalConnection();
		try {
			if (connection.isReadOnly()) {
				connection.setAutoCommit(originalAutoCommit);
				return XAResource.XA_RDONLY;
			}
		} catch (Exception ex) {
			logger.debug("Error occurred while preparing local-xa-resource: {}", ex.getMessage());
		}
		return XAResource.XA_OK;
	}

	public synchronized void commit(Xid xid, boolean onePhase) throws XAException {
		try {
			if (xid == null) {
				throw new XAException();
			} else if (this.currentXid == null) {
				throw new XAException();
			} else if (!this.currentXid.equals(xid)) {
				throw new XAException();
			}

			this.managedConnection.commitLocalTransaction();
		} catch (XAException xae) {
			throw xae;
		} catch (Exception ex) {
			XAException xae = new XAException();
			xae.initCause(ex);
			throw xae;
		} finally {
			this.releasePhysicalConnection();
		}
	}

	public synchronized void rollback(Xid xid) throws XAException {
		try {
			if (xid == null) {
				throw new XAException();
			} else if (this.currentXid == null) {
				throw new XAException();
			} else if (!this.currentXid.equals(xid)) {
				throw new XAException();
			}

			this.managedConnection.rollbackLocalTransaction();
		} catch (XAException xae) {
			throw xae;
		} catch (Exception ex) {
			XAException xae = new XAException();
			xae.initCause(ex);
			throw xae;
		} finally {
			this.releasePhysicalConnection();
		}
	}

	private void releasePhysicalConnection() {
		Connection connection = this.managedConnection.getPhysicalConnection();
		try {
			connection.setAutoCommit(originalAutoCommit);
		} catch (Exception ex) {
			logger.warn("Error occurred while configuring attr 'autoCommit' of physical connection.", ex);
		} finally {
			// LocalXAConnection is only used for wrapping,
			// once the transaction completed it can be closed immediately.
			this.managedConnection.closeQuietly();
			this.forgetQuietly(this.currentXid);
		}
	}

	public boolean isSameRM(XAResource xares) {
		return this == xares;
	}

	public void forgetQuietly(Xid xid) {
		try {
			this.forget(xid);
		} catch (Exception ex) {
			logger.warn("Error occurred while forgeting local-xa-resource.", xid);
		}
	}

	public synchronized void forget(Xid xid) throws XAException {
		if (xid == null || this.currentXid == null) {
			logger.warn("Error occurred while forgeting local-xa-resource: invalid xid.");
		} else {
			this.currentXid = null;
			this.originalAutoCommit = true;
			this.managedConnection = null;
		}
	}

	public Xid[] recover(int flags) throws XAException {
		return new Xid[0];
	}

	protected boolean isTableExists(Connection conn) throws SQLException {

		String catalog = null;
		try {
			catalog = conn.getCatalog();
		} catch (Throwable throwable) {
			logger.debug("Error occurred while getting catalog of java.sql.Connection!");
		}
		String schema = null;
		try {
			schema = conn.getSchema();
		} catch (Throwable throwable) {
			logger.debug("Error occurred while getting schema of java.sql.Connection!");
		}

		ResultSet rs = null;
		try {
			DatabaseMetaData metadata = conn.getMetaData();
			rs = metadata.getTables(catalog, schema, "bytejta", null);
			return rs.next();
		} finally {
			this.closeQuietly(rs);
		}
	}

	protected void closeQuietly(ResultSet closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (Exception ex) {
				logger.debug("Error occurred while closing resource {}.", closeable);
			}
		}
	}

	protected void closeQuietly(Statement closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (Exception ex) {
				logger.debug("Error occurred while closing resource {}.", closeable);
			}
		}
	}

	protected void closeQuietly(Connection closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (Exception ex) {
				logger.debug("Error occurred while closing resource {}.", closeable);
			}
		}
	}

	// public boolean hasParticipatedTx() {
	// return currentXid != null;
	// }

	public int getTransactionTimeout() {
		return 0;
	}

	public boolean setTransactionTimeout(int transactionTimeout) {
		return false;
	}

	// public void setLocalTransaction(Connection localTransaction) {
	// this.localTransaction = localTransaction;
	// }

	public LocalXAConnection getManagedConnection() {
		return managedConnection;
	}

}
