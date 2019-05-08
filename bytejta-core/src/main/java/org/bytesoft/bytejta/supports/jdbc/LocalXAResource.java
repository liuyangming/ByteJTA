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
import org.bytesoft.transaction.xa.XidFactory;
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
		byte[] globalTransactionId = xid.getGlobalTransactionId();
		byte[] branchQualifier = xid.getBranchQualifier();

		String gxid = ByteUtils.byteArrayToString(globalTransactionId);
		String bxid = null;
		if (branchQualifier == null || branchQualifier.length == 0) {
			bxid = gxid;
		} else {
			bxid = ByteUtils.byteArrayToString(branchQualifier);
		}

		String identifier = this.getIdentifier(globalTransactionId, branchQualifier);

		Connection connection = this.managedConnection.getPhysicalConnection();

		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			StringBuilder sql = new StringBuilder();
			sql.append("select xid, gxid, bxid from bytejta where xid = ? gxid = ? and bxid = ? ");
			stmt = connection.prepareStatement(sql.toString());
			stmt.setString(1, identifier);
			stmt.setString(2, gxid);
			stmt.setString(3, bxid);
			rs = stmt.executeQuery();
			if (rs.next() == false) {
				throw new XAException(XAException.XAER_NOTA);
			}
		} catch (SQLException ex) {
			try {
				this.isTableExists(connection);
			} catch (SQLException sqlEx) {
				logger.warn("Error occurred while recovering local-xa-resource.", ex);
				throw new XAException(XAException.XAER_RMFAIL);
			} catch (RuntimeException rex) {
				logger.warn("Error occurred while recovering local-xa-resource.", ex);
				throw new XAException(XAException.XAER_RMFAIL);
			}

			throw new XAException(XAException.XAER_RMERR);
		} catch (RuntimeException ex) {
			logger.warn("Error occurred while recovering local-xa-resource.", ex);
			throw new XAException(XAException.XAER_RMERR);
		} finally {
			this.closeQuietly(rs);
			this.closeQuietly(stmt);
		}
	}

	public synchronized void start(Xid xid, int flags) throws XAException {
		if (xid == null) {
			throw new XAException(XAException.XAER_INVAL);
		} else if (flags == XAResource.TMRESUME && this.suspendXid != null) {
			if (this.suspendXid.equals(xid)) {
				this.suspendXid = null;
				this.currentXid = xid;
				this.originalAutoCommit = this.suspendAutoCommit;
				this.suspendAutoCommit = true;
				return;
			} else {
				throw new XAException(XAException.XAER_PROTO);
			}
		} else if (flags == XAResource.TMJOIN) {
			if (this.currentXid == null) {
				throw new XAException(XAException.XAER_PROTO);
			}
		} else if (flags != XAResource.TMNOFLAGS) {
			throw new XAException(XAException.XAER_PROTO);
		} else if (this.currentXid != null) {
			throw new XAException(XAException.XAER_PROTO);
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
				XAException xae = new XAException(XAException.XAER_RMERR);
				xae.initCause(ex);
				throw xae;
			}

			this.currentXid = xid;
		}
	}

	public synchronized void end(Xid xid, int flags) throws XAException {
		if (xid == null) {
			throw new XAException(XAException.XAER_INVAL);
		} else if (this.currentXid == null) {
			throw new XAException(XAException.XAER_PROTO);
		} else if (!this.currentXid.equals(xid)) {
			throw new XAException(XAException.XAER_PROTO);
		} else if (flags == XAResource.TMSUSPEND) {
			this.suspendXid = xid;
			this.suspendAutoCommit = this.originalAutoCommit;
			this.currentXid = null;
			this.originalAutoCommit = true;
		} else if (flags == XAResource.TMSUCCESS) {
			// delay the logging operation to the commit phase.
			// this.createTransactionLogIfNecessary(xid);
		} else if (flags == XAResource.TMFAIL) {
			logger.debug("Error occurred while ending local-xa-resource.");
		} else {
			throw new XAException(XAException.XAER_PROTO);
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

	public synchronized void commit(Xid xid, boolean loggingRequired) throws XAException {
		try {
			if (xid == null) {
				throw new XAException(XAException.XAER_INVAL);
			} else if (this.currentXid == null) {
				throw new XAException(XAException.XAER_PROTO);
			} else if (!this.currentXid.equals(xid)) {
				throw new XAException(XAException.XAER_PROTO);
			}

			if (loggingRequired) {
				this.createTransactionLogIfNecessary(xid);
			} // end-if (loggingRequired)

			this.managedConnection.commitLocalTransaction();
		} catch (XAException xae) {
			throw xae;
		} catch (Exception ex) {
			XAException xae = new XAException(XAException.XAER_RMERR);
			xae.initCause(ex);
			throw xae;
		} finally {
			this.releasePhysicalConnection();
		}
	}

	public synchronized void rollback(Xid xid) throws XAException {
		try {
			if (xid == null) {
				throw new XAException(XAException.XAER_INVAL);
			} else if (this.currentXid == null) {
				throw new XAException(XAException.XAER_PROTO);
			} else if (!this.currentXid.equals(xid)) {
				throw new XAException(XAException.XAER_PROTO);
			}

			this.managedConnection.rollbackLocalTransaction();
		} catch (XAException xae) {
			throw xae;
		} catch (Exception ex) {
			XAException xae = new XAException(XAException.XAER_RMERR);
			xae.initCause(ex);
			throw xae;
		} finally {
			this.releasePhysicalConnection();
		}
	}

	private void createTransactionLogIfNecessary(Xid xid) throws XAException {
		byte[] globalTransactionId = xid.getGlobalTransactionId();
		byte[] branchQualifier = xid.getBranchQualifier();

		String gxid = ByteUtils.byteArrayToString(globalTransactionId);
		String bxid = null;
		if (branchQualifier == null || branchQualifier.length == 0) {
			bxid = gxid;
		} else {
			bxid = ByteUtils.byteArrayToString(branchQualifier);
		}

		String identifier = this.getIdentifier(globalTransactionId, branchQualifier);

		Connection connection = this.managedConnection.getPhysicalConnection();

		PreparedStatement stmt = null;
		try {
			stmt = connection.prepareStatement("insert into bytejta(xid, gxid, bxid, ctime) values(?, ?, ?, ?)");
			stmt.setString(1, identifier);
			stmt.setString(2, gxid);
			stmt.setString(3, bxid);
			stmt.setLong(4, System.currentTimeMillis());
			int value = stmt.executeUpdate();
			if (value == 0) {
				throw new IllegalStateException("The operation failed and the data was not written to the database!");
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
		if (this == xares) {
			return true;
		} else if (LocalXAResource.class.isInstance(xares) == false) {
			return false;
		}

		LocalXAResource that = (LocalXAResource) xares;
		Connection thisConn = this.managedConnection.getPhysicalConnection();
		Connection thatConn = that.managedConnection.getPhysicalConnection();
		return thisConn == thatConn;
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

	protected String getIdentifier(byte[] globalByteArray, byte[] branchByteArray) {
		if (branchByteArray == null || branchByteArray.length != XidFactory.BRANCH_QUALIFIER_LENGTH) {
			logger.warn("Invalid branchByteArray: the length of branchQulifier not equals to 16!");
			return ByteUtils.byteArrayToString(globalByteArray);
		}

		byte[] gvalueByteArray = new byte[4];
		System.arraycopy(globalByteArray, 6, gvalueByteArray, 0, 4);
		int global = ByteUtils.byteArrayToInt(gvalueByteArray);

		int gday = (global << 8) >>> 27;
		int ghour = (global << 13) >>> 27;
		int gminute = (global << 18) >>> 26;
		int gsecond = (global << 24) >>> 26;
		int gmillis = ((global << 30) >>> 22) | (globalByteArray[11] - Byte.MIN_VALUE);

		int datime = 0;
		datime = datime | (gday << 27);
		datime = datime | (ghour << 22);
		datime = datime | (gminute << 16);
		datime = datime | (gsecond << 10);
		datime = datime | gmillis;
		byte[] datimeByteArray = ByteUtils.intToByteArray(datime);

		byte[] bvalueByteArray = new byte[4];
		System.arraycopy(branchByteArray, 6, bvalueByteArray, 0, 4);
		int branch = ByteUtils.byteArrayToInt(bvalueByteArray);

		int bmonth = (branch << 4) >> 28;
		int bminute = (branch << 18) >>> 26;
		int bsecond = (branch << 24) >>> 26;
		int bmillis = ((branch << 30) >>> 22) | (branchByteArray[11] - Byte.MIN_VALUE);

		byte[] randomByteArray = new byte[4];
		System.arraycopy(branchByteArray, 12, randomByteArray, 0, 4);
		int brandom = ByteUtils.byteArrayToInt(randomByteArray);
		int bprefix = ((brandom >>> 16) << 26) >>> 26;

		int millis = 0;
		millis = millis | (bmonth << 28);
		millis = millis | (bminute << 22);
		millis = millis | (bsecond << 16);
		millis = millis | (bmillis << 6);
		millis = millis | bprefix;
		byte[] millisByteArray = ByteUtils.intToByteArray(millis);

		byte[] resultByteArray = new byte[16];
		System.arraycopy(globalByteArray, 0, resultByteArray, 0, 6);
		System.arraycopy(datimeByteArray, 0, resultByteArray, 6, 4);
		System.arraycopy(millisByteArray, 0, resultByteArray, 10, 4);
		System.arraycopy(randomByteArray, 2, resultByteArray, 14, 2);
		return ByteUtils.byteArrayToString(resultByteArray);
	}

	public int getTransactionTimeout() {
		return 0;
	}

	public boolean setTransactionTimeout(int transactionTimeout) {
		return false;
	}

	public LocalXAConnection getManagedConnection() {
		return managedConnection;
	}

}
