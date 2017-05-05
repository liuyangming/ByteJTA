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

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.bytesoft.transaction.supports.resource.XAResourceDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonResourceDescriptor implements XAResourceDescriptor {
	static Logger logger = LoggerFactory.getLogger(CommonResourceDescriptor.class);

	private XAResource delegate;
	private String identifier;

	private transient Xid recoverXid;
	private transient Object managed;
	// private transient boolean recved;

	public boolean isTransactionCommitted(Xid xid) throws IllegalStateException {
		throw new IllegalStateException();
	}

	public String toString() {
		return String.format("common-resource[id= %s]", this.identifier);
	}

	public void setTransactionTimeoutQuietly(int timeout) {
		try {
			this.delegate.setTransactionTimeout(timeout);
		} catch (Exception ex) {
			// ignore
		}
	}

	public void commit(Xid arg0, boolean arg1) throws XAException {
		delegate.commit(arg0, arg1);
	}

	public void end(Xid arg0, int arg1) throws XAException {
		delegate.end(arg0, arg1);
	}

	public void forget(Xid arg0) throws XAException {
		try {
			delegate.forget(arg0);
		} finally {
			this.closeIfNecessary();
		}
	}

	private void closeIfNecessary() {
		if (this.recoverXid != null && this.managed != null) {
			if (javax.sql.XAConnection.class.isInstance(this.managed)) {
				this.closeQuietly((javax.sql.XAConnection) this.managed);
			} else if (javax.jms.XAConnection.class.isInstance(this.managed)) {
				this.closeQuietly((javax.jms.XAConnection) this.managed);
			} else if (javax.resource.spi.ManagedConnection.class.isInstance(this.managed)) {
				this.closeQuietly((javax.resource.spi.ManagedConnection) this.managed);
			}
		}
	}

	private void closeQuietly(javax.jms.XAConnection closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (Exception ex) {
				logger.debug(ex.getMessage());
			}
		}
	}

	private void closeQuietly(javax.sql.XAConnection closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (Exception ex) {
				logger.debug(ex.getMessage());
			}
		}
	}

	private void closeQuietly(javax.resource.spi.ManagedConnection closeable) {
		if (closeable != null) {
			try {
				closeable.cleanup();
			} catch (Exception ex) {
				logger.debug(ex.getMessage());
			}

			try {
				closeable.destroy();
			} catch (Exception ex) {
				logger.debug(ex.getMessage());
			}
		}
	}

	public int getTransactionTimeout() throws XAException {
		return delegate.getTransactionTimeout();
	}

	public boolean isSameRM(XAResource arg0) throws XAException {
		return delegate.isSameRM(arg0);
	}

	public int prepare(Xid arg0) throws XAException {
		return delegate.prepare(arg0);
	}

	public Xid[] recover(int arg0) throws XAException {
		Xid[] xidArray = delegate.recover(arg0);
		// for (int i = 0; this.recoverXid != null && i < xidArray.length; i++) {
		// Xid xid = xidArray[i];
		// boolean formatIdEquals = xid.getFormatId() == this.recoverXid.getFormatId();
		// boolean globalTransactionIdEquals = Arrays.equals(xid.getGlobalTransactionId(),
		// this.recoverXid.getGlobalTransactionId());
		// boolean branchQualifierEquals = Arrays.equals(xid.getBranchQualifier(), this.recoverXid.getBranchQualifier());
		// if (formatIdEquals && globalTransactionIdEquals && branchQualifierEquals) {
		// this.recved = true;
		// }
		// }
		return xidArray;
	}

	public void rollback(Xid arg0) throws XAException {
		delegate.rollback(arg0);
	}

	public boolean setTransactionTimeout(int arg0) throws XAException {
		return delegate.setTransactionTimeout(arg0);
	}

	public void start(Xid arg0, int arg1) throws XAException {
		delegate.start(arg0, arg1);
	}

	public XAResource getDelegate() {
		return delegate;
	}

	public void setDelegate(XAResource delegate) {
		this.delegate = delegate;
	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

	public Xid getRecoverXid() {
		return recoverXid;
	}

	public void setRecoverXid(Xid recoverXid) {
		this.recoverXid = recoverXid;
	}

	public Object getManaged() {
		return managed;
	}

	public void setManaged(Object managed) {
		this.managed = managed;
	}

}
