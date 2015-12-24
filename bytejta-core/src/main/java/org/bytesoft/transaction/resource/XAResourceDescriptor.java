/**
 * Copyright 2014-2015 yangming.liu<liuyangming@gmail.com>.
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
package org.bytesoft.transaction.resource;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public class XAResourceDescriptor implements XAResource {
	private boolean remote;
	private XAResource delegate;
	private String identifier;
	private int descriptorId;

	public void commit(Xid xid, boolean arg1) throws XAException {
		delegate.commit(xid, arg1);
	}

	public void end(Xid xid, int arg1) throws XAException {
		delegate.end(xid, arg1);
	}

	public void forget(Xid xid) throws XAException {
		delegate.forget(xid);
	}

	public int getTransactionTimeout() throws XAException {
		return delegate.getTransactionTimeout();
	}

	public boolean isSameRM(XAResource arg0) throws XAException {
		if (XAResourceDescriptor.class.isInstance(arg0)) {
			XAResourceDescriptor descriptor = (XAResourceDescriptor) arg0;
			return delegate.isSameRM(descriptor.getDelegate());
		} else {
			return delegate.isSameRM(arg0);
		}
	}

	public int prepare(Xid xid) throws XAException {
		return delegate.prepare(xid);
	}

	public Xid[] recover(int flags) throws XAException {
		return delegate.recover(flags);
	}

	public void rollback(Xid xid) throws XAException {
		delegate.rollback(xid);
	}

	public boolean setTransactionTimeout(int seconds) throws XAException {
		return delegate.setTransactionTimeout(seconds);
	}

	public boolean setTransactionTimeoutQuietly(int seconds) {
		try {
			return delegate.setTransactionTimeout(seconds);
		} catch (XAException xa) {
			return false;
		}
	}

	public void start(Xid xid, int flag) throws XAException {
		delegate.start(xid, flag);
	}

	public String toString() {
		return String.format("xa-res-descriptor(identifier= %s, descriptor= %s, remote= %s)" //
				, this.identifier, this.descriptorId, this.remote);
	}

	public boolean isRemote() {
		return remote;
	}

	public void setRemote(boolean remote) {
		this.remote = remote;
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

	public int getDescriptorId() {
		return descriptorId;
	}

	public void setDescriptorId(int descriptorId) {
		this.descriptorId = descriptorId;
	}

}
