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
package org.bytesoft.transaction;

import java.io.Serializable;

import org.bytesoft.transaction.xa.TransactionXid;

public class TransactionContext implements Serializable, Cloneable {
	private static final long serialVersionUID = 1L;

	private transient boolean coordinator;
	private transient boolean recoveried;
	private transient boolean compensating;

	private TransactionXid xid;
	private long createdTime;
	private long expiredTime;
	private boolean compensable;

	public TransactionContext clone() {
		TransactionContext that = new TransactionContext();
		that.xid = this.xid.clone();
		that.createdTime = System.currentTimeMillis();
		that.expiredTime = this.expiredTime;
		that.compensable = this.compensable;
		return that;
	}

	public boolean isCoordinator() {
		return coordinator;
	}

	public void setCoordinator(boolean coordinator) {
		this.coordinator = coordinator;
	}

	public boolean isRecoveried() {
		return recoveried;
	}

	public void setRecoveried(boolean recoveried) {
		this.recoveried = recoveried;
	}

	public TransactionXid getXid() {
		return xid;
	}

	public void setXid(TransactionXid xid) {
		this.xid = xid;
	}

	public long getCreatedTime() {
		return createdTime;
	}

	public void setCreatedTime(long createdTime) {
		this.createdTime = createdTime;
	}

	public long getExpiredTime() {
		return expiredTime;
	}

	public void setExpiredTime(long expiredTime) {
		this.expiredTime = expiredTime;
	}

	public boolean isCompensable() {
		return compensable;
	}

	public void setCompensable(boolean compensable) {
		this.compensable = compensable;
	}

	public boolean isCompensating() {
		return compensating;
	}

	public void setCompensating(boolean compensating) {
		this.compensating = compensating;
	}

}
