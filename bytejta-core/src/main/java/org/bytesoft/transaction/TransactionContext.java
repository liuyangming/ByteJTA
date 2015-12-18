/**
 * Copyright 2014 yangming.liu<liuyangming@gmail.com>.
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

import org.bytesoft.bytejta.common.TransactionConfigurator;
import org.bytesoft.transaction.xa.TransactionXid;

public class TransactionContext implements Serializable, Cloneable {
	private static final long serialVersionUID = 1L;

	private transient boolean optimized;
	private transient boolean coordinator;
	private transient boolean recovery;
	private transient int prepareVote = -1;

	private TransactionXid currentXid;
	private Object propagated;
	private long createdTime;
	private long expiredTime;
	private boolean compensable;
	private boolean nonxaResourceAllowed;

	public TransactionContext() {
		TransactionConfigurator transactionConfigurator = TransactionConfigurator.getInstance();
		this.nonxaResourceAllowed = transactionConfigurator.isOptimizeEnabled();
	}

	public TransactionContext(boolean nonxaResourceAllowed) {
		this.nonxaResourceAllowed = nonxaResourceAllowed;
	}

	public TransactionContext clone() {
		TransactionContext that = new TransactionContext();
		that.currentXid = this.currentXid;
		that.propagated = this.propagated;
		that.createdTime = System.currentTimeMillis();
		that.expiredTime = this.expiredTime;
		that.compensable = this.compensable;
		that.nonxaResourceAllowed = this.nonxaResourceAllowed;
		return that;
	}

	public boolean isCoordinator() {
		return this.coordinator;
	}

	public TransactionXid getCurrentXid() {
		return currentXid;
	}

	public void setCurrentXid(TransactionXid branchXid) {
		this.currentXid = branchXid;
	}

	public TransactionXid getGlobalXid() {
		return this.currentXid.getGlobalXid();
	}

	public void setCoordinator(boolean coordinator) {
		this.coordinator = coordinator;
	}

	public long getExpiredTime() {
		return expiredTime;
	}

	public void setExpiredTime(long expiredTime) {
		this.expiredTime = expiredTime;
	}

	public long getCreatedTime() {
		return createdTime;
	}

	public void setCreatedTime(long createdTime) {
		this.createdTime = createdTime;
	}

	public boolean isCompensable() {
		return compensable;
	}

	public void setCompensable(boolean compensable) {
		this.compensable = compensable;
	}

	public boolean isFresh() {
		return this.recovery == false;
	}

	public boolean isRecovery() {
		return recovery;
	}

	public void setRecovery(boolean recovery) {
		this.recovery = recovery;
	}

	public boolean isOptimized() {
		return optimized;
	}

	public void setOptimized(boolean optimized) {
		this.optimized = optimized;
	}

	public int getPrepareVote() {
		return prepareVote;
	}

	public void setPrepareVote(int prepareVote) {
		this.prepareVote = prepareVote;
	}

	public Object getPropagated() {
		return propagated;
	}

	public void setPropagated(Object propagated) {
		this.propagated = propagated;
	}

	public boolean isNonxaResourceAllowed() {
		return nonxaResourceAllowed;
	}

	public void setNonxaResourceAllowed(boolean nonxaResourceAllowed) {
		this.nonxaResourceAllowed = nonxaResourceAllowed;
	}

}
