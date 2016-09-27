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
package org.bytesoft.transaction.xa;

import java.io.Serializable;
import java.util.Arrays;

import javax.transaction.xa.Xid;

import org.bytesoft.common.utils.ByteUtils;

public class TransactionXid implements Xid, Cloneable, Serializable {
	private static final long serialVersionUID = 1L;

	private int formatId;
	private byte[] globalTransactionId;
	private byte[] branchQualifier;

	public TransactionXid() {
	}

	public TransactionXid(int formatId, byte[] global) {
		this(formatId, global, new byte[0]);
	}

	public TransactionXid(int formatId, byte[] global, byte[] branch) {
		if (global == null) {
			throw new IllegalArgumentException("globalTransactionId cannot be null.");
		} else if (global.length > MAXGTRIDSIZE) {
			throw new IllegalArgumentException("length of globalTransactionId cannot exceed 64 bytes.");
		}

		if (branch == null) {
			throw new IllegalArgumentException("branchQualifier cannot be null.");
		} else if (branch.length > MAXBQUALSIZE) {
			throw new IllegalArgumentException("length of branchQualifier cannot exceed 64 bytes.");
		}

		this.globalTransactionId = new byte[global.length];
		this.branchQualifier = new byte[branch.length];

		System.arraycopy(global, 0, this.globalTransactionId, 0, global.length);
		System.arraycopy(branch, 0, this.branchQualifier, 0, branch.length);

		this.formatId = formatId;
		this.globalTransactionId = global;
		this.branchQualifier = branch;
	}

	public TransactionXid clone() {
		TransactionXid that = new TransactionXid();
		that.setFormatId(this.formatId);
		byte[] global = new byte[this.globalTransactionId.length];
		byte[] branch = new byte[this.branchQualifier.length];

		System.arraycopy(this.globalTransactionId, 0, global, 0, this.globalTransactionId.length);
		System.arraycopy(this.branchQualifier, 0, branch, 0, this.branchQualifier.length);

		that.setGlobalTransactionId(global);
		that.setBranchQualifier(branch);

		return that;
	}

	public int getFormatId() {
		return this.formatId;
	}

	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.getFormatId();
		result = prime * result + Arrays.hashCode(branchQualifier);
		result = prime * result + Arrays.hashCode(globalTransactionId);
		return result;
	}

	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		} else if (obj == null) {
			return false;
		} else if (getClass() != obj.getClass()) {
			return false;
		}
		TransactionXid other = (TransactionXid) obj;
		if (this.formatId != other.formatId) {
			return false;
		} else if (Arrays.equals(branchQualifier, other.branchQualifier) == false) {
			return false;
		} else if (Arrays.equals(globalTransactionId, other.globalTransactionId) == false) {
			return false;
		}
		return true;
	}

	public String toString() {
		String global = this.globalTransactionId == null ? null : ByteUtils.byteArrayToString(this.globalTransactionId);
		String branch = this.branchQualifier == null ? null : ByteUtils.byteArrayToString(this.branchQualifier);
		return String.format("%s-%s-%s", this.getFormatId(), global, branch);
	}

	public byte[] getGlobalTransactionId() {
		return globalTransactionId;
	}

	public void setGlobalTransactionId(byte[] globalTransactionId) {
		this.globalTransactionId = globalTransactionId;
	}

	public byte[] getBranchQualifier() {
		return branchQualifier;
	}

	public void setBranchQualifier(byte[] branchQualifier) {
		this.branchQualifier = branchQualifier;
	}

	public void setFormatId(int formatId) {
		this.formatId = formatId;
	}

}
