package org.bytesoft.transaction.logging.store;

import java.util.Arrays;

import javax.transaction.xa.Xid;

public class VirtualLoggingKey implements Xid {
	public static final int FORMAT_ID = 9257;

	private byte[] globalTransactionId = new byte[0];
	private byte[] branchQualifier = new byte[0];

	public int hashCode() {
		int hash = 11;
		hash += 13 * this.getFormatId();
		hash += 17 * Arrays.hashCode(branchQualifier);
		hash += 19 * Arrays.hashCode(globalTransactionId);
		return hash;
	}

	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		} else if (VirtualLoggingKey.class.isInstance(obj) == false) {
			return false;
		}
		VirtualLoggingKey that = (VirtualLoggingKey) obj;
		if (this.getFormatId() != that.getFormatId()) {
			return false;
		} else if (Arrays.equals(branchQualifier, that.branchQualifier) == false) {
			return false;
		} else if (Arrays.equals(globalTransactionId, that.globalTransactionId) == false) {
			return false;
		}
		return true;
	}

	public int getFormatId() {
		return FORMAT_ID;
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

}
