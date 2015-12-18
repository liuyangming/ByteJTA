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
package org.bytesoft.transaction.supports.rpc;

import java.io.Serializable;
import java.util.Arrays;

public class TransactionCredential implements Serializable {
	private static final long serialVersionUID = 1L;

	private final byte[] credential;

	public TransactionCredential(byte[] bytes) {
		this.credential = bytes;
	}

	public byte[] getCredential() {
		return credential;
	}

	public int hashCode() {
		if (this.credential == null) {
			return 0;
		} else {
			return 17 * Arrays.hashCode(this.credential);
		}
	}

	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		} else if (TransactionCredential.class.isInstance(obj) == false) {
			return false;
		}
		TransactionCredential that = (TransactionCredential) obj;
		byte[] thisCredential = this.credential;
		byte[] thatCredential = that.credential;
		if (thisCredential == null && thatCredential == null) {
			return true;
		} else if (thisCredential == null) {
			return false;
		} else if (thatCredential == null) {
			return false;
		} else {
			return Arrays.equals(thisCredential, thatCredential);
		}
	}

}
