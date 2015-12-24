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
package org.bytesoft.bytejta.supports.store;

import org.bytesoft.transaction.supports.store.TransactionStorageKey;
import org.bytesoft.transaction.supports.store.TransactionStorageObject;
import org.bytesoft.transaction.xa.XidFactory;

public class SimpleTransactionStorageObject implements TransactionStorageObject {
	private final byte[] byteArray;

	public SimpleTransactionStorageObject(byte[] contentByteArray) {
		this.byteArray = contentByteArray;
	}

	public TransactionStorageKey getStorageKey() {
		byte[] instanceKey = new byte[XidFactory.GLOBAL_TRANSACTION_LENGTH];
		System.arraycopy(this.byteArray, 0, instanceKey, 0, instanceKey.length);
		return new SimpleTransactionStorageKey(instanceKey);
	}

	public byte[] getContentByteArray() {
		return this.byteArray;
	}

}
