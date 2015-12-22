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
package org.bytesoft.bytejta.wire;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionManagerStubRegistry {
	static final TransactionManagerStubRegistry instance = new TransactionManagerStubRegistry();

	private final Map<String, TransactionManagerStub> txManagerMap = new ConcurrentHashMap<String, TransactionManagerStub>();

	public void putTransactionManagerStub(String address, TransactionManagerStub stub) {
		this.txManagerMap.put(address, stub);
	}

	public TransactionManagerStub getTransactionManagerStub(String address) {
		return this.txManagerMap.get(address);
	}

	public void removeTransactionManagerStub(String address) {
		this.txManagerMap.remove(address);
	}

	private TransactionManagerStubRegistry() {
		if (instance != null) {
			throw new IllegalStateException();
		}
	}

	public static TransactionManagerStubRegistry getInstance() {
		return instance;
	}

}
