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

import java.nio.MappedByteBuffer;

import org.bytesoft.transaction.supports.store.TransactionStorageKey;

public class SimpleStoragePosition {
	private boolean enabled;
	private transient TransactionStorageKey key;
	private MappedByteBuffer buffer;

	public TransactionStorageKey getKey() {
		return key;
	}

	public void setKey(TransactionStorageKey key) {
		this.key = key;
	}

	public MappedByteBuffer getBuffer() {
		return buffer;
	}

	public void setBuffer(MappedByteBuffer buffer) {
		this.buffer = buffer;
	}

	public boolean isEnabled() {
		return enabled;
	}

	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

}
