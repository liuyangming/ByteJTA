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
package org.bytesoft.transaction.supports.store;

import java.util.List;

public interface TransactionStorageManager {

	public int registerResource(String identifier) throws IllegalArgumentException;

	public List<String> getRegisteredResources();

	public int getRegisteredResource(String identifier) throws IllegalStateException;

	public String getRegisteredResource(int index) throws IllegalStateException;

	public List<TransactionStorageKey> getStorageKeyList();

	public TransactionStorageObject locateStorageObject(TransactionStorageKey storageKey);

	public void createStorageObject(TransactionStorageObject storageObject);

	public void modifyStorageObject(TransactionStorageObject storageObject);

	public void deleteStorageObject(TransactionStorageObject storageObject);

}
