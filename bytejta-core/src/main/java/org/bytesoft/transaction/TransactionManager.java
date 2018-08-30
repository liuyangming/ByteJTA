/**
 * Copyright 2014-2016 yangming.liu<liuyangming@gmail.com>.
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

import javax.transaction.SystemException;

public interface TransactionManager extends javax.transaction.TransactionManager {

	public int getTimeoutSeconds();

	public void setTimeoutSeconds(int timeoutSeconds);

	public void associateThread(Transaction transaction);

	public Transaction desociateThread();

	public Transaction getTransaction(Thread thread);

	public Transaction getTransactionQuietly();

	public Transaction getTransaction() throws SystemException;

	public Transaction suspend() throws SystemException;

	public void setRollbackOnlyQuietly();

}
