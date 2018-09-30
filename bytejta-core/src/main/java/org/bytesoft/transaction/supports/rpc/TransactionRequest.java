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
package org.bytesoft.transaction.supports.rpc;

import org.bytesoft.transaction.TransactionContext;
import org.bytesoft.transaction.remote.RemoteCoordinator;

public interface TransactionRequest {

	public RemoteCoordinator getTargetTransactionCoordinator();

	public TransactionContext getTransactionContext();

	public void setTransactionContext(TransactionContext transactionContext);

	public Object getHeader(String name);

	public void setHeader(String name, Object value);

}
