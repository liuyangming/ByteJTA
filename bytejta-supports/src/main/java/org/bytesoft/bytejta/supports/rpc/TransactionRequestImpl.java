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
package org.bytesoft.bytejta.supports.rpc;

import java.util.HashMap;
import java.util.Map;

import org.bytesoft.transaction.TransactionContext;
import org.bytesoft.transaction.remote.RemoteCoordinator;
import org.bytesoft.transaction.supports.rpc.TransactionRequest;

public class TransactionRequestImpl implements TransactionRequest {

	private RemoteCoordinator participantCoordinator;
	private TransactionContext transactionContext;

	private transient boolean participantEnlistFlag;

	private final Map<String, Object> headers = new HashMap<String, Object>();

	public Object getHeader(String name) {
		return this.headers.get(name);
	}

	public void setHeader(String name, Object value) {
		this.headers.put(name, value);
	}

	public RemoteCoordinator getTargetTransactionCoordinator() {
		return this.participantCoordinator;
	}

	public void setTargetTransactionCoordinator(RemoteCoordinator remoteCoordinator) {
		this.participantCoordinator = remoteCoordinator;
	}

	public TransactionContext getTransactionContext() {
		return this.transactionContext;
	}

	public void setTransactionContext(TransactionContext transactionContext) {
		this.transactionContext = transactionContext;
	}

	public boolean isParticipantEnlistFlag() {
		return participantEnlistFlag;
	}

	public void setParticipantEnlistFlag(boolean participantEnlistFlag) {
		this.participantEnlistFlag = participantEnlistFlag;
	}

}
