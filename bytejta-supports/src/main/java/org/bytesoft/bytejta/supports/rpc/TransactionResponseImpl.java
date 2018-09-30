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
import org.bytesoft.transaction.supports.rpc.TransactionResponse;

public class TransactionResponseImpl implements TransactionResponse {

	private boolean participantStickyRequired = true;
	private boolean participantRollbackOnly;

	private RemoteCoordinator participantCoordinator;
	private TransactionContext transactionContext;

	private transient boolean participantEnlistFlag;
	private transient boolean participantDelistFlag;

	private final Map<String, Object> headers = new HashMap<String, Object>();

	public Object getHeader(String name) {
		return this.headers.get(name);
	}

	public void setHeader(String name, Object value) {
		this.headers.put(name, value);
	}

	public RemoteCoordinator getSourceTransactionCoordinator() {
		return this.participantCoordinator;
	}

	public void setSourceTransactionCoordinator(RemoteCoordinator remoteCoordinator) {
		this.participantCoordinator = remoteCoordinator;
	}

	public TransactionContext getTransactionContext() {
		return this.transactionContext;
	}

	public void setTransactionContext(TransactionContext transactionContext) {
		this.transactionContext = transactionContext;
	}

	public boolean isParticipantRollbackOnly() {
		return participantRollbackOnly;
	}

	public void setParticipantRollbackOnly(boolean participantRollbackOnly) {
		this.participantRollbackOnly = participantRollbackOnly;
	}

	public boolean isParticipantStickyRequired() {
		return participantStickyRequired;
	}

	public void setParticipantStickyRequired(boolean participantStickyRequired) {
		this.participantStickyRequired = participantStickyRequired;
	}

	public boolean isParticipantDelistFlag() {
		return participantDelistFlag;
	}

	public void setParticipantDelistFlag(boolean participantDelistFlag) {
		this.participantDelistFlag = participantDelistFlag;
	}

	public boolean isParticipantEnlistFlag() {
		return participantEnlistFlag;
	}

	public void setParticipantEnlistFlag(boolean participantEnlistFlag) {
		this.participantEnlistFlag = participantEnlistFlag;
	}

}
