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

import org.bytesoft.bytejta.supports.wire.RemoteCoordinator;
import org.bytesoft.transaction.TransactionContext;
import org.bytesoft.transaction.supports.rpc.TransactionResponse;

public class TransactionResponseImpl implements TransactionResponse {

	private boolean participantRollbackRequired;
	private boolean participantStickyRequired = true;
	private RemoteCoordinator participantCoordinator;
	private TransactionContext transactionContext;

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

	public boolean isParticipantRollbackRequired() {
		return participantRollbackRequired;
	}

	public void setParticipantRollbackRequired(boolean participantRollbackRequired) {
		this.participantRollbackRequired = participantRollbackRequired;
	}

	public boolean isParticipantStickyRequired() {
		return participantStickyRequired;
	}

	public void setParticipantStickyRequired(boolean participantStickyRequired) {
		this.participantStickyRequired = participantStickyRequired;
	}

}
