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

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.bytesoft.bytejta.supports.resource.RemoteResourceDescriptor;
import org.bytesoft.transaction.Transaction;
import org.bytesoft.transaction.TransactionBeanFactory;
import org.bytesoft.transaction.TransactionContext;
import org.bytesoft.transaction.TransactionManager;
import org.bytesoft.transaction.TransactionParticipant;
import org.bytesoft.transaction.aware.TransactionBeanFactoryAware;
import org.bytesoft.transaction.remote.RemoteCoordinator;
import org.bytesoft.transaction.supports.rpc.TransactionInterceptor;
import org.bytesoft.transaction.supports.rpc.TransactionRequest;
import org.bytesoft.transaction.supports.rpc.TransactionResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionInterceptorImpl implements TransactionInterceptor, TransactionBeanFactoryAware {
	static final Logger logger = LoggerFactory.getLogger(TransactionInterceptorImpl.class);

	@javax.inject.Inject
	protected TransactionBeanFactory beanFactory;

	public void beforeSendRequest(TransactionRequest request) throws IllegalStateException {
		TransactionManager transactionManager = this.beanFactory.getTransactionManager();
		Transaction transaction = transactionManager.getTransactionQuietly();
		if (transaction == null) {
			return;
		}

		if (transaction.getTransactionStatus() == Status.STATUS_MARKED_ROLLBACK) {
			throw new IllegalStateException(
					"Transaction has been marked as rollback only, can not propagate its context to remote branch.");
		} // end-if (transaction.getTransactionStatus() == Status.STATUS_MARKED_ROLLBACK)

		TransactionContext srcTransactionContext = transaction.getTransactionContext();
		TransactionContext transactionContext = srcTransactionContext.clone();
		request.setTransactionContext(transactionContext);
		try {
			RemoteCoordinator resource = request.getTargetTransactionCoordinator();

			RemoteResourceDescriptor descriptor = new RemoteResourceDescriptor();
			descriptor.setDelegate(resource);

			boolean participantEnlisted = transaction.enlistResource(descriptor);
			((TransactionRequestImpl) request).setParticipantEnlistFlag(participantEnlisted);
		} catch (IllegalStateException ex) {
			logger.error("TransactionInterceptorImpl.beforeSendRequest(TransactionRequest)", ex);
			throw ex;
		} catch (RollbackException ex) {
			transaction.setRollbackOnlyQuietly();
			logger.error("TransactionInterceptorImpl.beforeSendRequest(TransactionRequest)", ex);
			throw new IllegalStateException(ex);
		} catch (SystemException ex) {
			logger.error("TransactionInterceptorImpl.beforeSendRequest(TransactionRequest)", ex);
			throw new IllegalStateException(ex);
		}
	}

	public void beforeSendResponse(TransactionResponse response) throws IllegalStateException {
		TransactionManager transactionManager = this.beanFactory.getTransactionManager();
		Transaction transaction = transactionManager.getTransactionQuietly();
		if (transaction == null) {
			return;
		}

		TransactionParticipant coordinator = this.beanFactory.getNativeParticipant();

		TransactionContext srcTransactionContext = transaction.getTransactionContext();
		TransactionContext transactionContext = srcTransactionContext.clone();
		transactionContext.setPropagatedBy(srcTransactionContext.getPropagatedBy());

		response.setTransactionContext(transactionContext);
		// response.setSourceTransactionCoordinator(coordinator);
		try {
			coordinator.end(transactionContext, XAResource.TMSUCCESS);
		} catch (XAException ex) {
			throw new IllegalStateException(ex);
		}

	}

	public void afterReceiveRequest(TransactionRequest request) throws IllegalStateException {
		TransactionContext srcTransactionContext = request.getTransactionContext();
		if (srcTransactionContext == null) {
			return;
		}

		TransactionParticipant coordinator = this.beanFactory.getNativeParticipant();

		TransactionContext transactionContext = srcTransactionContext.clone();
		transactionContext.setPropagatedBy(srcTransactionContext.getPropagatedBy());
		try {
			coordinator.start(transactionContext, XAResource.TMNOFLAGS);
		} catch (XAException ex) {
			throw new IllegalStateException(ex);
		}
	}

	public void afterReceiveResponse(TransactionResponse response) throws IllegalStateException {
		TransactionManager transactionManager = this.beanFactory.getTransactionManager();
		Transaction transaction = transactionManager.getTransactionQuietly();
		TransactionContext transactionContext = response.getTransactionContext();
		RemoteCoordinator resource = response.getSourceTransactionCoordinator();

		boolean participantEnlistFlag = ((TransactionResponseImpl) response).isParticipantEnlistFlag();
		// boolean participantDelistFlag = ((TransactionResponseImpl) response).isParticipantDelistFlag();

		if (transaction == null || transactionContext == null) {
			return;
		} else if (participantEnlistFlag == false) {
			return;
		} else if (resource == null) {
			logger.error("TransactionInterceptorImpl.afterReceiveResponse(TransactionRequest): remote coordinator is null.");
			throw new IllegalStateException("remote coordinator is null.");
		}

		try {
			RemoteResourceDescriptor descriptor = new RemoteResourceDescriptor();
			descriptor.setDelegate(resource);
			// descriptor.setIdentifier(resource.getIdentifier());

			transaction.delistResource(descriptor, XAResource.TMSUCCESS);
			// transaction.delistResource(descriptor, participantDelistFlag ? XAResource.TMFAIL : XAResource.TMSUCCESS);
		} catch (IllegalStateException ex) {
			logger.error("TransactionInterceptorImpl.afterReceiveResponse(TransactionRequest)", ex);
			throw ex;
		} catch (SystemException ex) {
			logger.error("TransactionInterceptorImpl.afterReceiveResponse(TransactionRequest)", ex);
			throw new IllegalStateException(ex);
		}

	}

	public TransactionBeanFactory getBeanFactory() {
		return this.beanFactory;
	}

	public void setBeanFactory(TransactionBeanFactory tbf) {
		this.beanFactory = tbf;
	}

}
