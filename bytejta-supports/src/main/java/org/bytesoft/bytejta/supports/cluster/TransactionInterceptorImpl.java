/**
 * Copyright 2014-2018 yangming.liu<bytefox@126.com>.
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
package org.bytesoft.bytejta.supports.cluster;

import java.util.List;

import javax.transaction.Status;

import org.bytesoft.bytejta.TransactionImpl;
import org.bytesoft.bytejta.supports.internal.MongoTransactionLogger;
import org.bytesoft.transaction.Transaction;
import org.bytesoft.transaction.TransactionContext;
import org.bytesoft.transaction.TransactionManager;
import org.bytesoft.transaction.archive.XAResourceArchive;
import org.bytesoft.transaction.logging.TransactionLogger;
import org.bytesoft.transaction.supports.rpc.TransactionInterceptor;
import org.bytesoft.transaction.supports.rpc.TransactionRequest;
import org.bytesoft.transaction.supports.rpc.TransactionResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionInterceptorImpl extends org.bytesoft.bytejta.supports.rpc.TransactionInterceptorImpl
		implements TransactionInterceptor {
	static final Logger logger = LoggerFactory.getLogger(TransactionInterceptorImpl.class);

	public void beforeSendRequest(TransactionRequest request) throws IllegalStateException {
		MongoTransactionLogger transactionLogger = (MongoTransactionLogger) this.beanFactory.getTransactionLogger();
		TransactionManager transactionManager = this.beanFactory.getTransactionManager();

		TransactionImpl transaction = (TransactionImpl) transactionManager.getTransactionQuietly();
		List<XAResourceArchive> participants = transaction == null ? null : transaction.getRemoteParticipantList();

		super.beforeSendRequest(request);

		if (participants != null && participants.isEmpty()) {
			transactionLogger.createTransactionImmediately(transaction.getTransactionArchive());
		} // end-if (participants != null && participants.isEmpty())

	}

	public void beforeSendResponse(TransactionResponse response) throws IllegalStateException {
		TransactionManager transactionManager = this.beanFactory.getTransactionManager();
		TransactionLogger transactionLogger = this.beanFactory.getTransactionLogger();

		TransactionImpl transaction = (TransactionImpl) transactionManager.getTransactionQuietly();
		if (transaction == null) {
			super.beforeSendResponse(response);
		} else {
			transaction.delistAllResourceQuietly();

			int oldStatus = transaction.getTransactionStatus();
			transactionLogger.updateTransaction(transaction.getTransactionArchive());
			int newStatus = transaction.getTransactionStatus();

			super.beforeSendResponse(response);

			if (oldStatus != newStatus && newStatus == Status.STATUS_MARKED_ROLLBACK) {
				throw new IllegalStateException();
			} // end-if (oldStatus != newStatus && newStatus == Status.STATUS_MARKED_ROLLBACK)
		} // end-if (transaction != null)

	}

	public void afterReceiveRequest(TransactionRequest request) throws IllegalStateException {
		MongoTransactionLogger transactionLogger = (MongoTransactionLogger) this.beanFactory.getTransactionLogger();
		TransactionManager transactionManager = this.beanFactory.getTransactionManager();

		super.afterReceiveRequest(request);

		Transaction transaction = transactionManager.getTransactionQuietly();
		TransactionContext transactionContext = transaction == null ? null : transaction.getTransactionContext();
		if (transactionContext != null && transactionContext.isRecoveried() == false) {
			transactionLogger.createTransactionImmediately(transaction.getTransactionArchive());
		} // end-if (transactionContext != null && transactionContext.isRecoveried() == false)

	}

	public void afterReceiveResponse(TransactionResponse response) throws IllegalStateException {
		TransactionManager transactionManager = this.beanFactory.getTransactionManager();
		TransactionLogger transactionLogger = this.beanFactory.getTransactionLogger();

		super.afterReceiveResponse(response);

		Transaction transaction = transactionManager.getTransactionQuietly();
		if (transaction != null) {
			transactionLogger.updateTransaction(transaction.getTransactionArchive());
		} // end-if (transaction != null)

	}

}
