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
package org.bytesoft.transaction.supports.rpc;

import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.xa.XAResource;

import org.apache.log4j.Logger;
import org.bytesoft.bytejta.aware.TransactionBeanFactoryAware;
import org.bytesoft.bytejta.supports.resource.RemoteResourceDescriptor;
import org.bytesoft.bytejta.supports.wire.RemoteCoordinator;
import org.bytesoft.transaction.Transaction;
import org.bytesoft.transaction.TransactionBeanFactory;
import org.bytesoft.transaction.TransactionContext;
import org.bytesoft.transaction.TransactionManager;
import org.bytesoft.transaction.xa.TransactionXid;

public class TransactionInterceptorImpl implements TransactionInterceptor, TransactionBeanFactoryAware {
	static final Logger logger = Logger.getLogger(TransactionInterceptorImpl.class.getSimpleName());
	private TransactionBeanFactory transactionBeanFactory;

	public void beforeSendRequest(TransactionRequest request) throws IllegalStateException {
		TransactionManager transactionManager = this.transactionBeanFactory.getTransactionManager();
		Transaction transaction = transactionManager.getTransactionQuietly();
		if (transaction != null) {
			TransactionContext srcTransactionContext = transaction.getTransactionContext();
			TransactionContext transactionContext = srcTransactionContext.clone();
			TransactionXid currentXid = srcTransactionContext.getXid();
			TransactionXid globalXid = currentXid.getGlobalXid();
			transactionContext.setXid(globalXid);
			request.setTransactionContext(transactionContext);

			try {
				RemoteCoordinator resource = request.getTargetTransactionCoordinator();

				RemoteResourceDescriptor descriptor = new RemoteResourceDescriptor();
				descriptor.setDelegate(resource);
				descriptor.setIdentifier(resource.getIdentifier());

				transaction.enlistResource(descriptor);
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
	}

	public void beforeSendResponse(TransactionResponse response) throws IllegalStateException {
		TransactionManager transactionManager = this.transactionBeanFactory.getTransactionManager();
		Transaction transaction = transactionManager.getTransactionQuietly();
		if (transaction != null) {
			TransactionContext srcTransactionContext = transaction.getTransactionContext();
			TransactionContext transactionContext = srcTransactionContext.clone();
			response.setTransactionContext(transactionContext);
		}
	}

	public void afterReceiveRequest(TransactionRequest request) throws IllegalStateException {

		// TransactionManager transactionManager = this.transactionBeanFactory.getTransactionManager();

		TransactionContext srcTransactionContext = request.getTransactionContext();
		if (srcTransactionContext != null) {
			// TransactionContext transactionContext = srcTransactionContext.clone();
			// try {
			// transactionManager.propagationBegin(transactionContext);
			// } catch (SystemException ex) {
			// IllegalStateException exception = new IllegalStateException();
			// exception.initCause(ex);
			// throw exception;
			// } catch (NotSupportedException ex) {
			// IllegalStateException exception = new IllegalStateException();
			// exception.initCause(ex);
			// throw exception;
			// }
		}
	}

	public void afterReceiveResponse(TransactionResponse response) throws IllegalStateException {

		TransactionManager transactionManager = this.transactionBeanFactory.getTransactionManager();
		Transaction transaction = transactionManager.getTransactionQuietly();
		if (transaction != null) {
			// TransactionContext nativeTransactionContext = transaction.getTransactionContext();
			TransactionContext remoteTransactionContext = response.getTransactionContext();
			if (remoteTransactionContext != null) {

				try {
					RemoteCoordinator resource = response.getSourceTransactionCoordinator();

					RemoteResourceDescriptor descriptor = new RemoteResourceDescriptor();
					descriptor.setDelegate(resource);
					descriptor.setIdentifier(resource.getIdentifier());

					transaction.delistResource(descriptor, XAResource.TMSUCCESS);
				} catch (IllegalStateException ex) {
					logger.error("TransactionInterceptorImpl.afterReceiveResponse(TransactionRequest)", ex);
					throw ex;
				} catch (SystemException ex) {
					logger.error("TransactionInterceptorImpl.afterReceiveResponse(TransactionRequest)", ex);
					throw new IllegalStateException(ex);
				}
			}
		}// end-if(transaction!=null)

	}

	public void setBeanFactory(TransactionBeanFactory tbf) {
		this.transactionBeanFactory = tbf;
	}

}
