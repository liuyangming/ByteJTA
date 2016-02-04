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
package org.bytesoft.bytejta;

import org.bytesoft.bytejta.supports.wire.RemoteCoordinator;
import org.bytesoft.transaction.TransactionBeanFactory;
import org.bytesoft.transaction.TransactionManager;
import org.bytesoft.transaction.TransactionRecovery;
import org.bytesoft.transaction.TransactionRepository;
import org.bytesoft.transaction.supports.TransactionTimer;
import org.bytesoft.transaction.supports.logger.TransactionLogger;
import org.bytesoft.transaction.supports.logger.TransactionLoggerProxy;
import org.bytesoft.transaction.supports.rpc.TransactionInterceptor;
import org.bytesoft.transaction.xa.XidFactory;

public class TransactionBeanFactoryImpl implements TransactionBeanFactory {
	private TransactionManager transactionManager;
	private XidFactory xidFactory;
	private TransactionTimer transactionTimer;
	private final TransactionLoggerProxy transactionLogger = new TransactionLoggerProxy();
	private TransactionRepository<TransactionImpl> transactionRepository;
	private TransactionInterceptor transactionInterceptor;
	private TransactionRecovery transactionRecovery;
	private RemoteCoordinator nativeCoordinator;

	public void setTransactionLogger(TransactionLogger transactionLogger) {
		this.transactionLogger.setDelegate(transactionLogger);
	}

	public TransactionManager getTransactionManager() {
		return transactionManager;
	}

	public void setTransactionManager(TransactionManager transactionManager) {
		this.transactionManager = transactionManager;
	}

	public XidFactory getXidFactory() {
		return xidFactory;
	}

	public void setXidFactory(XidFactory xidFactory) {
		this.xidFactory = xidFactory;
	}

	public TransactionTimer getTransactionTimer() {
		return transactionTimer;
	}

	public void setTransactionTimer(TransactionTimer transactionTimer) {
		this.transactionTimer = transactionTimer;
	}

	@SuppressWarnings("unchecked")
	public TransactionRepository<TransactionImpl> getTransactionRepository() {
		return transactionRepository;
	}

	public void setTransactionRepository(TransactionRepository<TransactionImpl> transactionRepository) {
		this.transactionRepository = transactionRepository;
	}

	public TransactionInterceptor getTransactionInterceptor() {
		return transactionInterceptor;
	}

	public void setTransactionInterceptor(TransactionInterceptor transactionInterceptor) {
		this.transactionInterceptor = transactionInterceptor;
	}

	public TransactionRecovery getTransactionRecovery() {
		return transactionRecovery;
	}

	public void setTransactionRecovery(TransactionRecovery transactionRecovery) {
		this.transactionRecovery = transactionRecovery;
	}

	public RemoteCoordinator getNativeCoordinator() {
		return nativeCoordinator;
	}

	public void setNativeCoordinator(RemoteCoordinator nativeCoordinator) {
		this.nativeCoordinator = nativeCoordinator;
	}

	public TransactionLoggerProxy getTransactionLogger() {
		return transactionLogger;
	}

}
