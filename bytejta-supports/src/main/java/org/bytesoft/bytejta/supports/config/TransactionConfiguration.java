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
package org.bytesoft.bytejta.supports.config;

import org.bytesoft.bytejta.TransactionBeanFactoryImpl;
import org.bytesoft.bytejta.TransactionManagerImpl;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.TransactionManagementConfigurer;

@org.springframework.context.annotation.Configuration
@EnableTransactionManagement
public class TransactionConfiguration implements TransactionManagementConfigurer {

	public PlatformTransactionManager annotationDrivenTransactionManager() {
		org.bytesoft.transaction.TransactionManager transactionManager = this.transactionManager();
		javax.transaction.UserTransaction userTransaction = this.bytejtaUserTransaction();

		org.springframework.transaction.jta.JtaTransactionManager jtaTransactionManager //
				= new org.springframework.transaction.jta.JtaTransactionManager();
		jtaTransactionManager.setUserTransaction(userTransaction);
		jtaTransactionManager.setTransactionManager(transactionManager);

		return jtaTransactionManager;
	}

	@org.springframework.context.annotation.Bean
	public org.bytesoft.bytejta.supports.spring.ManagedConnectionFactoryPostProcessor managedConnectionFactoryPostProcessor() {
		return new org.bytesoft.bytejta.supports.spring.ManagedConnectionFactoryPostProcessor();
	}

	@org.springframework.context.annotation.Bean
	public org.bytesoft.transaction.TransactionManager transactionManager() {
		TransactionManagerImpl transactionManager = new TransactionManagerImpl();
		TransactionBeanFactoryImpl.getInstance().setTransactionManager(transactionManager);
		TransactionBeanFactoryImpl.getInstance().setTransactionTimer(transactionManager);
		return transactionManager;
	}

	@org.springframework.context.annotation.Bean
	public javax.transaction.UserTransaction bytejtaUserTransaction() {
		return new org.bytesoft.bytejta.UserTransactionImpl();
	}

	@org.springframework.context.annotation.Bean
	public org.bytesoft.bytejta.TransactionCoordinator bytejtaTransactionCoordinator() {
		org.bytesoft.bytejta.TransactionCoordinator transactionCoordinator = new org.bytesoft.bytejta.TransactionCoordinator();
		TransactionBeanFactoryImpl.getInstance().setTransactionCoordinator(transactionCoordinator);
		return transactionCoordinator;
	}

	@org.springframework.context.annotation.Bean
	public org.bytesoft.transaction.TransactionRecovery bytejtaTransactionRecovery() {
		org.bytesoft.bytejta.TransactionRecoveryImpl transactionRecovery = new org.bytesoft.bytejta.TransactionRecoveryImpl();
		TransactionBeanFactoryImpl.getInstance().setTransactionRecovery(transactionRecovery);
		return transactionRecovery;
	}

	@org.springframework.context.annotation.Bean
	public org.bytesoft.transaction.TransactionRepository bytejtaTransactionRepository() {
		org.bytesoft.bytejta.TransactionRepositoryImpl transactionRepository = new org.bytesoft.bytejta.TransactionRepositoryImpl();
		TransactionBeanFactoryImpl.getInstance().setTransactionRepository(transactionRepository);
		return transactionRepository;
	}

	@org.springframework.context.annotation.Bean
	public org.bytesoft.transaction.xa.XidFactory bytejtaXidFactory() {
		org.bytesoft.bytejta.xa.XidFactoryImpl xidFactory = new org.bytesoft.bytejta.xa.XidFactoryImpl();
		TransactionBeanFactoryImpl.getInstance().setXidFactory(xidFactory);
		return xidFactory;
	}

	@org.springframework.context.annotation.Bean
	public org.bytesoft.transaction.supports.rpc.TransactionInterceptor bytejtaTransactionInterceptor() {
		org.bytesoft.bytejta.supports.rpc.TransactionInterceptorImpl transactionInterceptor = new org.bytesoft.bytejta.supports.rpc.TransactionInterceptorImpl();
		TransactionBeanFactoryImpl.getInstance().setTransactionInterceptor(transactionInterceptor);
		return transactionInterceptor;
	}

	@org.springframework.context.annotation.Bean(initMethod = "construct")
	public org.bytesoft.transaction.logging.TransactionLogger bytejtaTransactionLogger() {
		org.bytesoft.bytejta.logging.SampleTransactionLogger transactionLogger = new org.bytesoft.bytejta.logging.SampleTransactionLogger();
		TransactionBeanFactoryImpl.getInstance().setTransactionLogger(transactionLogger);
		return transactionLogger;
	}

	@org.springframework.context.annotation.Bean
	public org.bytesoft.transaction.TransactionLock bytejtaTransactionLock() {
		org.bytesoft.bytejta.VacantTransactionLock transactionLock = new org.bytesoft.bytejta.VacantTransactionLock();
		TransactionBeanFactoryImpl.getInstance().setTransactionLock(transactionLock);
		return transactionLock;
	}

}
