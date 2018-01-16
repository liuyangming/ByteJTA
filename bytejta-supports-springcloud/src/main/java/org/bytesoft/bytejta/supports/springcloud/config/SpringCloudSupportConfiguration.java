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
package org.bytesoft.bytejta.supports.springcloud.config;

@org.springframework.context.annotation.Configuration
public class SpringCloudSupportConfiguration {

	@org.springframework.context.annotation.Bean
	public org.bytesoft.bytejta.supports.springcloud.SpringCloudEndpointPostProcessor springCloudEndpointPostProcessor() {
		return new org.bytesoft.bytejta.supports.springcloud.SpringCloudEndpointPostProcessor();
	}

	@org.springframework.context.annotation.Bean
	public org.bytesoft.bytejta.supports.springcloud.SpringCloudBeanRegistry springCloudBeanRegistry() {
		return org.bytesoft.bytejta.supports.springcloud.SpringCloudBeanRegistry.getInstance();
	}

	// @org.springframework.context.annotation.Bean
	// public org.bytesoft.bytejta.supports.springcloud.controller.TransactionCoordinatorController
	// transactionCoordinatorController() {
	// return new org.bytesoft.bytejta.supports.springcloud.controller.TransactionCoordinatorController();
	// }

	@org.springframework.context.annotation.Bean
	public org.bytesoft.bytejta.logging.deserializer.XAResourceArchiveDeserializer bytejtaXAResourceDeserializer() {
		return new org.bytesoft.bytejta.logging.deserializer.XAResourceArchiveDeserializer();
	}

	@org.springframework.context.annotation.Bean
	public org.bytesoft.bytejta.logging.deserializer.TransactionArchiveDeserializer bytejtaTransactionDeserializer(
			@org.springframework.beans.factory.annotation.Autowired org.bytesoft.bytejta.logging.deserializer.XAResourceArchiveDeserializer resourceArchiveDeserializer) {
		org.bytesoft.bytejta.logging.deserializer.TransactionArchiveDeserializer transactionArchiveDeserializer //
				= new org.bytesoft.bytejta.logging.deserializer.TransactionArchiveDeserializer();
		transactionArchiveDeserializer.setResourceArchiveDeserializer(resourceArchiveDeserializer);
		return transactionArchiveDeserializer;
	}

	@org.springframework.context.annotation.Bean
	public org.bytesoft.bytejta.logging.ArchiveDeserializerImpl bytejtaArchiveDeserializer(
			@org.springframework.beans.factory.annotation.Autowired org.bytesoft.bytejta.logging.deserializer.TransactionArchiveDeserializer transactionArchiveDeserializer,
			@org.springframework.beans.factory.annotation.Autowired org.bytesoft.bytejta.logging.deserializer.XAResourceArchiveDeserializer xaResourceArchiveDeserializer) {
		org.bytesoft.bytejta.logging.ArchiveDeserializerImpl archiveDeserializer //
				= new org.bytesoft.bytejta.logging.ArchiveDeserializerImpl();
		archiveDeserializer.setTransactionArchiveDeserializer(transactionArchiveDeserializer);
		archiveDeserializer.setXaResourceArchiveDeserializer(xaResourceArchiveDeserializer);
		return archiveDeserializer;
	}

	@org.springframework.context.annotation.Bean
	public org.bytesoft.bytejta.supports.springcloud.serialize.XAResourceDeserializerImpl bytejtaResourceDeserializer() {
		return new org.bytesoft.bytejta.supports.springcloud.serialize.XAResourceDeserializerImpl();
	}

	@org.springframework.context.annotation.Bean
	public org.bytesoft.bytejta.TransactionBeanFactoryImpl bytejtaBeanFactory(
			@org.springframework.beans.factory.annotation.Autowired org.bytesoft.transaction.TransactionManager transactionManager,
			@org.springframework.beans.factory.annotation.Autowired org.bytesoft.transaction.xa.XidFactory xidFactory,
			@org.springframework.beans.factory.annotation.Autowired org.bytesoft.transaction.supports.TransactionTimer transactionTimer,
			@org.springframework.beans.factory.annotation.Autowired org.bytesoft.transaction.logging.TransactionLogger transactionLogger,
			@org.springframework.beans.factory.annotation.Autowired org.bytesoft.transaction.TransactionRepository transactionRepository,
			@org.springframework.beans.factory.annotation.Autowired org.bytesoft.transaction.supports.rpc.TransactionInterceptor transactionInterceptor,
			@org.springframework.beans.factory.annotation.Autowired org.bytesoft.transaction.TransactionRecovery transactionRecovery,
			@org.springframework.beans.factory.annotation.Autowired org.bytesoft.bytejta.supports.wire.RemoteCoordinator transactionCoordinator,
			@org.springframework.beans.factory.annotation.Autowired org.bytesoft.transaction.TransactionLock transactionLock,
			@org.springframework.beans.factory.annotation.Autowired org.bytesoft.transaction.logging.ArchiveDeserializer archiveDeserializer,
			@org.springframework.beans.factory.annotation.Autowired org.bytesoft.transaction.supports.serialize.XAResourceDeserializer resourceDeserializer) {

		org.bytesoft.bytejta.TransactionBeanFactoryImpl beanFactory = new org.bytesoft.bytejta.TransactionBeanFactoryImpl();

		beanFactory.setTransactionManager(transactionManager);
		beanFactory.setXidFactory(xidFactory);
		beanFactory.setTransactionTimer(transactionTimer);
		beanFactory.setTransactionLogger(transactionLogger);
		beanFactory.setTransactionRepository(transactionRepository);
		beanFactory.setTransactionInterceptor(transactionInterceptor);
		beanFactory.setTransactionRecovery(transactionRecovery);
		beanFactory.setTransactionCoordinator(transactionCoordinator);
		beanFactory.setTransactionLock(transactionLock);
		beanFactory.setArchiveDeserializer(archiveDeserializer);
		beanFactory.setResourceDeserializer(resourceDeserializer);

		return beanFactory;
	}

}
