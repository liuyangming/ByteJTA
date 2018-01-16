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
package org.bytesoft.bytejta.supports.dubbo.config;

import org.bytesoft.bytejta.TransactionBeanFactoryImpl;
import org.bytesoft.bytejta.supports.config.ScheduleWorkConfiguration;
import org.bytesoft.bytejta.supports.config.TransactionConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.TransactionManagementConfigurer;

@EnableTransactionManagement
@Import({ TransactionConfiguration.class, ScheduleWorkConfiguration.class })
@Configuration
public class DubboSupportConfiguration implements TransactionManagementConfigurer, ApplicationContextAware {
	private ApplicationContext applicationContext;

	public PlatformTransactionManager annotationDrivenTransactionManager() {
		org.springframework.transaction.jta.JtaTransactionManager jtaTransactionManager //
				= new org.springframework.transaction.jta.JtaTransactionManager();
		jtaTransactionManager.setTransactionManager(TransactionBeanFactoryImpl.getInstance().getTransactionManager());
		return jtaTransactionManager;
	}

	@org.springframework.context.annotation.Bean
	public org.bytesoft.bytejta.supports.dubbo.TransactionEndpointPostProcessor transactionEndpointPostProcessor() {
		return new org.bytesoft.bytejta.supports.dubbo.TransactionEndpointPostProcessor();
	}

	@org.springframework.context.annotation.Bean
	public org.bytesoft.bytejta.supports.dubbo.DubboConfigPostProcessor dubboConfigPostProcessor() {
		return new org.bytesoft.bytejta.supports.dubbo.DubboConfigPostProcessor();
	}

	@org.springframework.context.annotation.Bean
	public org.bytesoft.bytejta.supports.dubbo.TransactionConfigPostProcessor transactionConfigPostProcessor() {
		return new org.bytesoft.bytejta.supports.dubbo.TransactionConfigPostProcessor();
	}

	@org.springframework.context.annotation.Bean
	public org.bytesoft.bytejta.supports.dubbo.TransactionBeanRegistry springCloudBeanRegistry() {
		return org.bytesoft.bytejta.supports.dubbo.TransactionBeanRegistry.getInstance();
	}

	@org.springframework.context.annotation.Bean
	public org.bytesoft.bytejta.logging.deserializer.XAResourceArchiveDeserializer bytejtaXAResourceDeserializer() {
		return new org.bytesoft.bytejta.logging.deserializer.XAResourceArchiveDeserializer();
	}

	@org.springframework.context.annotation.Bean
	public org.bytesoft.bytejta.logging.deserializer.TransactionArchiveDeserializer bytejtaTransactionDeserializer(
			@Autowired org.bytesoft.bytejta.logging.deserializer.XAResourceArchiveDeserializer resourceArchiveDeserializer) {
		org.bytesoft.bytejta.logging.deserializer.TransactionArchiveDeserializer transactionArchiveDeserializer //
				= new org.bytesoft.bytejta.logging.deserializer.TransactionArchiveDeserializer();
		transactionArchiveDeserializer.setResourceArchiveDeserializer(resourceArchiveDeserializer);
		return transactionArchiveDeserializer;
	}

	@org.springframework.context.annotation.Bean
	public org.bytesoft.bytejta.logging.ArchiveDeserializerImpl bytejtaArchiveDeserializer(
			@Autowired org.bytesoft.bytejta.logging.deserializer.TransactionArchiveDeserializer transactionArchiveDeserializer,
			@Autowired org.bytesoft.bytejta.logging.deserializer.XAResourceArchiveDeserializer xaResourceArchiveDeserializer) {
		org.bytesoft.bytejta.logging.ArchiveDeserializerImpl archiveDeserializer //
				= new org.bytesoft.bytejta.logging.ArchiveDeserializerImpl();
		archiveDeserializer.setTransactionArchiveDeserializer(transactionArchiveDeserializer);
		archiveDeserializer.setXaResourceArchiveDeserializer(xaResourceArchiveDeserializer);
		TransactionBeanFactoryImpl.getInstance().setArchiveDeserializer(archiveDeserializer);
		return archiveDeserializer;
	}

	@org.springframework.context.annotation.Bean
	public org.bytesoft.bytejta.supports.dubbo.serialize.XAResourceDeserializerImpl bytejtaResourceDeserializer() {
		org.bytesoft.bytejta.supports.dubbo.serialize.XAResourceDeserializerImpl resourceDeserializer //
				= new org.bytesoft.bytejta.supports.dubbo.serialize.XAResourceDeserializerImpl();
		TransactionBeanFactoryImpl.getInstance().setResourceDeserializer(resourceDeserializer);
		return resourceDeserializer;
	}

	@org.springframework.context.annotation.Bean
	public org.bytesoft.transaction.TransactionBeanFactory transactionBeanFactory() {
		return TransactionBeanFactoryImpl.getInstance();
	}

	public ApplicationContext getApplicationContext() {
		return applicationContext;
	}

	public void setApplicationContext(ApplicationContext applicationContext) {
		this.applicationContext = applicationContext;
	}

}
