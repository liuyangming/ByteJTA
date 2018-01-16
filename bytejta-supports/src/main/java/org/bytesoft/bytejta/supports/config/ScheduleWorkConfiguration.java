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

@org.springframework.context.annotation.Configuration
public class ScheduleWorkConfiguration {

	@org.springframework.context.annotation.Bean
	public org.bytesoft.bytejta.work.TransactionWork transactionWork() {
		return new org.bytesoft.bytejta.work.TransactionWork();
	}

	@org.springframework.context.annotation.Bean
	public org.bytesoft.transaction.work.SimpleWorkManager transactionWorkManager() {
		return new org.bytesoft.transaction.work.SimpleWorkManager();
	}

	@org.springframework.context.annotation.Bean
	public org.bytesoft.transaction.adapter.ResourceAdapterImpl transactionResourceAdapter(
			@org.springframework.beans.factory.annotation.Autowired org.bytesoft.bytejta.work.TransactionWork transactionWork,
			@org.springframework.beans.factory.annotation.Autowired org.bytesoft.bytejta.logging.SampleTransactionLogger transactionLogger) {
		org.bytesoft.transaction.adapter.ResourceAdapterImpl resourceAdapter = new org.bytesoft.transaction.adapter.ResourceAdapterImpl();
		resourceAdapter.getWorkList().add(transactionWork);
		resourceAdapter.getWorkList().add(transactionLogger);
		return resourceAdapter;
	}

	@org.springframework.context.annotation.Bean
	public org.springframework.jca.support.ResourceAdapterFactoryBean resourceAdapter(
			@org.springframework.beans.factory.annotation.Autowired org.bytesoft.transaction.adapter.ResourceAdapterImpl resourceAdapter,
			@org.springframework.beans.factory.annotation.Autowired org.bytesoft.transaction.work.SimpleWorkManager workManager) {
		org.springframework.jca.support.ResourceAdapterFactoryBean factory = new org.springframework.jca.support.ResourceAdapterFactoryBean();
		factory.setWorkManager(workManager);
		factory.setResourceAdapter(resourceAdapter);
		return factory;
	}
}
