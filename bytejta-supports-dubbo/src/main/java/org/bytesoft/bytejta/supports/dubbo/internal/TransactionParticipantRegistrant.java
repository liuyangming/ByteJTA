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
package org.bytesoft.bytejta.supports.dubbo.internal;

import org.bytesoft.bytejta.TransactionCoordinator;
import org.bytesoft.bytejta.supports.dubbo.TransactionBeanRegistry;
import org.bytesoft.common.utils.CommonUtils;
import org.bytesoft.transaction.aware.TransactionEndpointAware;
import org.bytesoft.transaction.remote.RemoteCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.FatalBeanException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.SingletonBeanRegistry;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.config.ReferenceConfig;
import com.alibaba.dubbo.config.ServiceConfig;

public class TransactionParticipantRegistrant
		implements SmartInitializingSingleton, TransactionEndpointAware, BeanFactoryAware {
	static final Logger logger = LoggerFactory.getLogger(TransactionParticipantRegistrant.class);

	private BeanFactory beanFactory;
	private String endpoint;

	public void afterSingletonsInstantiated() {
		TransactionCoordinator transactionCoordinator = this.beanFactory.getBean(TransactionCoordinator.class);
		TransactionBeanRegistry beanRegistry = this.beanFactory.getBean(TransactionBeanRegistry.class);

		if (transactionCoordinator == null) {
			throw new FatalBeanException("No configuration of class org.bytesoft.bytejta.TransactionCoordinator was found.");
		} else if (beanRegistry == null) {
			throw new FatalBeanException(
					"No configuration of class org.bytesoft.bytejta.supports.dubbo.TransactionBeanRegistry was found.");
		}

		this.initializeForProvider(transactionCoordinator);
		this.initializeForConsumer(beanRegistry);
	}

	public void initializeForProvider(RemoteCoordinator reference) throws BeansException {
		SingletonBeanRegistry registry = (SingletonBeanRegistry) this.beanFactory;
		ServiceConfig<RemoteCoordinator> globalServiceConfig = new ServiceConfig<RemoteCoordinator>();
		globalServiceConfig.setInterface(RemoteCoordinator.class);
		globalServiceConfig.setRef(reference);
		globalServiceConfig.setCluster("failfast");
		globalServiceConfig.setLoadbalance("bytejta");
		globalServiceConfig.setFilter("bytejta");
		globalServiceConfig.setGroup("org-bytesoft-bytejta");
		globalServiceConfig.setRetries(-1);
		globalServiceConfig.setTimeout(15000);

		ServiceConfig<RemoteCoordinator> applicationServiceConfig = new ServiceConfig<RemoteCoordinator>();
		applicationServiceConfig.setInterface(RemoteCoordinator.class);
		applicationServiceConfig.setRef(reference);
		applicationServiceConfig.setCluster("failfast");
		applicationServiceConfig.setLoadbalance("bytejta");
		applicationServiceConfig.setFilter("bytejta");
		applicationServiceConfig.setGroup(CommonUtils.getApplication(this.endpoint));
		applicationServiceConfig.setRetries(-1);
		applicationServiceConfig.setTimeout(15000);

		try {
			com.alibaba.dubbo.config.ApplicationConfig applicationConfig = //
					this.beanFactory.getBean(com.alibaba.dubbo.config.ApplicationConfig.class);
			globalServiceConfig.setApplication(applicationConfig);
			applicationServiceConfig.setApplication(applicationConfig);
		} catch (NoSuchBeanDefinitionException error) {
			logger.warn("No configuration of class com.alibaba.dubbo.config.ApplicationConfig was found.");
		}

		try {
			com.alibaba.dubbo.config.RegistryConfig registryConfig = //
					this.beanFactory.getBean(com.alibaba.dubbo.config.RegistryConfig.class);
			if (registryConfig != null) {
				globalServiceConfig.setRegistry(registryConfig);
				applicationServiceConfig.setRegistry(registryConfig);
			}
		} catch (NoSuchBeanDefinitionException error) {
			logger.warn("No configuration of class com.alibaba.dubbo.config.RegistryConfig was found.");
		}

		try {
			com.alibaba.dubbo.config.ProtocolConfig protocolConfig = //
					this.beanFactory.getBean(com.alibaba.dubbo.config.ProtocolConfig.class);
			globalServiceConfig.setProtocol(protocolConfig);
			applicationServiceConfig.setProtocol(protocolConfig);
		} catch (NoSuchBeanDefinitionException error) {
			logger.warn("No configuration of class com.alibaba.dubbo.config.ProtocolConfig was found.");
		}

		globalServiceConfig.export();
		applicationServiceConfig.export();

		String globalSkeletonBeanId = String.format("skeleton@%s", RemoteCoordinator.class.getName());
		registry.registerSingleton(globalSkeletonBeanId, globalServiceConfig);

		String applicationSkeletonBeanId = //
				String.format("%s@%s", CommonUtils.getApplication(this.endpoint), RemoteCoordinator.class.getName());
		registry.registerSingleton(applicationSkeletonBeanId, applicationServiceConfig);
	}

	public void initializeForConsumer(TransactionBeanRegistry beanRegistry) throws BeansException {
		SingletonBeanRegistry registry = (SingletonBeanRegistry) this.beanFactory;
		ReferenceConfig<RemoteCoordinator> referenceConfig = new ReferenceConfig<RemoteCoordinator>();
		referenceConfig.setInterface(RemoteCoordinator.class);
		referenceConfig.setTimeout(15000);
		referenceConfig.setCluster("failfast");
		referenceConfig.setLoadbalance("bytejta");
		referenceConfig.setFilter("bytejta");
		referenceConfig.setGroup("org-bytesoft-bytejta");
		referenceConfig.setCheck(false);
		referenceConfig.setRetries(-1);
		referenceConfig.setScope(Constants.SCOPE_REMOTE);

		try {
			com.alibaba.dubbo.config.ApplicationConfig applicationConfig = //
					this.beanFactory.getBean(com.alibaba.dubbo.config.ApplicationConfig.class);
			referenceConfig.setApplication(applicationConfig);
		} catch (NoSuchBeanDefinitionException error) {
			logger.warn("No configuration of class com.alibaba.dubbo.config.ApplicationConfig was found.");
		}

		try {
			com.alibaba.dubbo.config.RegistryConfig registryConfig = //
					this.beanFactory.getBean(com.alibaba.dubbo.config.RegistryConfig.class);
			if (registryConfig != null) {
				referenceConfig.setRegistry(registryConfig);
			}
		} catch (NoSuchBeanDefinitionException error) {
			logger.warn("No configuration of class com.alibaba.dubbo.config.RegistryConfig was found.");
		}

		try {
			com.alibaba.dubbo.config.ProtocolConfig protocolConfig = //
					this.beanFactory.getBean(com.alibaba.dubbo.config.ProtocolConfig.class);
			referenceConfig.setProtocol(protocolConfig.getName());
		} catch (NoSuchBeanDefinitionException error) {
			logger.warn("No configuration of class com.alibaba.dubbo.config.ProtocolConfig was found.");
		}

		RemoteCoordinator globalCoordinator = referenceConfig.get();
		beanRegistry.setConsumeCoordinator(globalCoordinator);

		String stubBeanId = String.format("stub@%s", RemoteCoordinator.class.getName());
		registry.registerSingleton(stubBeanId, globalCoordinator);
	}

	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
	}

	public String getEndpoint() {
		return this.endpoint;
	}

	public void setEndpoint(String identifier) {
		this.endpoint = identifier;
	}

}
