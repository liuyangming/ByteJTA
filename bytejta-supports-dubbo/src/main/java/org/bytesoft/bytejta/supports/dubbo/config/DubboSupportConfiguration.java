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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.transaction.UserTransaction;

import org.bytesoft.bytejta.TransactionCoordinator;
import org.bytesoft.transaction.TransactionManager;
import org.bytesoft.transaction.remote.RemoteCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.ImportResource;
import org.springframework.core.env.Environment;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.TransactionManagementConfigurer;
import org.springframework.transaction.jta.JtaTransactionManager;

import com.alibaba.dubbo.config.ApplicationConfig;
import com.alibaba.dubbo.config.ProtocolConfig;
import com.alibaba.dubbo.config.ReferenceConfig;
import com.alibaba.dubbo.config.RegistryConfig;
import com.alibaba.dubbo.config.ServiceConfig;

@ImportResource({ "classpath:bytejta-supports-dubbo.xml" })
@EnableAspectJAutoProxy(proxyTargetClass = true)
@EnableTransactionManagement
public class DubboSupportConfiguration implements TransactionManagementConfigurer, ApplicationContextAware, EnvironmentAware {
	static final Logger logger = LoggerFactory.getLogger(DubboSupportConfiguration.class);

	static final String CONSTANTS_SKEN_ID = "skeleton@org.bytesoft.transaction.remote.RemoteCoordinator";
	static final String CONSTANTS_STUB_ID = "stub@org.bytesoft.transaction.remote.RemoteCoordinator";

	static final int CONSTANTS_TIMEOUT_MILLIS = 6000;
	static final String CONSTANTS_TIMEOUT_KEY = "org.bytesoft.bytejta.timeout";

	private Environment environment;
	private ApplicationContext applicationContext;

	public PlatformTransactionManager annotationDrivenTransactionManager() {
		JtaTransactionManager jtaTransactionManager = new JtaTransactionManager();
		jtaTransactionManager.setTransactionManager(this.applicationContext.getBean(TransactionManager.class));
		jtaTransactionManager.setUserTransaction(this.applicationContext.getBean(UserTransaction.class));
		return jtaTransactionManager;
	}

	@Bean(CONSTANTS_SKEN_ID)
	public ServiceConfig<RemoteCoordinator> skeletonRemoteCoordinator(
			@Autowired TransactionCoordinator transactionCoordinator) {
		int timeout = this.environment.getProperty(CONSTANTS_TIMEOUT_KEY, Integer.TYPE, CONSTANTS_TIMEOUT_MILLIS);

		ServiceConfig<RemoteCoordinator> serviceConfig = new ServiceConfig<RemoteCoordinator>();
		serviceConfig.setInterface(RemoteCoordinator.class);
		serviceConfig.setRef(transactionCoordinator);
		serviceConfig.setCluster("failfast");
		serviceConfig.setLoadbalance("bytejta");
		serviceConfig.setFilter("bytejta");
		serviceConfig.setGroup("bytejta");
		serviceConfig.setRetries(0);
		serviceConfig.setTimeout(timeout);

		try {
			serviceConfig.setApplication(this.applicationContext.getBean(ApplicationConfig.class));
		} catch (NoUniqueBeanDefinitionException ex) {
			throw ex;
		} catch (NoSuchBeanDefinitionException ex) {
			logger.debug("Error occurred while creating ServiceConfig!", ex);
		} catch (BeansException ex) {
			throw new RuntimeException("Error occurred while creating ServiceConfig!", ex);
		}

		try {
			Map<String, ProtocolConfig> protocolMap = this.applicationContext.getBeansOfType(ProtocolConfig.class);
			List<ProtocolConfig> protocols = new ArrayList<ProtocolConfig>();
			protocols.addAll(protocolMap.values());

			serviceConfig.setProtocols(protocols);
		} catch (NoSuchBeanDefinitionException ex) {
			logger.debug("Error occurred while creating ServiceConfig!", ex);
		} catch (BeansException ex) {
			throw new RuntimeException("Error occurred while creating ServiceConfig!", ex);
		}

		try {
			Map<String, RegistryConfig> registryMap = this.applicationContext.getBeansOfType(RegistryConfig.class);
			List<RegistryConfig> registries = new ArrayList<RegistryConfig>();
			registries.addAll(registryMap.values());

			serviceConfig.setRegistries(registries);
		} catch (NoSuchBeanDefinitionException ex) {
			logger.debug("Error occurred while creating ServiceConfig!", ex);
		} catch (BeansException ex) {
			throw new RuntimeException("Error occurred while creating ServiceConfig!", ex);
		}

		serviceConfig.export();
		return serviceConfig;
	}

	@Bean(CONSTANTS_STUB_ID)
	public Object stubRemoteCoordinator() {
		int timeout = this.environment.getProperty(CONSTANTS_TIMEOUT_KEY, Integer.TYPE, CONSTANTS_TIMEOUT_MILLIS);

		ReferenceConfig<RemoteCoordinator> referenceConfig = new ReferenceConfig<RemoteCoordinator>();
		referenceConfig.setInterface(RemoteCoordinator.class);
		referenceConfig.setCluster("failfast");
		referenceConfig.setLoadbalance("bytejta");
		referenceConfig.setFilter("bytejta");
		referenceConfig.setGroup("bytejta");
		referenceConfig.setScope("remote");
		referenceConfig.setRetries(0);
		referenceConfig.setTimeout(timeout);
		referenceConfig.setCheck(false);

		try {
			referenceConfig.setApplication(this.applicationContext.getBean(ApplicationConfig.class));
		} catch (NoUniqueBeanDefinitionException ex) {
			throw ex;
		} catch (NoSuchBeanDefinitionException ex) {
			logger.debug("Error occurred while creating ReferenceConfig!", ex);
		} catch (BeansException ex) {
			throw new RuntimeException("Error occurred while creating ReferenceConfig!", ex);
		}

		try {
			Map<String, RegistryConfig> registryMap = this.applicationContext.getBeansOfType(RegistryConfig.class);
			List<RegistryConfig> registries = new ArrayList<RegistryConfig>();
			registries.addAll(registryMap.values());

			referenceConfig.setRegistries(registries);
		} catch (NoSuchBeanDefinitionException ex) {
			logger.debug("Error occurred while creating ReferenceConfig!", ex);
		} catch (BeansException ex) {
			throw new RuntimeException("Error occurred while creating ReferenceConfig!", ex);
		}

		return referenceConfig;
	}

	public Environment getEnvironment() {
		return environment;
	}

	public void setEnvironment(Environment environment) {
		this.environment = environment;
	}

	public ApplicationContext getApplicationContext() {
		return applicationContext;
	}

	public void setApplicationContext(ApplicationContext applicationContext) {
		this.applicationContext = applicationContext;
	}

}
