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

import javax.transaction.UserTransaction;

import org.bytesoft.bytejta.supports.resource.properties.ConnectorResourcePropertySourceFactory;
import org.bytesoft.transaction.TransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.TransactionManagementConfigurer;
import org.springframework.transaction.jta.JtaTransactionManager;

@PropertySource(value = "bytejta:connector.config", factory = ConnectorResourcePropertySourceFactory.class)
@ImportResource({ "classpath:bytejta-supports-dubbo.xml" })
@EnableAspectJAutoProxy(proxyTargetClass = true)
@EnableAutoConfiguration(exclude = { DataSourceAutoConfiguration.class })
@EnableTransactionManagement
public class DubboSupportConfiguration implements TransactionManagementConfigurer, ApplicationContextAware, EnvironmentAware {
	static final Logger logger = LoggerFactory.getLogger(DubboSupportConfiguration.class);

	private Environment environment;
	private ApplicationContext applicationContext;

	public PlatformTransactionManager annotationDrivenTransactionManager() {
		JtaTransactionManager jtaTransactionManager = new JtaTransactionManager();
		jtaTransactionManager.setTransactionManager(this.applicationContext.getBean(TransactionManager.class));
		jtaTransactionManager.setUserTransaction(this.applicationContext.getBean(UserTransaction.class));
		return jtaTransactionManager;
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
