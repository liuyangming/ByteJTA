/**
 * Copyright 2014-2016 yangming.liu<bytefox@126.com>.
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
package org.bytesoft.bytejta.supports.dubbo;

import java.util.ArrayList;
import java.util.List;

import org.bytesoft.bytejta.supports.wire.RemoteCoordinator;
import org.bytesoft.common.utils.CommonUtils;
import org.bytesoft.transaction.aware.TransactionEndpointAware;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.alibaba.dubbo.config.ApplicationConfig;
import com.alibaba.dubbo.config.ProtocolConfig;
import com.alibaba.dubbo.config.ServiceConfig;

public class DubboEndpointPostProcessor implements BeanPostProcessor, SmartInitializingSingleton, ApplicationContextAware {
	static final String KEY_REMOTE_COORDINATOR = "skeleton@org.bytesoft.bytejta.supports.wire.RemoteCoordinator";

	private final List<TransactionEndpointAware> beanList = new ArrayList<TransactionEndpointAware>();
	private ApplicationContext applicationContext;

	@SuppressWarnings("unchecked")
	public void afterSingletonsInstantiated() {
		ApplicationConfig applicationConfig = null;
		try {
			applicationConfig = this.applicationContext.getBean(ApplicationConfig.class);
		} catch (NoSuchBeanDefinitionException ex) {
			ServiceConfig<RemoteCoordinator> serviceConfig = //
					(ServiceConfig<RemoteCoordinator>) this.applicationContext.getBean(KEY_REMOTE_COORDINATOR);
			applicationConfig = serviceConfig.getApplication();
		}

		ProtocolConfig protocolConfig = null;
		try {
			protocolConfig = this.applicationContext.getBean(ProtocolConfig.class);
		} catch (NoSuchBeanDefinitionException ex) {
			ServiceConfig<RemoteCoordinator> serviceConfig = //
					(ServiceConfig<RemoteCoordinator>) this.applicationContext.getBean(KEY_REMOTE_COORDINATOR);
			protocolConfig = serviceConfig.getProtocol();
		}

		if (applicationConfig == null) {
		}

		if (protocolConfig == null) {
		}

		String host = CommonUtils.getInetAddress();
		String name = applicationConfig.getName();
		String port = String.valueOf(protocolConfig.getPort());
		String identifier = String.format("%s:%s:%s", host, name, port);

		for (int i = 0; i < this.beanList.size(); i++) {
			TransactionEndpointAware aware = this.beanList.get(i);
			aware.setEndpoint(identifier);
		}
	}

	public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
		return bean;
	}

	public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
		if (TransactionEndpointAware.class.isInstance(bean)) {
			this.beanList.add((TransactionEndpointAware) bean);
		} // end-if (TransactionEndpointAware.class.isInstance(bean))

		return bean;
	}

	public ApplicationContext getApplicationContext() {
		return applicationContext;
	}

	public void setApplicationContext(ApplicationContext applicationContext) {
		this.applicationContext = applicationContext;
	}

}
