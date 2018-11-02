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
package org.bytesoft.bytejta.supports.spring;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.Iterator;
import java.util.Map;

import javax.jms.XAConnectionFactory;
import javax.resource.spi.ManagedConnectionFactory;
import javax.sql.XADataSource;
import javax.transaction.TransactionManager;

import org.bytesoft.bytejta.supports.jdbc.LocalXADataSource;
import org.bytesoft.bytejta.supports.resource.ManagedConnectionFactoryHandler;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

public class ManagedConnectionFactoryPostProcessor
		implements BeanPostProcessor, SmartInitializingSingleton, ApplicationContextAware {
	static String BEAN_TRANSACTION_MANAGER = "transactionManager";

	private ApplicationContext applicationContext;

	public void afterSingletonsInstantiated() {
		Map<String, LocalXADataSource> beanMap = this.applicationContext.getBeansOfType(LocalXADataSource.class);
		Iterator<Map.Entry<String, LocalXADataSource>> iterator = beanMap == null ? null : beanMap.entrySet().iterator();
		while (iterator != null && iterator.hasNext()) {
			Map.Entry<String, LocalXADataSource> entry = iterator.next();
			LocalXADataSource bean = entry.getValue();
			this.initializeTransactionManagerIfNecessary(bean);
		}
	}

	private void initializeTransactionManagerIfNecessary(LocalXADataSource target) {
		if (target.getTransactionManager() == null) {
			TransactionManager transactionManager = //
					(TransactionManager) this.applicationContext.getBean(BEAN_TRANSACTION_MANAGER);
			target.setTransactionManager(transactionManager);
		} // end-if (target.getTransactionManager() == null)
	}

	public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
		return this.wrapManagedConnectionFactoryIfNecessary(bean, beanName);
	}

	public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
		return this.wrapManagedConnectionFactoryIfNecessary(bean, beanName);
	}

	private Object wrapManagedConnectionFactoryIfNecessary(Object bean, String beanName) throws BeansException {
		Class<?> clazz = bean.getClass();
		ClassLoader cl = clazz.getClassLoader();

		Class<?>[] interfaces = clazz.getInterfaces();

		if (this.hasAlreadyBeenWrappedBySelf(bean)) /* wrapped */ {
			return bean;
		} else if (LocalXADataSource.class.isInstance(bean)) {
			LocalXADataSource target = (LocalXADataSource) bean;
			this.initializeTransactionManagerIfNecessary(target);
			return bean;
		} else if (XADataSource.class.isInstance(bean)) {
			ManagedConnectionFactoryHandler interceptor = new ManagedConnectionFactoryHandler(bean);
			interceptor.setIdentifier(beanName);
			return Proxy.newProxyInstance(cl, interfaces, interceptor);
		} else if (XAConnectionFactory.class.isInstance(bean)) {
			ManagedConnectionFactoryHandler interceptor = new ManagedConnectionFactoryHandler(bean);
			interceptor.setIdentifier(beanName);
			return Proxy.newProxyInstance(cl, interfaces, interceptor);
		} else if (ManagedConnectionFactory.class.isInstance(bean)) {
			ManagedConnectionFactoryHandler interceptor = new ManagedConnectionFactoryHandler(bean);
			interceptor.setIdentifier(beanName);
			return Proxy.newProxyInstance(cl, interfaces, interceptor);
		} else {
			return bean;
		}
	}

	private boolean hasAlreadyBeenWrappedBySelf(Object bean) {
		if (Proxy.isProxyClass(bean.getClass()) == false) {
			return false;
		} else {
			InvocationHandler handler = Proxy.getInvocationHandler(bean);
			return ManagedConnectionFactoryHandler.class.isInstance(handler);
		}
	}

	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

}
