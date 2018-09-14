/**
 * Copyright 2014-2016 yangming.liu<liuyangming@gmail.com>.
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.bytesoft.transaction.TransactionBeanFactory;
import org.bytesoft.transaction.aware.TransactionBeanFactoryAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.FatalBeanException;
import org.springframework.beans.MutablePropertyValues;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

public class TransactionBeanFactoryPostProcessor
		implements BeanFactoryPostProcessor, BeanPostProcessor, SmartInitializingSingleton, ApplicationContextAware {
	static final Logger logger = LoggerFactory.getLogger(TransactionBeanFactoryPostProcessor.class);

	private ApplicationContext applicationContext;

	public void afterSingletonsInstantiated() {
		Map<String, TransactionBeanFactoryAware> beanMap = //
				this.applicationContext.getBeansOfType(TransactionBeanFactoryAware.class);
		Iterator<Map.Entry<String, TransactionBeanFactoryAware>> iterator = //
				(beanMap == null) ? null : beanMap.entrySet().iterator();
		while (iterator != null && iterator.hasNext()) {
			Map.Entry<String, TransactionBeanFactoryAware> entry = iterator.next();
			TransactionBeanFactoryAware bean = entry.getValue();
			this.initializeTransactionBeanFactoryIfNecessary(bean);
		}
	}

	public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
		return bean;
	}

	public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
		if (TransactionBeanFactoryAware.class.isInstance(bean)) {
			this.initializeTransactionBeanFactoryIfNecessary((TransactionBeanFactoryAware) bean);
		} // end-if (TransactionBeanFactoryAware.class.isInstance(bean))

		return bean;
	}

	private void initializeTransactionBeanFactoryIfNecessary(TransactionBeanFactoryAware aware) {
		if (aware.getBeanFactory() == null) {
			TransactionBeanFactory beanFactory = //
					this.applicationContext.getBean(TransactionBeanFactory.class);
			aware.setBeanFactory(beanFactory);
		} // end-if (aware.getBeanFactory() == null)
	}

	public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
		ClassLoader cl = Thread.currentThread().getContextClassLoader();

		String beanFactoryBeanId = null;
		List<BeanDefinition> beanFactoryAwareBeanIdList = new ArrayList<BeanDefinition>();
		String[] beanNameArray = beanFactory.getBeanDefinitionNames();
		for (int i = 0; i < beanNameArray.length; i++) {
			String beanName = beanNameArray[i];
			BeanDefinition beanDef = beanFactory.getBeanDefinition(beanName);
			String beanClassName = beanDef.getBeanClassName();

			Class<?> beanClass = null;
			try {
				beanClass = cl.loadClass(beanClassName);
			} catch (Exception ex) {
				continue;
			}

			if (TransactionBeanFactoryAware.class.isAssignableFrom(beanClass)) {
				beanFactoryAwareBeanIdList.add(beanDef);
			}

			if (TransactionBeanFactory.class.isAssignableFrom(beanClass)) {
				if (beanFactoryBeanId == null) {
					beanFactoryBeanId = beanName;
				} else {
					throw new FatalBeanException("Duplicated transaction-bean-factory defined.");
				}
			}

		}

		for (int i = 0; beanFactoryBeanId != null && i < beanFactoryAwareBeanIdList.size(); i++) {
			BeanDefinition beanDef = beanFactoryAwareBeanIdList.get(i);
			MutablePropertyValues mpv = beanDef.getPropertyValues();
			RuntimeBeanReference beanRef = new RuntimeBeanReference(beanFactoryBeanId);
			mpv.addPropertyValue(TransactionBeanFactoryAware.BEAN_FACTORY_FIELD_NAME, beanRef);
		}

	}

	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

}
