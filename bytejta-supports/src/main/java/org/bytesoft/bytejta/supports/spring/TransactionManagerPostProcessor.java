/**
 * Copyright 2014-2017 yangming.liu<bytefox@126.com>.
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
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.FatalBeanException;
import org.springframework.beans.MutablePropertyValues;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.RuntimeBeanReference;

public class TransactionManagerPostProcessor implements BeanFactoryPostProcessor {
	static final Logger logger = LoggerFactory.getLogger(TransactionManagerPostProcessor.class);

	public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
		ClassLoader cl = Thread.currentThread().getContextClassLoader();

		String transactionManagerBeanName = null;
		String userTransactionBeanName = null;
		String jtaTransactionManagerBeanName = null;
		final List<String> beanNameList = new ArrayList<String>();

		String[] beanNameArray = beanFactory.getBeanDefinitionNames();
		for (int i = 0; i < beanNameArray.length; i++) {
			String beanName = beanNameArray[i];
			BeanDefinition beanDef = beanFactory.getBeanDefinition(beanName);
			String beanClassName = beanDef.getBeanClassName();

			Class<?> beanClass = null;
			try {
				beanClass = cl.loadClass(beanClassName);
			} catch (Exception ex) {
				logger.debug("Cannot load class {}, beanId= {}!", beanClassName, beanName, ex);
				continue;
			}

			if (org.bytesoft.bytejta.supports.jdbc.LocalXADataSource.class.equals(beanClass)) {
				beanNameList.add(beanName);
			} else if (org.bytesoft.bytejta.UserTransactionImpl.class.equals(beanClass)) {
				beanNameList.add(beanName);
				userTransactionBeanName = beanName;
			} else if (org.bytesoft.bytejta.TransactionManagerImpl.class.equals(beanClass)) {
				transactionManagerBeanName = beanName;
			} else if (org.springframework.transaction.jta.JtaTransactionManager.class.equals(beanClass)) {
				beanNameList.add(beanName);
				jtaTransactionManagerBeanName = beanName;
			}

		}

		if (transactionManagerBeanName == null) {
			throw new FatalBeanException("No configuration of class org.bytesoft.bytetcc.TransactionManagerImpl was found.");
		}

		if (jtaTransactionManagerBeanName == null) {
			throw new FatalBeanException(
					"No configuration of org.springframework.transaction.jta.JtaTransactionManager was found.");
		}

		for (int i = 0; i < beanNameList.size(); i++) {
			String beanName = beanNameList.get(i);
			BeanDefinition beanDef = beanFactory.getBeanDefinition(beanName);
			MutablePropertyValues mpv = beanDef.getPropertyValues();
			RuntimeBeanReference beanRef = new RuntimeBeanReference(transactionManagerBeanName);
			mpv.addPropertyValue("transactionManager", beanRef);
		}

		BeanDefinition jtaTransactionManagerBeanDef = beanFactory.getBeanDefinition(jtaTransactionManagerBeanName);
		MutablePropertyValues jtaTransactionManagerMPV = jtaTransactionManagerBeanDef.getPropertyValues();
		RuntimeBeanReference userTransactionBeanRef = new RuntimeBeanReference(userTransactionBeanName);
		jtaTransactionManagerMPV.addPropertyValue("userTransaction", userTransactionBeanRef);
	}

}
