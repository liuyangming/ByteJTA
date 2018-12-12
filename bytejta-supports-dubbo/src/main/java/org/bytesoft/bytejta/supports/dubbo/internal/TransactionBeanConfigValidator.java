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
package org.bytesoft.bytejta.supports.dubbo.internal;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.FatalBeanException;
import org.springframework.beans.MutablePropertyValues;
import org.springframework.beans.PropertyValue;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;

import com.alibaba.dubbo.config.ProtocolConfig;
import com.alibaba.dubbo.config.spring.ServiceBean;
import com.alibaba.dubbo.remoting.RemotingException;

public class TransactionBeanConfigValidator implements BeanPostProcessor, BeanFactoryPostProcessor {
	static final Logger logger = LoggerFactory.getLogger(TransactionBeanConfigValidator.class);

	public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
		return bean;
	}

	public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
		if (ProtocolConfig.class.isInstance(bean)) {
			this.validateProtocolConfig(beanName, (ProtocolConfig) bean);
		} else if (ServiceBean.class.isInstance(bean)) {
			this.validateServiceBean(beanName, (ServiceBean<?>) bean);
		}

		return bean;
	}

	private void validateProtocolConfig(String beanName, ProtocolConfig protocolConfig) throws BeansException {
		Integer port = protocolConfig.getPort();
		if (port == null) {
			throw new FatalBeanException(
					"The value of the attribute 'port' (<dubbo:protocol port='...' />) must be explicitly specified.");
		} else if (port.intValue() <= 0) {
			throw new FatalBeanException(
					"The value of the attribute 'port' (<dubbo:protocol port='...' />) must be explicitly specified and not equal to -1.");
		}
	}

	private void validateServiceBean(String beanName, ServiceBean<?> serviceBean) throws BeansException {
		Integer retries = serviceBean.getRetries();
		String loadbalance = serviceBean.getLoadbalance();
		String cluster = serviceBean.getCluster();
		String filter = serviceBean.getFilter();
		String group = serviceBean.getGroup();

		if (StringUtils.isBlank(group)) {
			return;
		} else if (StringUtils.equalsIgnoreCase("x-bytejta", group) == false
				&& StringUtils.lowerCase(group).startsWith("x-bytejta-") == false) {
			return;
		}

		String[] filterArray = filter == null ? new String[0] : filter.split("\\s*,\\s*");

		if (retries != null && retries.intValue() >= 0) {
			throw new FatalBeanException(String.format("The value of attr 'retries'(beanId= %s) should be '-1'.", beanName));
		} else if (loadbalance == null || StringUtils.equals("bytejta", loadbalance) == false) {
			throw new FatalBeanException(
					String.format("The value of attr 'loadbalance'(beanId= %s) should be 'bytejta'.", beanName));
		} else if (cluster == null || StringUtils.equals("failfast", cluster) == false) {
			throw new FatalBeanException(
					String.format("The value of attribute 'cluster' (beanId= %s) must be 'failfast'.", beanName));
		} else if (filterArray.length == 0) {
			throw new FatalBeanException(String.format("The value of attr 'filter'(beanId= %s) must be null.", beanName));
		} else if (StringUtils.equalsIgnoreCase(filterArray[0], "bytejta") == false) {
			throw new FatalBeanException(
					String.format("The first value of attr 'filter'(beanId= %s) should be 'bytejta'.", beanName));
		}
	}

	private void validateReferenceConfig(String beanName, BeanDefinition beanDef) throws BeansException {
		MutablePropertyValues mpv = beanDef.getPropertyValues();
		PropertyValue group = mpv.getPropertyValue("group");
		PropertyValue retries = mpv.getPropertyValue("retries");
		PropertyValue loadbalance = mpv.getPropertyValue("loadbalance");
		PropertyValue cluster = mpv.getPropertyValue("cluster");
		PropertyValue filter = mpv.getPropertyValue("filter");

		String filterValue = filter == null || filter.getValue() == null ? null : String.valueOf(filter.getValue());
		String[] filterArray = filter == null ? new String[0] : filterValue.split("\\s*,\\s*");

		if (group == null || group.getValue() == null //
				|| ("x-bytejta".equals(group.getValue())
						|| String.valueOf(group.getValue()).startsWith("x-bytejta-")) == false) {
			throw new FatalBeanException(String.format(
					"The value of attr 'group'(beanId= %s) should be 'x-bytejta' or starts with 'x-bytejta-'.", beanName));
		} else if (retries == null || retries.getValue() == null || "-1".equals(retries.getValue()) == false) {
			throw new FatalBeanException(String.format("The value of attr 'retries'(beanId= %s) should be '-1'.", beanName));
		} else if (loadbalance == null || loadbalance.getValue() == null || "bytejta".equals(loadbalance.getValue()) == false) {
			throw new FatalBeanException(
					String.format("The value of attr 'loadbalance'(beanId= %s) should be 'bytejta'.", beanName));
		} else if (cluster == null || cluster.getValue() == null || "failfast".equals(cluster.getValue()) == false) {
			throw new FatalBeanException(
					String.format("The value of attribute 'cluster' (beanId= %s) must be 'failfast'.", beanName));
		} else if (filter == null || filter.getValue() == null || String.class.isInstance(filter.getValue()) == false) {
			throw new FatalBeanException(String
					.format("The value of attr 'filter'(beanId= %s) must be java.lang.String and cannot be null.", beanName));
		} else if (StringUtils.equalsIgnoreCase(filterArray[filterArray.length - 1], "bytejta") == false) {
			throw new FatalBeanException(
					String.format("The last value of attr 'filter'(beanId= %s) should be 'bytejta'.", beanName));
		}

		PropertyValue pv = mpv.getPropertyValue("interface");
		String clazzName = String.valueOf(pv.getValue());
		ClassLoader cl = Thread.currentThread().getContextClassLoader();

		Class<?> clazz = null;
		try {
			clazz = cl.loadClass(clazzName);
		} catch (Exception ex) {
			throw new FatalBeanException(String.format("Cannot load class %s.", clazzName));
		}

		Method[] methodArray = clazz.getMethods();
		for (int i = 0; i < methodArray.length; i++) {
			Method method = methodArray[i];
			boolean declared = false;
			Class<?>[] exceptionTypeArray = method.getExceptionTypes();
			for (int j = 0; j < exceptionTypeArray.length; j++) {
				Class<?> exceptionType = exceptionTypeArray[j];
				if (RemotingException.class.isAssignableFrom(exceptionType)) {
					declared = true;
					break;
				}
			}

			if (declared == false) {
				logger.warn("The remote call method({}) should be declared to throw a remote exception: {}!", method,
						RemotingException.class.getName());
			}

		}

	}

	public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
		ClassLoader cl = Thread.currentThread().getContextClassLoader();

		Map<String, BeanDefinition> referenceMap = new HashMap<String, BeanDefinition>();

		Map<String, Class<?>> clazzMap = new HashMap<String, Class<?>>();

		String[] beanNameArray = beanFactory.getBeanDefinitionNames();
		for (int i = 0; i < beanNameArray.length; i++) {
			String beanName = beanNameArray[i];
			BeanDefinition beanDef = beanFactory.getBeanDefinition(beanName);
			String beanClassName = beanDef.getBeanClassName();

			if (StringUtils.equals(com.alibaba.dubbo.config.spring.ReferenceBean.class.getName(), beanClassName) == false) {
				continue;
			}

			Class<?> beanClass = null;
			try {
				beanClass = cl.loadClass(beanClassName);
			} catch (Exception ex) {
				logger.debug("Cannot load class {}, beanId= {}!", beanClassName, beanName, ex);
				continue;
			}

			clazzMap.put(beanClassName, beanClass);
		}

		for (int i = 0; i < beanNameArray.length; i++) {
			String beanName = beanNameArray[i];
			BeanDefinition beanDef = beanFactory.getBeanDefinition(beanName);
			String beanClassName = beanDef.getBeanClassName();

			Class<?> beanClass = clazzMap.get(beanClassName);

			if (com.alibaba.dubbo.config.spring.ReferenceBean.class.equals(beanClass)) {
				MutablePropertyValues mpv = beanDef.getPropertyValues();
				PropertyValue group = mpv.getPropertyValue("group");
				if (group == null || group.getValue() == null) {
					continue;
				}

				String groupValue = String.valueOf(group.getValue());
				if (StringUtils.equalsIgnoreCase("x-bytejta", groupValue)) {
					referenceMap.put(beanName, beanDef);
				} else if (StringUtils.lowerCase(groupValue).startsWith("x-bytejta-")) {
					referenceMap.put(beanName, beanDef);
				}
			}
		}

		for (Iterator<Map.Entry<String, BeanDefinition>> itr = referenceMap.entrySet().iterator(); itr.hasNext();) {
			Map.Entry<String, BeanDefinition> entry = itr.next();
			this.validateReferenceConfig(entry.getKey(), entry.getValue());
		}
	}

}
