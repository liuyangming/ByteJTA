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
package org.bytesoft.bytejta.supports.spring;

import org.apache.commons.lang3.StringUtils;
import org.bytesoft.transaction.aware.TransactionDebuggable;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;

public class TransactionDebuggablePostProcessor implements BeanPostProcessor, EnvironmentAware {
	static final String KEY_DEBUGGABLE = "org.bytesoft.bytejta.debuggable";

	private Environment environment;

	public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
		this.setDebuggableIfNecessary(bean);
		return bean;
	}

	public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
		this.setDebuggableIfNecessary(bean);
		return bean;
	}

	private void setDebuggableIfNecessary(Object bean) {
		if (TransactionDebuggable.class.isInstance(bean)) {
			String debuggable = this.environment.getProperty(KEY_DEBUGGABLE);
			TransactionDebuggable target = (TransactionDebuggable) bean;
			target.setDebuggingEnabled(StringUtils.equalsIgnoreCase(Boolean.TRUE.toString(), debuggable));
		}
	}

	public void setEnvironment(Environment environment) {
		this.environment = environment;
	}

}
