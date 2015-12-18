/**
 * Copyright 2014-2015 yangming.liu<liuyangming@gmail.com>.
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
package org.bytesoft.transaction.supports.spring;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

public class ApplicationContextUtils implements ApplicationContextAware {
	private static final ApplicationContextUtils instance = new ApplicationContextUtils();
	private ApplicationContext context;

	private ApplicationContextUtils() {
	}

	public static ApplicationContextUtils getInstance() {
		return instance;
	}

	public static ApplicationContext getCurrentApplicationContext() {
		return instance.getContext();
	}

	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		instance.setContext(applicationContext);
	}

	public ApplicationContext getApplicationContext() {
		return instance.getContext();
	}

	public ApplicationContext getContext() {
		return context;
	}

	public void setContext(ApplicationContext context) {
		this.context = context;
	}

}
