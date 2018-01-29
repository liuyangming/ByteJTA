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
package org.bytesoft.bytejta.supports.springcloud.hystrix;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;

import com.netflix.hystrix.HystrixCommand.Setter;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;

import feign.InvocationHandlerFactory.MethodHandler;

public class TransactionHystrixBeanPostProcessor implements BeanPostProcessor {
	static final String HYSTRIX_COMMAND_NAME = "TransactionHystrixInvocationHandler#invoke(Thread,Method,Object[])";
	static final String HYSTRIX_INVOKER_NAME = "invoke";

	static final String HYSTRIX_FIELD_DISPATH = "dispatch";
	static final String HYSTRIX_FIELD_SETTERS = "setterMethodMap";
	static final String HYSTRIX_SETTER_GRPKEY = "groupKey";
	static final String HYSTRIX_CLAZZ_NAME = "feign.hystrix.HystrixInvocationHandler";

	public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
		return bean;
	}

	@SuppressWarnings("unchecked")
	public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
		if (Proxy.isProxyClass(bean.getClass()) == false) {
			return bean;
		}

		InvocationHandler handler = Proxy.getInvocationHandler(bean);
		if (StringUtils.equals(HYSTRIX_CLAZZ_NAME, handler.getClass().getName()) == false) {
			return bean;
		}

		TransactionHystrixFeignHandler feignHandler = new TransactionHystrixFeignHandler();
		feignHandler.setDelegate(handler);

		Class<?> clazz = bean.getClass();
		Class<?>[] interfaces = clazz.getInterfaces();
		ClassLoader loader = clazz.getClassLoader();

		try {
			Field dispatchField = handler.getClass().getDeclaredField(HYSTRIX_FIELD_DISPATH);
			dispatchField.setAccessible(true);
			Map<Method, MethodHandler> dispatch = (Map<Method, MethodHandler>) dispatchField.get(handler);

			Field setterField = handler.getClass().getDeclaredField(HYSTRIX_FIELD_SETTERS);
			setterField.setAccessible(true);
			Map<Method, Setter> setterMap = (Map<Method, Setter>) setterField.get(handler);

			Field groupKeyField = Setter.class.getDeclaredField(HYSTRIX_SETTER_GRPKEY);
			groupKeyField.setAccessible(true);

			HystrixCommandGroupKey hystrixCommandGroupKey = null;
			for (Iterator<Map.Entry<Method, Setter>> itr = setterMap.entrySet().iterator(); hystrixCommandGroupKey == null
					&& itr.hasNext();) {
				Map.Entry<Method, Setter> entry = itr.next();
				Setter setter = entry.getValue();

				hystrixCommandGroupKey = setter == null ? hystrixCommandGroupKey
						: (HystrixCommandGroupKey) groupKeyField.get(setter);
			}

			final String commandGroupKeyName = hystrixCommandGroupKey == null ? null : hystrixCommandGroupKey.name();
			HystrixCommandGroupKey groupKey = new HystrixCommandGroupKey() {
				public String name() {
					return commandGroupKeyName;
				}
			};

			HystrixCommandKey commandKey = new HystrixCommandKey() {
				public String name() {
					return HYSTRIX_COMMAND_NAME;
				}
			};

			Setter setter = Setter.withGroupKey(groupKey).andCommandKey(commandKey);

			Method key = TransactionHystrixInvocationHandler.class.getDeclaredMethod(HYSTRIX_INVOKER_NAME,
					new Class<?>[] { Thread.class, Method.class, Object[].class });
			setterMap.put(key, setter);
			dispatch.put(key, new TransactionHystrixMethodHandler(dispatch));

		} catch (Exception ex) {
			throw new IllegalStateException("Error occurred!");
		}

		return Proxy.newProxyInstance(loader, interfaces, feignHandler);
	}
}
