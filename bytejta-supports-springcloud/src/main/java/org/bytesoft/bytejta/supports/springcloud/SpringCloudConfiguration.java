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
package org.bytesoft.bytejta.supports.springcloud;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.bytesoft.bytejta.supports.springcloud.feign.TransactionClientRegistry;
import org.bytesoft.bytejta.supports.springcloud.feign.TransactionFeignContract;
import org.bytesoft.bytejta.supports.springcloud.feign.TransactionFeignDecoder;
import org.bytesoft.bytejta.supports.springcloud.feign.TransactionFeignErrorDecoder;
import org.bytesoft.bytejta.supports.springcloud.feign.TransactionFeignHandler;
import org.bytesoft.bytejta.supports.springcloud.feign.TransactionFeignInterceptor;
import org.bytesoft.bytejta.supports.springcloud.property.TransactionPropertySourceFactory;
import org.bytesoft.bytejta.supports.springcloud.web.TransactionHandlerInterceptor;
import org.bytesoft.bytejta.supports.springcloud.web.TransactionRequestInterceptor;
import org.bytesoft.transaction.aware.TransactionEndpointAware;
import org.springframework.beans.BeansException;
import org.springframework.beans.MutablePropertyValues;
import org.springframework.beans.PropertyValue;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.web.HttpMessageConverters;
import org.springframework.cloud.netflix.feign.support.ResponseEntityDecoder;
import org.springframework.cloud.netflix.feign.support.SpringDecoder;
import org.springframework.cloud.netflix.feign.support.SpringMvcContract;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import feign.Feign;
import feign.Feign.Builder;
import feign.InvocationHandlerFactory;
import feign.Target;
import feign.codec.ErrorDecoder;

@PropertySource(value = "bytejta:loadbalancer.config", factory = TransactionPropertySourceFactory.class)
@Configuration
public class SpringCloudConfiguration extends WebMvcConfigurerAdapter
		implements BeanFactoryPostProcessor, TransactionEndpointAware, EnvironmentAware, ApplicationContextAware {
	static final String CONSTANT_INCLUSIONS = "org.bytesoft.bytejta.feign.inclusions";
	static final String CONSTANT_EXCLUSIONS = "org.bytesoft.bytejta.feign.exclusions";
	static final String FEIGN_FACTORY_CLASS = "org.springframework.cloud.netflix.feign.FeignClientFactoryBean";

	private ApplicationContext applicationContext;
	private String identifier;
	private Environment environment;
	private transient final Set<String> transientClientSet = new HashSet<String>();

	@org.springframework.context.annotation.Primary
	@org.springframework.context.annotation.Bean
	public InvocationHandlerFactory transactionInvocationHandlerFactory() {
		return new InvocationHandlerFactory() {
			@SuppressWarnings("rawtypes")
			public InvocationHandler create(Target target, Map<Method, MethodHandler> dispatch) {
				TransactionFeignHandler handler = new TransactionFeignHandler();
				handler.setTarget(target);
				handler.setHandlers(dispatch);
				return handler;
			}
		};
	}

	@org.springframework.context.annotation.Primary
	@org.springframework.context.annotation.Bean
	public Builder transactionFeignBuilder(@Autowired InvocationHandlerFactory invocationHandlerFactory) {
		return Feign.builder().invocationHandlerFactory(invocationHandlerFactory);
	}

	@org.springframework.context.annotation.Bean
	public TransactionFeignInterceptor transactionFeignInterceptor() {
		TransactionFeignInterceptor interceptor = new TransactionFeignInterceptor();
		interceptor.setEndpoint(this.identifier);
		return interceptor;
	}

	@org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean(feign.Contract.class)
	@org.springframework.context.annotation.Bean
	public feign.Contract feignContract() {
		return new SpringMvcContract();
	}

	@org.springframework.context.annotation.Primary
	@org.springframework.context.annotation.Bean
	public feign.Contract transactionFeignContract(@Autowired feign.Contract contract) {
		return new TransactionFeignContract(contract);
	}

	@org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean(feign.codec.Decoder.class)
	@org.springframework.context.annotation.Bean
	public feign.codec.Decoder feignDecoder(@Autowired ObjectFactory<HttpMessageConverters> messageConverters) {
		return new ResponseEntityDecoder(new SpringDecoder(messageConverters));
	}

	@org.springframework.context.annotation.Primary
	@org.springframework.context.annotation.Bean
	public feign.codec.Decoder transactionFeignDecoder(@Autowired feign.codec.Decoder decoder) {
		return new TransactionFeignDecoder(decoder);
	}

	@org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean(feign.codec.ErrorDecoder.class)
	@org.springframework.context.annotation.Bean
	public feign.codec.ErrorDecoder errorDecoder() {
		return new ErrorDecoder.Default();
	}

	@org.springframework.context.annotation.Primary
	@org.springframework.context.annotation.Bean
	public feign.codec.ErrorDecoder transactionErrorDecoder(@Autowired feign.codec.ErrorDecoder decoder) {
		return new TransactionFeignErrorDecoder(decoder);
	}

	@org.springframework.context.annotation.Bean
	public TransactionHandlerInterceptor transactionHandlerInterceptor() {
		TransactionHandlerInterceptor interceptor = new TransactionHandlerInterceptor();
		interceptor.setEndpoint(this.identifier);
		// interceptor.setApplicationContext(this.applicationContext);
		return interceptor;
	}

	@org.springframework.context.annotation.Bean
	public TransactionRequestInterceptor transactionRequestInterceptor() {
		TransactionRequestInterceptor interceptor = new TransactionRequestInterceptor();
		interceptor.setEndpoint(this.identifier);
		return interceptor;
	}

	@org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean(ClientHttpRequestFactory.class)
	@org.springframework.context.annotation.Bean
	public ClientHttpRequestFactory defaultRequestFactory() {
		return new org.springframework.http.client.Netty4ClientHttpRequestFactory();
	}

	@org.springframework.context.annotation.Bean("transactionRestTemplate")
	public RestTemplate transactionTemplate(@Autowired ClientHttpRequestFactory requestFactory) {
		RestTemplate restTemplate = new RestTemplate();
		restTemplate.setRequestFactory(requestFactory);
		return restTemplate;
	}

	@DependsOn("transactionRestTemplate")
	@org.springframework.context.annotation.Bean
	public SpringCloudBeanRegistry beanRegistry(@Qualifier("transactionRestTemplate") @Autowired RestTemplate restTemplate) {
		SpringCloudBeanRegistry registry = SpringCloudBeanRegistry.getInstance();
		registry.setRestTemplate(restTemplate);
		return registry;
	}

	@org.springframework.context.annotation.Primary
	@org.springframework.cloud.client.loadbalancer.LoadBalanced
	@org.springframework.context.annotation.Bean
	public RestTemplate defaultRestTemplate(@Autowired TransactionRequestInterceptor transactionRequestInterceptor) {
		RestTemplate restTemplate = new RestTemplate();
		restTemplate.getInterceptors().add(transactionRequestInterceptor);
		return restTemplate;
	}

	public void addInterceptors(InterceptorRegistry registry) {
		TransactionHandlerInterceptor transactionHandlerInterceptor = this.applicationContext
				.getBean(TransactionHandlerInterceptor.class);
		registry.addInterceptor(transactionHandlerInterceptor);
	}

	public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {

		String[] beanNameArray = beanFactory.getBeanDefinitionNames();
		for (int i = 0; i < beanNameArray.length; i++) {
			BeanDefinition beanDef = beanFactory.getBeanDefinition(beanNameArray[i]);
			String beanClassName = beanDef.getBeanClassName();

			if (FEIGN_FACTORY_CLASS.equals(beanClassName) == false) {
				continue;
			}

			MutablePropertyValues mpv = beanDef.getPropertyValues();
			PropertyValue pv = mpv.getPropertyValue("name");
			String client = String.valueOf(pv.getValue() == null ? "" : pv.getValue());
			if (StringUtils.isNotBlank(client)) {
				this.transientClientSet.add(client);
			}

		}

		this.fireAfterPropertiesSet();

	}

	public void fireAfterPropertiesSet() {
		TransactionClientRegistry registry = TransactionClientRegistry.getInstance();

		String inclusions = this.environment.getProperty(CONSTANT_INCLUSIONS);
		String exclusions = this.environment.getProperty(CONSTANT_EXCLUSIONS);

		if (StringUtils.isNotBlank(inclusions) && StringUtils.isNotBlank(exclusions)) {
			throw new IllegalStateException(String.format("Property '%s' and '%s' can not be configured together!",
					CONSTANT_INCLUSIONS, CONSTANT_EXCLUSIONS));
		} else if (StringUtils.isNotBlank(inclusions)) {
			String[] clients = inclusions.split("\\s*,\\s*");
			for (int i = 0; i < clients.length; i++) {
				String client = clients[i];
				registry.registerClient(client);
			} // end-for (int i = 0; i < clients.length; i++)

			this.transientClientSet.clear();
		} else if (StringUtils.isNotBlank(exclusions)) {
			String[] clients = exclusions.split("\\s*,\\s*");
			for (int i = 0; i < clients.length; i++) {
				String client = clients[i];
				this.transientClientSet.remove(client);
			} // end-for (int i = 0; i < clients.length; i++)

			Iterator<String> itr = this.transientClientSet.iterator();
			while (itr.hasNext()) {
				String client = itr.next();
				itr.remove();
				registry.registerClient(client);
			} // end-while (itr.hasNext())
		} else {
			Iterator<String> itr = this.transientClientSet.iterator();
			while (itr.hasNext()) {
				String client = itr.next();
				itr.remove();
				registry.registerClient(client);
			} // end-while (itr.hasNext())
		}

	}

	public void setEndpoint(String identifier) {
		this.identifier = identifier;
	}

	public void setEnvironment(Environment environment) {
		this.environment = environment;
	}

	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

}
