package org.bytesoft.bytejta.supports.spring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.transaction.jta.JtaTransactionManager;

public class JtaTransactionManagerPostProcessor implements BeanPostProcessor, EnvironmentAware {
    static final Logger logger = LoggerFactory.getLogger(JtaTransactionManagerPostProcessor.class);
    static final String JTA_TM_NAME = "jtaTransactionManager";
    private Environment environment;

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        if (JTA_TM_NAME.equals(beanName) && bean instanceof JtaTransactionManager) {
            boolean allowCustomIsolationLevels = Boolean.valueOf(this.environment.getProperty("spring.jta.allowCustomIsolationLevels", "true"));
            ((JtaTransactionManager) bean).setAllowCustomIsolationLevels(allowCustomIsolationLevels);
        }
        return bean;
    }
}
