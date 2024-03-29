package com.atguigu.dga.governance.utils;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class SpringBeanProvider implements ApplicationContextAware {

    private ApplicationContext applicationContext;


    public <T> T getBean(String beanName,Class<T> clazz) {
        return applicationContext.getBean(beanName,clazz);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
