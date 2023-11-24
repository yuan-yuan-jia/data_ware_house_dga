package com.atguigu.dga.ds.service;

import com.atguigu.dga.ds.bean.TDsTaskDefinition;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.Map;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author ff
 * @since 2023-11-22
 */
public interface TDsTaskDefinitionService extends IService<TDsTaskDefinition> {

    Map<String, TDsTaskDefinition> getTaskDefinitionMap(String assessDate);
}
