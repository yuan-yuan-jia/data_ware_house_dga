package com.atguigu.dga.ds.service;

import com.atguigu.dga.ds.bean.TDsTaskInstance;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;
import java.util.Map;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author ff
 * @since 2023-11-22
 */
public interface TDsTaskInstanceService extends IService<TDsTaskInstance> {

   Map<String,List<TDsTaskInstance>> getTaskInstanceList(String assessDate);
}
