package com.atguigu.dga.ds.service.impl;

import com.atguigu.dga.ds.bean.TDsTaskInstance;
import com.atguigu.dga.ds.mapper.TDsTaskInstanceMapper;
import com.atguigu.dga.ds.service.TDsTaskInstanceService;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author ff
 * @since 2023-11-22
 */
@Service
@DS("dl")
public class TDsTaskInstanceServiceImpl extends ServiceImpl<TDsTaskInstanceMapper, TDsTaskInstance> implements TDsTaskInstanceService {

    @Override
    public Map<String,List<TDsTaskInstance>> getTaskInstanceList(String assessDate) {
        List<TDsTaskInstance> list = list(new QueryWrapper<TDsTaskInstance>().eq("date_format(submit_time,'%Y-%m-%d')", assessDate));

        Map<String,List<TDsTaskInstance>> taskInstanceListMap = new HashMap<>();
        for (TDsTaskInstance tDsTaskInstance : list) {
            // 检查是否map中已经存在该表的实例
            //
            String name = tDsTaskInstance.getName();
            List<TDsTaskInstance> tDsTaskInstances = taskInstanceListMap.get(name);
            if (tDsTaskInstances == null) {
                tDsTaskInstances = new LinkedList<>();
                taskInstanceListMap.put(name,tDsTaskInstances);
            }
            tDsTaskInstances.add(tDsTaskInstance);


        }

        return taskInstanceListMap;
    }
}
