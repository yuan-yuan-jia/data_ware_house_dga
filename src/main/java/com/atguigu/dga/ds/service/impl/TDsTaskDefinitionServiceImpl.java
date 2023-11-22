package com.atguigu.dga.ds.service.impl;

import com.atguigu.dga.ds.bean.TDsTaskDefinition;
import com.atguigu.dga.ds.mapper.TDsTaskDefinitionMapper;
import com.atguigu.dga.ds.service.TDsTaskDefinitionService;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

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
public class TDsTaskDefinitionServiceImpl extends ServiceImpl<TDsTaskDefinitionMapper, TDsTaskDefinition> implements TDsTaskDefinitionService {

}
