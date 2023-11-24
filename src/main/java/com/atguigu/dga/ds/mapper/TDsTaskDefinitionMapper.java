package com.atguigu.dga.ds.mapper;

import com.atguigu.dga.ds.bean.TDsTaskDefinition;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * <p>
 *  Mapper 接口
 * </p>
 *
 * @author ff
 * @since 2023-11-22
 */
@Mapper
@DS("dl")
public interface TDsTaskDefinitionMapper extends BaseMapper<TDsTaskDefinition> {

    List<TDsTaskDefinition> getTaskDefinitionOfInstanceOfSuccessOnAssessDate(
            @Param("assessDate") String assessDate,
            @Param("stateF") Integer stateF
    );
}
