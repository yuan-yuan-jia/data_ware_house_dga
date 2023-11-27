package com.atguigu.dga.lineage.mapper;

import com.atguigu.dga.lineage.bean.GovernanceLineageTable;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/**
 * <p>
 *  Mapper 接口
 * </p>
 *
 * @author ff
 * @since 2023-11-27
 */
@Mapper
@DS("dga")
public interface GovernanceLineageTableMapper extends BaseMapper<GovernanceLineageTable> {

}
