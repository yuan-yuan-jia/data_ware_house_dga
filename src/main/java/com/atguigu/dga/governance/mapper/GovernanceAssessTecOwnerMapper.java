package com.atguigu.dga.governance.mapper;

import com.atguigu.dga.governance.bean.GovernanceAssessTecOwner;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/**
 * <p>
 * 技术负责人治理考评表 Mapper 接口
 * </p>
 *
 * @author ff
 * @since 2023-11-25
 */
@Mapper
@DS("dga")
public interface GovernanceAssessTecOwnerMapper extends BaseMapper<GovernanceAssessTecOwner> {

}
