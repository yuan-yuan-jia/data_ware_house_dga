package com.atguigu.dga.governance.service.impl;

import com.atguigu.dga.governance.bean.GovernanceAssessTable;
import com.atguigu.dga.governance.bean.GovernanceAssessTecOwner;
import com.atguigu.dga.governance.mapper.GovernanceAssessTableMapper;
import com.atguigu.dga.governance.mapper.GovernanceAssessTecOwnerMapper;
import com.atguigu.dga.governance.service.GovernanceAssessTecOwnerService;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * <p>
 * 技术负责人治理考评表 服务实现类
 * </p>
 *
 * @author ff
 * @since 2023-11-25
 */
@Service
@DS("dga")
public class GovernanceAssessTecOwnerServiceImpl extends ServiceImpl<GovernanceAssessTecOwnerMapper, GovernanceAssessTecOwner> implements GovernanceAssessTecOwnerService {


    @Autowired
    GovernanceAssessTableMapper governanceAssessTableMapper;

    public void genAssessTecOwnerByTable(String assessDate) {
        //0 清除当天
        remove(new QueryWrapper<GovernanceAssessTecOwner>().eq("assess_date",assessDate));
        // 查询
        List<GovernanceAssessTecOwner> assessTableDetails = governanceAssessTableMapper.getAssessTecOwnerByTable(assessDate);
        // 保存
        saveBatch(assessTableDetails);
    }
}
