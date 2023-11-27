package com.atguigu.dga.governance.service.impl;

import com.atguigu.dga.governance.bean.GovernanceAssessGlobal;
import com.atguigu.dga.governance.bean.GovernanceAssessTecOwner;
import com.atguigu.dga.governance.mapper.GovernanceAssessGlobalMapper;
import com.atguigu.dga.governance.mapper.GovernanceAssessTableMapper;
import com.atguigu.dga.governance.service.GovernanceAssessGlobalService;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * <p>
 * 治理总考评表 服务实现类
 * </p>
 *
 * @author ff
 * @since 2023-11-25
 */
@Service
@DS("dga")
public class GovernanceAssessGlobalServiceImpl extends ServiceImpl<GovernanceAssessGlobalMapper, GovernanceAssessGlobal> implements GovernanceAssessGlobalService {


    @Autowired
    GovernanceAssessTableMapper governanceAssessTableMapper;

    public void genAssessGlobalByTable(String assessDate) {
        //0 清除当天
        remove(new QueryWrapper<GovernanceAssessGlobal>().eq("assess_date",assessDate));
        // 查询
        GovernanceAssessGlobal governanceAssessGlobal = governanceAssessTableMapper.getAssessGlobalByTable(assessDate);
        // 保存
        save(governanceAssessGlobal);
    }
}
