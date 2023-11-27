package com.atguigu.dga.governance.service.impl;

import com.atguigu.dga.governance.bean.GovernanceAssessTable;
import com.atguigu.dga.governance.mapper.GovernanceAssessTableMapper;
import com.atguigu.dga.governance.service.GovernanceAssessTableService;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * <p>
 * 表治理考评情况 服务实现类
 * </p>
 *
 * @author ff
 * @since 2023-11-25
 */
@Service
@DS("dga")
public class GovernanceAssessTableServiceImpl extends ServiceImpl<GovernanceAssessTableMapper, GovernanceAssessTable> implements GovernanceAssessTableService {

    public void genAssessTable(String assessDate) {
        //0 清除当天
        remove(new QueryWrapper<GovernanceAssessTable>().eq("assess_date",assessDate));
        // 查询
        List<GovernanceAssessTable> assessTableDetails = baseMapper.getAssessTableDetail(assessDate);
        // 保存
        saveBatch(assessTableDetails);
    }

}
