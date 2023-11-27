package com.atguigu.dga.governance.service.impl;

import com.atguigu.dga.governance.bean.GovernanceAssessGlobal;
import com.atguigu.dga.governance.mapper.GovernanceAssessGlobalMapper;
import com.atguigu.dga.governance.service.GovernanceAssessGlobalService;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

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

}
