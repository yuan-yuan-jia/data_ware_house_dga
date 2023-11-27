package com.atguigu.dga.governance.service.impl;

import com.atguigu.dga.governance.bean.GovernanceAssessTecOwner;
import com.atguigu.dga.governance.mapper.GovernanceAssessTecOwnerMapper;
import com.atguigu.dga.governance.service.GovernanceAssessTecOwnerService;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

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

}
