package com.atguigu.dga.governance.service;

import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 * 治理考评结果明细 服务类
 * </p>
 *
 * @author ff
 * @since 2023-11-21
 */
public interface GovernanceAssessDetailService extends IService<GovernanceAssessDetail> {

    void mainAssess(String assessDate);
}
