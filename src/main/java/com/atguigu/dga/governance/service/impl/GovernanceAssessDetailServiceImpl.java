package com.atguigu.dga.governance.service.impl;

import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import com.atguigu.dga.governance.bean.TableMetaInfo;
import com.atguigu.dga.governance.mapper.GovernanceAssessDetailMapper;
import com.atguigu.dga.governance.service.GovernanceAssessDetailService;
import com.atguigu.dga.governance.service.TableMetaInfoService;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * <p>
 * 治理考评结果明细 服务实现类
 * </p>
 *
 * @author ff
 * @since 2023-11-21
 */
@Service
@DS("dga")
public class GovernanceAssessDetailServiceImpl extends ServiceImpl<GovernanceAssessDetailMapper, GovernanceAssessDetail> implements GovernanceAssessDetailService {


    @Autowired
    TableMetaInfoService tableMetaInfoService;

    public void mainAssess(String assessDate) {
        //1 提取要考评的元数据和辅助信息
        List<TableMetaInfo> tableMetaInfoList = tableMetaInfoService.getTableMetaInfoWithExtraList(assessDate);
        System.out.println(tableMetaInfoList);
        //2 提取要考评的指标
        //3 对每张表每个指标进行考评 --> 生成考评结果明细
        //4 把考评结果明细保存

    }

}
