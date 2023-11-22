package com.atguigu.dga.governance.service.impl;

import com.atguigu.dga.ds.bean.TDsTaskInstance;
import com.atguigu.dga.ds.service.TDsTaskDefinitionService;
import com.atguigu.dga.ds.service.TDsTaskInstanceService;
import com.atguigu.dga.governance.assess.Assessor;
import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import com.atguigu.dga.governance.bean.GovernanceMetric;
import com.atguigu.dga.governance.bean.TableMetaInfo;
import com.atguigu.dga.governance.mapper.GovernanceAssessDetailMapper;
import com.atguigu.dga.governance.service.GovernanceAssessDetailService;
import com.atguigu.dga.governance.service.GovernanceMetricService;
import com.atguigu.dga.governance.service.TableMetaInfoService;
import com.atguigu.dga.governance.utils.SpringBeanProvider;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    @Autowired
    GovernanceMetricService metricService;


    @Autowired
    SpringBeanProvider beanProvider;

    @Autowired
    TDsTaskDefinitionService taskDefinitionService;

    @Autowired
    TDsTaskInstanceService taskInstanceService;



    public void mainAssess(String assessDate) {

        //0 清理今天已经做过的考评
        remove(new QueryWrapper<GovernanceAssessDetail>().eq("assess_date",assessDate));

        //1 提取要考评的元数据和辅助信息
        List<TableMetaInfo> tableMetaInfoList = tableMetaInfoService.getTableMetaInfoWithExtraList(assessDate);
        System.out.println(tableMetaInfoList);
        //2 提取要考评的指标
        List<GovernanceMetric> metrics = metricService.list(new QueryWrapper<GovernanceMetric>().eq("is_disabled", "0"));

        //2.5 准备ds的任务定义和任务实例
        //实例
        Map<String, List<TDsTaskInstance>> taskInstanceListMap = taskInstanceService.getTaskInstanceList(assessDate);



        List<GovernanceAssessDetail> governanceAssessDetails = new ArrayList<>(10);
        //3 对每张表每个指标进行考评 --> 生成考评结果明细
        for (TableMetaInfo tableMetaInfo : tableMetaInfoList) {
            for (GovernanceMetric metric : metrics) {
                String metricCode = metric.getMetricCode();
                Assessor assessor = beanProvider.getBean(metricCode, Assessor.class);
                AssessParam assessParam = new AssessParam();
                assessParam.setAssessDate(assessDate);
                assessParam.setGovernanceMetric(metric);
                assessParam.setTableMetaInfo(tableMetaInfo);
                assessParam.setAllTableMetaInfoList(tableMetaInfoList);

                // 当前表当天的任务列表
                List<TDsTaskInstance> tDsTaskInstances = taskInstanceListMap.get(tableMetaInfo.getSchemaName() + "." + tableMetaInfo.getTableName());
                assessParam.setTDsTaskInstances(tDsTaskInstances);

                GovernanceAssessDetail governanceAssessDetail = assessor.metricAssess(assessParam);
                governanceAssessDetails.add(governanceAssessDetail);
            }
        }
        //4 把考评结果明细保存
        saveBatch(governanceAssessDetails);

    }

}
