package com.atguigu.dga.governance.assess.spec;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.atguigu.dga.governance.assess.Assessor;
import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import com.atguigu.dga.governance.constant.CodeConstant;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component("REASONABLE_LIFECYCLE")
public class ReasonableLifeCycle extends Assessor {
    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) {
        String lifecycleType = assessParam.getTableMetaInfo().getTableMetaInfoExtra().getLifecycleType();
        Long lifecycleDays = assessParam.getTableMetaInfo().getTableMetaInfoExtra().getLifecycleDays();
        String partitionColNameJson = assessParam.getTableMetaInfo().getPartitionColNameJson();
        JSONArray partitionJSONArray = new JSONArray();
        if (!StringUtils.isEmpty(partitionColNameJson) && !partitionColNameJson.trim().isEmpty()) {
            partitionJSONArray = JSONArray.parseArray(partitionColNameJson);
        }
        // 未设置生命周期
        if (StringUtils.isEmpty(lifecycleType) ||
                lifecycleType.trim().isEmpty() ||
                CodeConstant.LIFECYCLE_TYPE_UNSET.equals(lifecycleType)
        ) {



            governanceAssessDetail.setAssessScore(new BigDecimal("0"));
            governanceAssessDetail.setAssessProblem("未设置生命周期");
            governanceAssessDetail.setAssessComment("未设置生命周期");
            return;

        }

        // 永久表或拉链表
        if (CodeConstant.LIFECYCLE_TYPE_PERM.equals(lifecycleType) ||
            CodeConstant.LIFECYCLE_TYPE_ZIP.equals(lifecycleType)
        ) {
            governanceAssessDetail.setAssessScore(new BigDecimal("10"));
            return;
        }


        //日分表
        if (CodeConstant.LIFECYCLE_TYPE_DAY.equals(lifecycleType)) {
            // 没设生命周期
            if (lifecycleDays == null ||
                    lifecycleDays == -1L

            ) {
                governanceAssessDetail.setAssessScore(new BigDecimal("0"));
                governanceAssessDetail.setAssessProblem("未设置生命周期");
                governanceAssessDetail.setAssessComment("未设置生命周期");
                return;
            }

            // 没有分区信息
            if (partitionJSONArray.isEmpty()) {
                governanceAssessDetail.setAssessScore(new BigDecimal("0"));
                governanceAssessDetail.setAssessProblem("未设置分区信息");
                governanceAssessDetail.setAssessComment("未设置分区信息");
            }
            return;
        }


        String metricParamsJson = assessParam.getGovernanceMetric().getMetricParamsJson();
        if (!StringUtils.isEmpty(metricParamsJson)) {
            // 是否超过建议周期天数
            Integer days = JSON.parseObject(metricParamsJson).getInteger("days");
            if (lifecycleDays > days) {
                governanceAssessDetail.setAssessScore(new BigDecimal("5"));
                governanceAssessDetail.setAssessProblem("超过建议周期天数");
                governanceAssessDetail.setAssessComment("超过建议周期天数:" + days);
            }
        }


    }
}
