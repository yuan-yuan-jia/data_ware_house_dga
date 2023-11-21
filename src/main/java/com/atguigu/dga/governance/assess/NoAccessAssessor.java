package com.atguigu.dga.governance.assess;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import com.atguigu.dga.governance.bean.TableMetaInfo;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.Date;

@Component("NO_ACCESS")
public class NoAccessAssessor extends Assessor{
    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) {
        Date tableLastAccessTime = assessParam.getTableMetaInfo().getTableLastAccessTime();

        String assessDateString = assessParam.getAssessDate();
        Date assessDate1 = null;
        try {
            assessDate1 = DateUtils.parseDate(assessDateString, "yyyy-MM-dd");
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        // 如果表的最后哦改日期+参数天数 < 当前时间
        // 那么就给差评

        String metricParamsJson = assessParam.getGovernanceMetric().getMetricParamsJson();

        JSONObject paramJsonObject = JSON.parseObject(metricParamsJson);

        Integer days = paramJsonObject.getInteger("days");

        Date limitDate = DateUtils.addDays(tableLastAccessTime, days);

        // 前小后大，给差评
        if (limitDate.compareTo(assessDate1) < 0) {
            governanceAssessDetail.setAssessScore(new BigDecimal("0"));
            governanceAssessDetail.setAssessProblem("长期没有没有访问");
            governanceAssessDetail.setAssessComment("最后访问时间:" + tableLastAccessTime);
        }
    }
}
