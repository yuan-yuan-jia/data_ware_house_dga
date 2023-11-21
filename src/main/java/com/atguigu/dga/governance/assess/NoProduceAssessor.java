package com.atguigu.dga.governance.assess;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import com.atguigu.dga.governance.bean.TableMetaInfo;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.Date;

@Component("NO_PRODUCE")
public class NoProduceAssessor extends Assessor{


    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam)  {
        TableMetaInfo tableMetaInfo = assessParam.getTableMetaInfo();
        Date tableLastModifyTime = tableMetaInfo.getTableLastModifyTime();
        String assessDate = assessParam.getAssessDate();
        Date assessDate1 = null;
        try {
            assessDate1 = DateUtils.parseDate(assessDate, "yyyy-MM-dd");
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        // 如果表的最后哦改日期+参数天数 < 当前时间
        // 那么就给差评

        String metricParamsJson = assessParam.getGovernanceMetric().getMetricParamsJson();

        JSONObject paramJsonObject = JSON.parseObject(metricParamsJson);

        Integer days = paramJsonObject.getInteger("days");

        Date limitDate = DateUtils.addDays(tableLastModifyTime, days);

        // 前小后大，给差评
        if (limitDate.compareTo(assessDate1) < 0) {
            governanceAssessDetail.setAssessScore(new BigDecimal("0"));
            governanceAssessDetail.setAssessProblem("长期没有数据产出");
            governanceAssessDetail.setAssessComment("最后产出时间:" + tableLastModifyTime);
        }



    }
}
