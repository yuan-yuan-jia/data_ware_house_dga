package com.atguigu.dga.governance.assess.quality;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga.ds.bean.TDsTaskInstance;
import com.atguigu.dga.ds.service.TDsTaskInstanceService;
import com.atguigu.dga.governance.assess.Assessor;
import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import com.atguigu.dga.governance.constant.CodeConstant;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Component("TIME_LINESS")
public class TimeLinessAssessor extends Assessor {


    @Autowired
    TDsTaskInstanceService taskInstanceService;

    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) {
        // 求当天的平均耗时
        List<TDsTaskInstance> tDsTaskInstances = assessParam.getTDsTaskInstances();
        if (tDsTaskInstances == null || tDsTaskInstances.isEmpty()) {
            return;
        }
        long totalDurationSec = 0L;
        long successfulTask = 0;
        for (TDsTaskInstance tDsTaskInstance : tDsTaskInstances) {
            if (tDsTaskInstance.getState().intValue() == CodeConstant.TASK_SUCCESS) {
                totalDurationSec += ((tDsTaskInstance.getEndTime().getTime() - tDsTaskInstance.getSubmitTime().getTime()) / 1000);
                successfulTask++;
            }
        }
        // 当日平均耗时
        BigDecimal curAvg = BigDecimal.valueOf(totalDurationSec)
                .divide(new BigDecimal(successfulTask), 1, RoundingMode.HALF_UP);

        // 求前n天的平均耗时，需要计算前n天起止日期
        String metricParamsJson = assessParam.getGovernanceMetric().getMetricParamsJson();
        JSONObject jsonObject = JSON.parseObject(metricParamsJson);
        Integer days = jsonObject.getInteger("days");
        Date assessDate = null;
        try {
            assessDate = DateUtils.parseDate(assessParam.getAssessDate(), "yyyy-MM-dd");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // 求占比： （当天的平均耗时 - 前n天的平均耗时） / 前n天的平均耗时
        String fromDt = DateFormatUtils.format(DateUtils.addDays(assessDate, -days), "yyyy-MM-dd");
        String endDt = DateFormatUtils.format(DateUtils.addDays(assessDate, -1), "yyyy-MM-dd");
        // 表名查询任务列表的范围
        /// 当前表、成功的任务、时间范围
        QueryWrapper<TDsTaskInstance> taskInstanceQueryWrapper = new QueryWrapper<TDsTaskInstance>()
                .select("avg(end_time - submit_time) avg_duration_secs")
                .eq("name", assessParam.getTableMetaInfo().getSchemaName() + "." + assessParam.getTableMetaInfo().getTableName())
                .eq("state", CodeConstant.TASK_SUCCESS)
                .between("date_format(submit_time,'%Y-%m-%d')", fromDt, endDt);
        Map<String, Object> resultMap = taskInstanceService.getMap(taskInstanceQueryWrapper);
        BigDecimal avgDurationSecs = (BigDecimal) resultMap.get("avg_duration_secs");
        if (avgDurationSecs == null || avgDurationSecs.compareTo(BigDecimal.ZERO) ==0 ){
            return;
        }

        // 占比和参考占比比较大小， 如果超过则给差评
        BigDecimal percent = curAvg.subtract(avgDurationSecs
                .movePointRight(2)
                .divide(avgDurationSecs, 1, RoundingMode.HALF_UP));

        BigDecimal paramPercent = jsonObject.getBigDecimal("percent");

        if (percent.compareTo(paramPercent) > 0) {
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("超过："+ days + "平均参考耗时:" + percent  + "% (当日平均耗时：" + curAvg +"s"+ ",前"+days + "平均耗时:" + avgDurationSecs+"s");
        }
    }
}
