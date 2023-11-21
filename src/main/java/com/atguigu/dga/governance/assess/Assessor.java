package com.atguigu.dga.governance.assess;

import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import com.atguigu.dga.governance.bean.GovernanceMetric;
import com.atguigu.dga.governance.bean.TableMetaInfo;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.util.Date;

public abstract class Assessor {
    public GovernanceAssessDetail metricAssess(AssessParam assessParam) {
        //由父类进行控制，实现公共且不变的行为
        // 创建结果对象
        TableMetaInfo tableMetaInfo = assessParam.getTableMetaInfo();
        String assessDate = assessParam.getAssessDate();
        GovernanceMetric governanceMetric = assessParam.getGovernanceMetric();
        GovernanceAssessDetail governanceAssessDetail = new GovernanceAssessDetail();

        governanceAssessDetail.setTableName(tableMetaInfo.getTableName());
        governanceAssessDetail.setSchemaName(tableMetaInfo.getSchemaName());
        governanceAssessDetail.setMetricId(governanceMetric.getId()+"");
        governanceAssessDetail.setMetricName(governanceMetric.getMetricName());
        governanceAssessDetail.setGovernanceType(governanceMetric.getGovernanceType());



        governanceAssessDetail.setAssessDate(assessDate);
        governanceAssessDetail.setTecOwner(tableMetaInfo.getTableMetaInfoExtra().getTecOwnerUserName());
        governanceAssessDetail.setCreateTime(new Date());
        governanceAssessDetail.setAssessScore(new BigDecimal("10"));
        // 填写基本信息 各种名称id 时间 给默认分数
        // 负责处理报错
        // 调用子类检查问题 子类检查是否有问题，如果有 给低分和问题原因
        try {
            checkProblem(governanceAssessDetail,assessParam);
        }catch (Exception e) {
            governanceAssessDetail.setIsAssessException("1");
            StringWriter stringWriter = new StringWriter();

            e.printStackTrace(new PrintWriter(stringWriter));
            String string = stringWriter.toString();
            String exceptionMsg = string.substring(0, Math.min(string.length(), 2000));
            governanceAssessDetail.setAssessExceptionMsg(exceptionMsg);
        }
        // 返回考评结果明细
        return governanceAssessDetail;
    }

    public abstract void checkProblem(GovernanceAssessDetail governanceAssessDetail,AssessParam assessParam);
}
