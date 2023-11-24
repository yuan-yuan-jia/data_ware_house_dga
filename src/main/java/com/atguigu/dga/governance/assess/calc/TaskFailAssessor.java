package com.atguigu.dga.governance.assess.calc;

import com.atguigu.dga.ds.bean.TDsTaskInstance;
import com.atguigu.dga.governance.assess.Assessor;
import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import com.atguigu.dga.governance.constant.CodeConstant;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;

@Component("TASK_FAIL")
public class TaskFailAssessor extends Assessor {
    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) {
        List<TDsTaskInstance> tDsTaskInstances = assessParam.getTDsTaskInstances();
        if (tDsTaskInstances == null) {
            return;
        }
        boolean hasFailTask = false;
        for (TDsTaskInstance tDsTaskInstance : tDsTaskInstances) {
            if (tDsTaskInstance.getState().intValue() == CodeConstant.TASK_FAIL) {
                hasFailTask = true;
                break;
            }
        }
        if (hasFailTask) {
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("有失败任务");
        }
    }
}
