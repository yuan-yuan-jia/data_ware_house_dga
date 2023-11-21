package com.atguigu.dga.governance.assess;

import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component("TABLE_IS_EMPTY")
public class TableIsEmptyAssessor extends Assessor {
    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) {
        Long tableSize = assessParam.getTableMetaInfo().getTableSize();
        if (tableSize == null || tableSize == 0) {
            governanceAssessDetail.setAssessScore(new BigDecimal("0"));
            governanceAssessDetail.setAssessProblem("表大小为空");
        }
    }
}
