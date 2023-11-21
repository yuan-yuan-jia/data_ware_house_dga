package com.atguigu.dga.governance.assess;

import com.atguigu.dga.governance.bean.*;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

import static com.atguigu.dga.governance.constant.CodeConstant.OWNER_UNSET;

@Component("BUSI_OWNER")
public class BusiOwnerAssessor extends Assessor {


    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) {
        TableMetaInfo tableMetaInfo = assessParam.getTableMetaInfo();
        GovernanceMetric governanceMetric = assessParam.getGovernanceMetric();
        TableMetaInfoExtra tableMetaInfoExtra = tableMetaInfo.getTableMetaInfoExtra();

        if (tableMetaInfoExtra.getBusiOwnerUserName() == null ||
                tableMetaInfoExtra.getBusiOwnerUserName().trim().isEmpty() ||
                tableMetaInfoExtra.getBusiOwnerUserName().equals(OWNER_UNSET)
        ) {
            governanceAssessDetail.setAssessScore(new BigDecimal("0"));
            governanceAssessDetail.setAssessProblem("未填写业务负责人");
            String governanceUrl = governanceMetric.getGovernanceUrl();
            governanceUrl = governanceUrl.replace("{tableId}",tableMetaInfo.getId()+"");
            governanceAssessDetail.setGovernanceUrl(governanceUrl);
        }

    }
}
