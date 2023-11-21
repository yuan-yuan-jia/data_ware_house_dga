package com.atguigu.dga.governance.assess;

import com.atguigu.dga.governance.bean.*;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

import static com.atguigu.dga.governance.constant.CodeConstant.OWNER_UNSET;

@Component("TEC_OWNER")
public class TecOwnerAssessor extends Assessor{

    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) {
        TableMetaInfo tableMetaInfo = assessParam.getTableMetaInfo();
        GovernanceMetric governanceMetric = assessParam.getGovernanceMetric();
        TableMetaInfoExtra tableMetaInfoExtra = tableMetaInfo.getTableMetaInfoExtra();

        if (tableMetaInfoExtra.getTecOwnerUserName() == null ||
                tableMetaInfoExtra.getTecOwnerUserName().trim().isEmpty() ||
                tableMetaInfoExtra.getTecOwnerUserName().equals(OWNER_UNSET)
        ) {
            governanceAssessDetail.setAssessScore(new BigDecimal("0"));
            governanceAssessDetail.setAssessProblem("未填写技术负责人");
            String governanceUrl = governanceMetric.getGovernanceUrl();
            governanceUrl = governanceUrl.replace("{tableId}",tableMetaInfo.getId()+"");
            governanceAssessDetail.setGovernanceUrl(governanceUrl);
        }
    }
}
