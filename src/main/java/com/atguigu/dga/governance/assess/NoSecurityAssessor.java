package com.atguigu.dga.governance.assess;

import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import com.atguigu.dga.governance.constant.CodeConstant;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component("NO_SECURITY")
public class NoSecurityAssessor extends Assessor {
    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) {
        String securityLevel = assessParam.getTableMetaInfo().getTableMetaInfoExtra().getSecurityLevel();

        if (StringUtils.isEmpty(securityLevel) ||
                securityLevel.trim().isEmpty() ||
                securityLevel.equals(CodeConstant.SECURITY_LEVEL_UNSET)) {
            governanceAssessDetail.setAssessScore(new BigDecimal("0"));
            governanceAssessDetail.setAssessProblem("安全等级未设置");
            governanceAssessDetail.setAssessComment("安全等级未设置");
        }
    }
}
