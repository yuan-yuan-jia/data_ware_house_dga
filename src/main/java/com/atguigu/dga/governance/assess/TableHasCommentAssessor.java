package com.atguigu.dga.governance.assess;

import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import com.atguigu.dga.governance.bean.TableMetaInfo;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 *  数仓表是否有备注
 */
@Component("TABLE_HAS_COMMENT")
public class TableHasCommentAssessor extends Assessor{
    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) {
        String tableComment = assessParam.getTableMetaInfo().getTableComment();
        if (StringUtils.isEmpty(tableComment) || tableComment.trim().isEmpty()) {
            governanceAssessDetail.setAssessScore(new BigDecimal("0"));
            governanceAssessDetail.setAssessProblem("表没有备注");
            governanceAssessDetail.setAssessComment("表没有备注");
        }
    }
}
