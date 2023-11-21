package com.atguigu.dga.governance.assess;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

@Component("TABLE_FIELDS_HAVE_COMMENT")
public class TableFieldsHaveCommentAssessor extends Assessor {
    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) {
        String colNameJson = assessParam.getTableMetaInfo().getColNameJson();
        JSONArray col = JSONArray.parseArray(colNameJson);
        int numberOfCol = col.size();
        List<String> emptyFields = new ArrayList<>(3);
        if (numberOfCol > 0) {
            int numberOfColThatHasComment = 0;
            for (int i = 0; i < col.size(); i++) {
                JSONObject jsonObject = col.getJSONObject(i);
                String comment = jsonObject.getString("comment");
                if (!StringUtils.isEmpty(comment)) {
                    numberOfColThatHasComment++;
                }else {
                    emptyFields.add(jsonObject.getString("name"));
                }
            }
            double v = (double) numberOfColThatHasComment / (double) numberOfCol;
            BigDecimal score = new BigDecimal("10").multiply(new BigDecimal(v));
            score = score.setScale(2, RoundingMode.HALF_UP);
            governanceAssessDetail.setAssessScore(score);
        }
        if (governanceAssessDetail.getAssessScore().compareTo(new BigDecimal("0")) == 0) {
            governanceAssessDetail.setAssessProblem("没有comment");
            governanceAssessDetail.setAssessComment(emptyFields.toString() + ",没有comment");
        }
    }
}
