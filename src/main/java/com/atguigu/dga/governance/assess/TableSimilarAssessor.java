package com.atguigu.dga.governance.assess;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import com.atguigu.dga.governance.bean.TableMetaInfo;
import com.atguigu.dga.governance.constant.CodeConstant;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Component("TABLE_SIMILAR")
public class TableSimilarAssessor extends Assessor {

    // 同层次两个表字段重复超过{percent}%，则给0分
    // 参数增加 其他所有表的清单
    // 循环遍历所有表（排除当前表，排除其他层次）
    // 遍历过程中，两表对比，有几个字段相同
    // 相同字段的占比：相同字段数 / 总字段数
    // 用占比与percent对比
    // 如果超过percent给差评，问题描述中和哪张表相似

    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) {
        TableMetaInfo currentTableMetaInfo = assessParam.getTableMetaInfo();
        JSONObject paramJSON = JSON.parseObject(assessParam.getGovernanceMetric().getMetricParamsJson());
        BigDecimal paramPercent = paramJSON.getBigDecimal("percent");
        List<TableMetaInfo> allTableMetaInfoList = assessParam.getAllTableMetaInfoList();

        // 排除ods,ods不参数评比
        if (currentTableMetaInfo.getTableMetaInfoExtra().getDwLevel().equalsIgnoreCase(CodeConstant.DW_LEVEL_ODS)) {
            return;
        }

        List<String> similarTableNames = new ArrayList<>();
        for (TableMetaInfo otherTableMetaInfo : allTableMetaInfoList) {
            // 排除当前表
            if (otherTableMetaInfo.getTableName().equals(currentTableMetaInfo.getTableName()) &&
                    otherTableMetaInfo.getSchemaName().equals(currentTableMetaInfo.getSchemaName())
            ) {
                continue;
            }

            // 排除其他层次的表
            if (!otherTableMetaInfo.getTableMetaInfoExtra().getDwLevel().equals(currentTableMetaInfo.getTableMetaInfoExtra().getDwLevel())) {
                continue;
            }


            // 两表对比
            String currentColNameJson = currentTableMetaInfo.getColNameJson();
            List<JSONObject> currentColArray = JSON.parseArray(currentColNameJson, JSONObject.class);

            List<JSONObject> otherColArray = JSON.parseArray(otherTableMetaInfo.getColNameJson(),JSONObject.class);


            Set<String> otherColNames = otherColArray.stream().filter(j -> {
                return j.getString("name") != null;
            }).map(j -> {
                return j.getString("name");
            }).collect(Collectors.toSet());

            int similarColCount  = 0;
            for (JSONObject jsonObject : currentColArray) {
                String colName = jsonObject.getString("name");
                if (otherColNames.contains(colName)) {
                    similarColCount++;
                }
            }

            // 算占比
            BigDecimal sameRatio = new BigDecimal(similarColCount).movePointRight(2).divide(new BigDecimal(currentColArray.size()),1, RoundingMode.HALF_UP);
            // 对比
            if (sameRatio.compareTo(paramPercent) > 0) {
                similarTableNames.add(otherTableMetaInfo.getSchemaName()+"."+otherTableMetaInfo.getTableName());
            }

        }

        // 只要清单中有值，就说明存在相似表，给差评
        if (!similarTableNames.isEmpty()) {
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("存在相似表:" + StringUtils.join(similarTableNames,","));
        }

    }
}
