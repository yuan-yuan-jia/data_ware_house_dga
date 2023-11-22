package com.atguigu.dga.governance.assess;

import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import com.atguigu.dga.governance.bean.TableMetaInfo;
import com.atguigu.dga.governance.bean.TableMetaInfoExtra;
import com.atguigu.dga.governance.constant.CodeConstant;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.regex.Pattern;

@Component("TABLE_NAME_STANDARD")
public class TableNameStandardAssessor extends Assessor{

    // 为不同层次构建不同的表达式
    Pattern odsPattern = Pattern.compile("^ods_\\w+_(inc|full)$");
    Pattern dimPattern = Pattern.compile("^dim_\\w+_(zip|full)$");
    Pattern dwdPattern = Pattern.compile("^dwd_\\w+_\\w+_(inc|full|acc)$");
    Pattern dwsPattern = Pattern.compile("^dws_\\w+_\\w+_\\w+_(1d|td|nd)$");
    Pattern adsPattern = Pattern.compile("^ads_\\w+$");
    Pattern dmPattern = Pattern.compile("^dm_\\w+$");



    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) {
        TableMetaInfo tableMetaInfo = assessParam.getTableMetaInfo();
        TableMetaInfoExtra tableMetaInfoExtra = tableMetaInfo.getTableMetaInfoExtra();
        String dwLevel = tableMetaInfoExtra.getDwLevel();
        String tableName = tableMetaInfo.getTableName();
        Pattern tableNamePattern = null;
        // 根据要考虑的表的层次，选择表达式
        if (dwLevel.equals(CodeConstant.DW_LEVEL_ODS)) {
            tableNamePattern = odsPattern;
        }else if (dwLevel.equals(CodeConstant.DW_LEVEL_DIM)) {
            tableNamePattern = dimPattern;
        }else if (dwLevel.equals(CodeConstant.DW_LEVEL_DWD)) {
            tableNamePattern = dwdPattern;
        }else if (dwLevel.equals(CodeConstant.DW_LEVEL_DWS)) {
            tableNamePattern = dwsPattern;
        }else if (dwLevel.equals(CodeConstant.DW_LEVEL_ADS)) {
            tableNamePattern = adsPattern;
        }else if (dwLevel.equals(CodeConstant.DW_LEVEL_DM)) {
            tableNamePattern = dmPattern;
        }else {
            governanceAssessDetail.setAssessScore(new BigDecimal("5"));
            governanceAssessDetail.setAssessProblem("未纳入分层");
            return;
        }
        //  利用表达式进行比较
        if (!tableNamePattern.matcher(tableName).matches()) {
            governanceAssessDetail.setAssessScore(new BigDecimal("0"));
            governanceAssessDetail.setAssessProblem("不符合表名规范");
        }
    }

}
