package com.atguigu.dga.governance.bean;

import com.atguigu.dga.ds.bean.TDsTaskDefinition;
import com.atguigu.dga.ds.bean.TDsTaskInstance;
import lombok.Data;

import java.util.List;

@Data
public class AssessParam {
    private String assessDate;
    private TableMetaInfo tableMetaInfo;
    private GovernanceMetric governanceMetric;

    private List<TableMetaInfo> allTableMetaInfoList;

    // 表的任务定义
    private TDsTaskDefinition tDsTaskDefinition;
    // 表的任务定义
    private List<TDsTaskInstance> tDsTaskInstances;
}
