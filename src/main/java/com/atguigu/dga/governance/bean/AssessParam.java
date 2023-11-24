package com.atguigu.dga.governance.bean;

import com.atguigu.dga.ds.bean.TDsTaskDefinition;
import com.atguigu.dga.ds.bean.TDsTaskInstance;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class AssessParam {
    private String assessDate;
    private TableMetaInfo tableMetaInfo;
    private GovernanceMetric governanceMetric;

    private List<TableMetaInfo> allTableMetaInfoList;

    private Map<String,TableMetaInfo> allTableMetaInfoMap;

    // 表的任务定义
    private TDsTaskDefinition tDsTaskDefinition;
    // 表的任务定义
    private List<TDsTaskInstance> tDsTaskInstances;
}
