package com.atguigu.dga.governance.bean;

import lombok.Data;

import java.util.List;

@Data
public class AssessParam {
    private String assessDate;
    private TableMetaInfo tableMetaInfo;
    private GovernanceMetric governanceMetric;

    List<TableMetaInfo> allTableMetaInfoList;
}
