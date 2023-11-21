package com.atguigu.dga.governance.bean;

import lombok.Data;

@Data
public class AssessParam {
    private String assessDate;
    private TableMetaInfo tableMetaInfo;
    private GovernanceMetric governanceMetric;
}
