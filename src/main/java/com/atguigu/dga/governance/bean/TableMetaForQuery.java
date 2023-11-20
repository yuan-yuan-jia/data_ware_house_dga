package com.atguigu.dga.governance.bean;

import lombok.Data;
import org.springframework.web.bind.annotation.RequestParam;

@Data
public class TableMetaForQuery {

    private Integer pageNo=1;

    private Integer pageSize=20;

    /**
     * 表名
     */
    private String tableName;

    /**
     * 库名
     */
    private String schemaName;

    /**
     * 数仓所在层级(ODSDWDDIMDWSADS) ( 来源: 附加)
     */
    private String dwLevel;
}
