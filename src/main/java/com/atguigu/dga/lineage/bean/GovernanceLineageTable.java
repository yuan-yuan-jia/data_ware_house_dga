package com.atguigu.dga.lineage.bean;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import java.io.Serializable;
import java.util.Date;
import lombok.Data;

/**
 * <p>
 * 
 * </p>
 *
 * @author ff
 * @since 2023-11-27
 */
@Data
@TableName("governance_lineage_table")
public class GovernanceLineageTable implements Serializable {

    private static final long serialVersionUID = 1L;

    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /**
     * 治理日期
     */
    private String governanceDate;

    /**
     * 库名
     */
    private String schemaName;

    /**
     * 表名
     */
    private String tableName;

    /**
     * 来源表
     */
    private String sourceTables;

    /**
     * 输出表
     */
    private String sinkTables;

    /**
     * 写入时间
     */
    private Date createTime;
}
