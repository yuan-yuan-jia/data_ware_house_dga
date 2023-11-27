package com.atguigu.dga.lineage.service;

import com.atguigu.dga.lineage.bean.GovernanceLineageTable;
import com.baomidou.mybatisplus.extension.service.IService;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author ff
 * @since 2023-11-27
 */
public interface GovernanceLineageTableService extends IService<GovernanceLineageTable> {

    void extractLineageTable(String governanceDate) throws ParseException, SemanticException;
}
