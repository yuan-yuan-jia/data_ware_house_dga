package com.atguigu.dga;

import com.atguigu.dga.governance.service.GovernanceAssessDetailService;
import com.atguigu.dga.governance.service.GovernanceAssessTableService;
import com.atguigu.dga.governance.service.impl.TableMetaInfoServiceImpl;
import com.atguigu.dga.lineage.service.GovernanceLineageTableService;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class Dga0717ApplicationTests {


    @Autowired
    TableMetaInfoServiceImpl tableMetaInfoService;

    @Autowired
    GovernanceAssessDetailService detailService;
    @Autowired
    GovernanceAssessTableService governanceAssessTableService;


    @Autowired
    GovernanceLineageTableService governanceLineageTableService;


    @Test
    void contextLoads() {
        tableMetaInfoService.initTableMeta("2023-05-01", "gmall");
    }

    @Test
    void testAssess() {
        detailService.mainAssess("2023-05-02");
    }

    @Test
    void testGenAssessTable() {
        governanceAssessTableService.genAssessTable("2023-05-02");
    }

    @Test
    void testGenLineageTable() {
        try {
            governanceLineageTableService.extractLineageTable("2023-05-02");
        } catch (ParseException e) {
            throw new RuntimeException(e);
        } catch (SemanticException e) {
            throw new RuntimeException(e);
        }
    }
}
