package com.atguigu.dga;

import com.atguigu.dga.governance.service.GovernanceAssessDetailService;
import com.atguigu.dga.governance.service.GovernanceAssessTableService;
import com.atguigu.dga.governance.service.impl.TableMetaInfoServiceImpl;
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

}
