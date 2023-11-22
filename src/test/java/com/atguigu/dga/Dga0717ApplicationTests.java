package com.atguigu.dga;

import com.atguigu.dga.governance.service.GovernanceAssessDetailService;
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


    @Test
    void contextLoads() {
        tableMetaInfoService.initTableMeta("2023-05-02", "gmall");
    }

    @Test
    void testAssess() {
        detailService.mainAssess("2023-05-02");
    }

}
