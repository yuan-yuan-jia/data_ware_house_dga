package com.atguigu.dga;

import com.atguigu.dga.governance.service.impl.TableMetaInfoServiceImpl;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class Dga0717ApplicationTests {


    @Autowired
    TableMetaInfoServiceImpl tableMetaInfoService;

    @Test
    void contextLoads() {
        tableMetaInfoService.initTableMeta("", "gmall");
    }

}
