package com.atguigu.dga.scheduler;

import com.atguigu.dga.governance.service.*;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class AssessScheduler {

    @Autowired
    TableMetaInfoService tableMetaInfoService;

    @Autowired
    GovernanceAssessDetailService assessDetailService;


    @Autowired
    GovernanceAssessTableService assessTableService;
    @Autowired
    GovernanceAssessTecOwnerService assessTecOwnerService;

    @Autowired
    GovernanceAssessGlobalService governanceAssessGlobalService;

    @Value("${assess.schema-names}")
    String assessSchemaNames;


    @Scheduled(cron = "3 3 10 * * *")
    public void exec() {
        System.out.println("定时执行: " + new Date());
        mainAssess();
    }


    public void mainAssess() {
        //String assessDate = DateFormatUtils.format(new Date(), "yyyy-MM-dd");
        String assessDate = "2023-05-02";
        String[] schemaNames = assessSchemaNames.split(",");
        for (String schemaName : schemaNames) {
            //1提取元数据
            tableMetaInfoService.initTableMeta(assessDate,schemaName);
            //2 考评
            assessDetailService.mainAssess(assessDate);
            //3 统计分数
            assessTableService.genAssessTable(assessDate);
            assessTecOwnerService.genAssessTecOwnerByTable(assessDate);
            governanceAssessGlobalService.genAssessGlobalByTable(assessDate);
        }

    }


}
