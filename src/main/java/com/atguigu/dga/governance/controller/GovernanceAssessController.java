package com.atguigu.dga.governance.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import com.atguigu.dga.governance.bean.GovernanceAssessGlobal;
import com.atguigu.dga.governance.bean.GovernanceAssessTecOwner;
import com.atguigu.dga.governance.service.GovernanceAssessDetailService;
import com.atguigu.dga.governance.service.GovernanceAssessGlobalService;
import com.atguigu.dga.governance.service.GovernanceAssessTableService;
import com.atguigu.dga.governance.service.GovernanceAssessTecOwnerService;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * 治理总考评表 前端控制器
 * </p>
 *
 * @author ff
 * @since 2023-11-25
 */
@RestController
@RequestMapping("/governance")
public class GovernanceAssessController {

    @Autowired
    GovernanceAssessGlobalService assessGlobalService;

    @Autowired
    GovernanceAssessTecOwnerService tecOwnerService;

    @Autowired
    GovernanceAssessTableService assessTableService;

    @Autowired
    GovernanceAssessDetailService assessDetailService;

    @GetMapping("/globalScore")
    public String getGlobalScore() {
        QueryWrapper<GovernanceAssessGlobal> queryWrapper = new QueryWrapper<GovernanceAssessGlobal>();
        queryWrapper.inSql("assess_date",
                "select max(assess_date) from governance_assess_global"
                );
        GovernanceAssessGlobal one = assessGlobalService.getOne(queryWrapper);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("assessDate",one.getAssessDate());
        jsonObject.put("sumScore",one.getScore());
        List<BigDecimal> scoreList = new ArrayList<>();
        scoreList.add(one.getScoreSpec());
        scoreList.add(one.getScoreStorage());
        scoreList.add(one.getScoreCalc());
        scoreList.add(one.getScoreQuality());
        scoreList.add(one.getScoreSecurity());
        jsonObject.put("scoreList",scoreList);

        return jsonObject.toJSONString();
    }


    @GetMapping("/rankList")
    public String getRankList() {
        QueryWrapper<GovernanceAssessTecOwner> queryWrapper = new QueryWrapper<GovernanceAssessTecOwner>()
                .inSql("assess_date",
                        "select max(assess_date) from governance_assess_tec_owner"
                );

        queryWrapper.orderByDesc("score")
                        .select("score","tec_owner tecOwner");
        List<Map<String, Object>> maps = tecOwnerService.listMaps(queryWrapper);

        return JSON.toJSONString(maps);
    }

    @GetMapping("/problemNum")
    public String getProblemNum() {
        QueryWrapper<GovernanceAssessDetail> governanceAssessDetailQueryWrapper = new QueryWrapper<>();

        governanceAssessDetailQueryWrapper.inSql("assess_date",
                "select max(assess_date) from governance_assess_detail"
        ).select("sum(if(governance_type = 'SPEC' and assess_score<10,1,0)) 'SPEC'",
                "sum(if(governance_type = 'CALC' and assess_score<10,1,0)) 'CALC'",
                "sum(if(governance_type = 'STORAGE' and assess_score<10,1,0)) 'STORAGE'",
                "sum(if(governance_type = 'SECURITY' and assess_score<10,1,0)) 'SECURITY'",
                "sum(if(governance_type = 'QUALITY' and assess_score<10,1,0)) 'QUALITY'"
                );

        Map<String, Object> map = assessDetailService.getMap(governanceAssessDetailQueryWrapper);
        return JSON.toJSONString(map);
    }

}
