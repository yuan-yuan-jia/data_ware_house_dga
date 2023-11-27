package com.atguigu.dga.governance.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga.governance.bean.GovernanceAssessGlobal;
import com.atguigu.dga.governance.service.GovernanceAssessGlobalService;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

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

}