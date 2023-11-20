package com.atguigu.dga.governance.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga.governance.bean.TableMetaForQuery;
import com.atguigu.dga.governance.bean.vo.TableMetaInfoVo;
import com.atguigu.dga.governance.service.TableMetaInfoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * 元数据表 前端控制器
 * </p>
 *
 * @author ff
 * @since 2023-11-18
 */
@RestController
@RequestMapping("/tableMetaInfo")
public class TableMetaInfoController {


    @Autowired
    TableMetaInfoService tableMetaInfoService;


    @GetMapping("/table-list")
    public String getTableMetaList(TableMetaForQuery tableMetaForQuery) {
        List<TableMetaInfoVo> vos  = tableMetaInfoService.getTableMetaListForQuery(tableMetaForQuery);
        long recordCount = tableMetaInfoService.getRecordCount(tableMetaForQuery);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("total",recordCount);
        jsonObject.put("list",vos);

        return jsonObject.toJSONString();
    }


}
