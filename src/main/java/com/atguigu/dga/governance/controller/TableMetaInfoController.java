package com.atguigu.dga.governance.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga.governance.bean.TableMetaForQuery;
import com.atguigu.dga.governance.bean.TableMetaInfo;
import com.atguigu.dga.governance.bean.TableMetaInfoExtra;
import com.atguigu.dga.governance.bean.vo.TableMetaInfoVo;
import com.atguigu.dga.governance.service.TableMetaInfoExtraService;
import com.atguigu.dga.governance.service.TableMetaInfoService;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Date;
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

    @Autowired
    TableMetaInfoExtraService metaInfoExtraService;


    @GetMapping("/table-list")
    public String getTableMetaList(TableMetaForQuery tableMetaForQuery) {
        List<TableMetaInfoVo> vos  = tableMetaInfoService.getTableMetaListForQuery(tableMetaForQuery);
        long recordCount = tableMetaInfoService.getRecordCount(tableMetaForQuery);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("total",recordCount);
        jsonObject.put("list",vos);

        return jsonObject.toJSONString();
    }


    @GetMapping("/table/{id}")
    public String getTableMeta(@PathVariable("id")String tableId) {
        TableMetaInfo tableMetaInfo = tableMetaInfoService.getById(tableId);
        TableMetaInfoExtra metaInfoExtra = metaInfoExtraService.getOne(new QueryWrapper<TableMetaInfoExtra>()
                .eq("schema_name", tableMetaInfo.getSchemaName())
                .eq("table_name", tableMetaInfo.getTableName())
        );
        JSONObject jsonObject = JSONObject.parseObject(JSON.toJSONString(tableMetaInfo));
        jsonObject.put("tableMetaInfoExtra",metaInfoExtra);
        return jsonObject.toJSONString();
    }


    @PostMapping("/tableExtra")
    public String saveTableExtra(@RequestBody TableMetaInfoExtra requestMetaInfoExtra) {
        requestMetaInfoExtra.setUpdateTime(new Date());
        metaInfoExtraService.saveOrUpdate(requestMetaInfoExtra);
        return "success";
    }


}
