package com.atguigu.dga.lineage.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga.governance.bean.TableMetaInfo;
import com.atguigu.dga.governance.service.TableMetaInfoService;
import com.atguigu.dga.lineage.bean.GovernanceLineageTable;
import com.atguigu.dga.lineage.service.GovernanceLineageTableService;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * <p>
 * 前端控制器
 * </p>
 *
 * @author ff
 * @since 2023-11-27
 */
@RestController
@RequestMapping("/lineage")
@Slf4j
public class GovernanceLineageTableController {


    @Autowired
    GovernanceLineageTableService lineageTableService;

    @Autowired
    TableMetaInfoService tableMetaInfoService;



    @GetMapping("/root/{tableName}")
    public String getLineageRoot(@PathVariable String tableName) {

        // 查询该表的血缘对象
        String[] tableNameWithSchema = tableName.split("\\.");
        String schemaName = tableNameWithSchema[0];
        String tableName1 = tableNameWithSchema[1];

        QueryWrapper<GovernanceLineageTable> queryWrapper = new QueryWrapper<GovernanceLineageTable>()
                .eq("schema_name", schemaName)
                .eq("table_name", tableName1)
                .inSql("governance_date",
                        "select max(governance_date) from governance_lineage_table"
                );
        GovernanceLineageTable lineageTable = lineageTableService.getOne(queryWrapper);
        if (lineageTable == null) {
            log.warn(tableName + "is not exists");
            return "{error:ee}";
        }
        //提取所有表的元数据
        List<TableMetaInfo> tableMetaInfoWithExtraList = tableMetaInfoService.getTableMetaInfoWithExtraList(lineageTable.getGovernanceDate());
        Map<String, TableMetaInfo> tableNameToMetaInfo = tableMetaInfoWithExtraList.stream()
                .collect(Collectors.toMap((t) -> {
                    return t.getSchemaName() + "." + t.getTableName();
                }, i -> i));

        // 封装根节点数据
        JSONObject result = new JSONObject();
        result.put("isRoot",true);
        result.put("id",tableName);
        TableMetaInfo tableMetaInfo = tableNameToMetaInfo.get(tableName);
        String tableComment = tableMetaInfo.getTableComment();
        result.put("comment",tableComment);
        String sinkTables = lineageTable.getSinkTables();
        String sourceTables = lineageTable.getSourceTables();
        // 准备子节点集合
        List<JSONObject> children = new ArrayList<>();
        if (!StringUtils.isEmpty(sourceTables)) {

            List<String> sourceTableNameArray = JSON.parseArray(sourceTables, String.class);

            for (String sourceTableName : sourceTableNameArray) {
                TableMetaInfo sourceTableMetaInfo = tableNameToMetaInfo.get(sourceTableName);
                if (sourceTableMetaInfo == null) {
                    log.warn(sourceTableName + ": do not have metaInfo");
                }else {
                    JSONObject jsonObject = new JSONObject();
                    jsonObject.put("id",sourceTableName);
                    jsonObject.put("relation","source");
                    jsonObject.put("comment",sourceTableMetaInfo.getTableComment());
                    children.add(jsonObject);
                }
            }
        }

        if (!StringUtils.isEmpty(sinkTables)) {

            List<String> sinkTableNameArray = JSON.parseArray(sinkTables, String.class);

            for (String sinkTableName : sinkTableNameArray) {
                TableMetaInfo sinkTableMetaInfo = tableNameToMetaInfo.get(sinkTableName);
                if (sinkTableMetaInfo == null) {
                    log.warn(sinkTableName + ": do not have metaInfo");
                }else {
                    JSONObject jsonObject = new JSONObject();
                    jsonObject.put("id",sinkTableName);
                    jsonObject.put("relation","sink");
                    jsonObject.put("comment",sinkTableMetaInfo.getTableComment());
                    children.add(jsonObject);
                }
            }
        }

        result.put("children",children);


        return result.toJSONString();
    }




}
