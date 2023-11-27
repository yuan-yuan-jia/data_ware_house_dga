package com.atguigu.dga.lineage.service.impl;

import com.alibaba.fastjson.JSON;
import com.atguigu.dga.ds.bean.TDsTaskDefinition;
import com.atguigu.dga.ds.service.TDsTaskDefinitionService;
import com.atguigu.dga.ds.service.TDsTaskInstanceService;
import com.atguigu.dga.governance.bean.GovernanceAssessTable;
import com.atguigu.dga.governance.bean.TableMetaInfo;
import com.atguigu.dga.governance.service.TableMetaInfoService;
import com.atguigu.dga.governance.utils.SqlParse;
import com.atguigu.dga.lineage.bean.GovernanceLineageTable;
import com.atguigu.dga.lineage.mapper.GovernanceLineageTableMapper;
import com.atguigu.dga.lineage.service.GovernanceLineageTableService;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author ff
 * @since 2023-11-27
 */
@Service
@DS("dga")
public class GovernanceLineageTableServiceImpl extends ServiceImpl<GovernanceLineageTableMapper, GovernanceLineageTable> implements GovernanceLineageTableService {


    @Autowired
    TableMetaInfoService tableMetaInfoService;


    @Autowired
    TDsTaskInstanceService taskInstanceService;

    @Autowired
    TDsTaskDefinitionService taskDefinitionService;


    //1. 要提取哪些表 -> 元数据信息 tableMetaInfoList

    //2  当天任务实例的 task的任务定义sql

    //3 解析sql  --> 提取来源表
    /// 通过遍历sql最后得到 --> 来源表关系map<String,List<String>>(key:输出表名,value:来源表的集合)
                             // 输出关系表 sinkmap<String,List<String>> key来源表名，输出表名
    //4 遍历元数据表
    /// 从来源关系表和输出关系表中提取数据
    public void extractLineageTable(String governanceDate) throws ParseException, SemanticException {

        //0. 清除当天的内容
        remove(new QueryWrapper<GovernanceLineageTable>().eq("governance_date",governanceDate));
        //1. 要提取哪些表 -> 元数据信息 tableMetaInfoList
        List<TableMetaInfo> tableMetaInfoWithExtraList = tableMetaInfoService.getTableMetaInfoWithExtraList(governanceDate);
        //2  当天任务实例的 task的任务定义sql
        Map<String, TDsTaskDefinition> taskDefinitionMap = taskDefinitionService.getTaskDefinitionMap(governanceDate);
        //3 解析sql  --> 提取来源表
        /// 通过遍历sql最后得到 --> 来源表关系map<String,List<String>>(key:输出表名,value:来源表的集合)
        // 输出关系表 sinkmap<String,List<String>> key来源表名，输出表名

        // 来源关系表
        HashMap<String, Collection<String>> sourceTableMap = new HashMap<>();
        // 输出关系表
        HashMap<String, Collection<String>> sinkTableMap = new HashMap<>();

        for (TDsTaskDefinition taskDefinition : taskDefinitionMap.values()) {
            String sql = taskDefinition.getSql();
            String[] tableNameWithSchema = taskDefinition.getName().split("\\.");
            String sinkTableSchemaName = tableNameWithSchema[0];
            if (StringUtils.isEmpty(sql) || sql.trim().isEmpty()) {
                continue;
            }

            // 解析sql
            /// 目的获取来源表的集合
            SourceTableDispatcher sourceTableDispatcher = new SourceTableDispatcher();
            sourceTableDispatcher.setDefaultSchemaName(sinkTableSchemaName);
            SqlParse.parse(sql,sourceTableDispatcher);
            Set<String> sourceTableSet = sourceTableDispatcher.getSourceTableSet();
            Set<String> subqueryTableSet = sourceTableDispatcher.getSubqueryTableSet();
            // 剩下的就是真实表
            sourceTableSet.removeAll(subqueryTableSet);
            //排除自身
            sourceTableSet.remove(taskDefinition.getName());
            String tableName = taskDefinition.getName();
            // 填来源表
            sourceTableMap.put(tableName,sourceTableSet);


            // 每一张来源表的输出 ->90行左右tableName
            for (String sourceTableName : sourceTableSet) {

                Collection<String> sinkTableNames = sinkTableMap.get(sourceTableName);
                if (sinkTableNames == null) {
                    sinkTableNames = new HashSet<>();
                    sinkTableMap.put(sourceTableName,sinkTableNames);
                }
                sinkTableNames.add(tableName);

            }

        }

        //4 遍历元数据表
        /// 从来源关系表和输出关系表中提取数据

        List<GovernanceLineageTable> lineageTables = new ArrayList<>();
        for (TableMetaInfo tableMetaInfo : tableMetaInfoWithExtraList) {
            GovernanceLineageTable governanceLineageTable = new GovernanceLineageTable();
            governanceLineageTable.setCreateTime(new Date());
            governanceLineageTable.setTableName(tableMetaInfo.getTableName());
            governanceLineageTable.setSchemaName(tableMetaInfo.getSchemaName());
            governanceLineageTable.setGovernanceDate(governanceDate);
            String tableNameWithSchema = tableMetaInfo.getSchemaName() + "." + tableMetaInfo.getTableName();
            Collection<String> sinkTables = sinkTableMap.get(tableNameWithSchema);
            Collection<String> sourceTables = sourceTableMap.get(tableNameWithSchema);
            if (sinkTables != null && !sinkTables.isEmpty()) {
                governanceLineageTable.setSinkTables(
                        JSON.toJSONString(sinkTables)
                );
            }
            if (sourceTables != null && !sourceTables.isEmpty()) {
                governanceLineageTable.setSourceTables(
                        JSON.toJSONString(sourceTables)
                );
            }
            lineageTables.add(governanceLineageTable);
        }

        if (lineageTables.isEmpty()) {
            log.warn("lineageTables is empty");
        }else {
            saveBatch(lineageTables);
        }

    }



    static class SourceTableDispatcher implements Dispatcher {


        @Setter
        String defaultSchemaName;

        // 查询来源表，含真实表和子查询表
        @Getter
        Set<String> sourceTableSet = new HashSet<>();

        // 子查询表名（临时表）
        @Getter
        Set<String> subqueryTableSet = new HashSet<>();


        @Override
        public Object dispatch(Node node, Stack<Node> stack, Object... objects) throws SemanticException {

             ASTNode astNode =(ASTNode)node;
             if (astNode.getType() == HiveParser.TOK_TABREF) {
                 // 带库名
                 if (astNode.getChild(0).getChildCount() > 1) {
                     String schemaName = astNode.getChild(0).getChild(0).getText();
                     String tableName = astNode.getChild(0).getChild(1).getText();
                     sourceTableSet.add(schemaName + "." + tableName);

                 }else if (astNode.getChild(0).getChildCount() == 1) {
                     String tableName = astNode.getChild(0).getChild(0).getText();
                     sourceTableSet.add(defaultSchemaName + "." + tableName);
                 }
             }else if (astNode.getType() == HiveParser.TOK_SUBQUERY) {
                 if (astNode.getChildCount() == 2) {
                     ASTNode subQueryNameNode = (ASTNode) astNode.getChild(1);
                     String subQueryName = subQueryNameNode.getText();
                     subqueryTableSet.add(defaultSchemaName + "." + subQueryName);
                 }
             }


            return null;
        }
    }

}
