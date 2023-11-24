package com.atguigu.dga.governance.assess.calc;

import com.atguigu.dga.ds.bean.TDsTaskDefinition;
import com.atguigu.dga.governance.assess.Assessor;
import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import com.atguigu.dga.governance.utils.SqlParse;
import com.google.common.collect.Sets;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.springframework.stereotype.Component;

import java.util.*;

@Component("IS_SIMPLE_PROCESS")
public class IsSimpleProcessAssessor extends Assessor {


    // 要把sql提取为语法树对象
    // 利用遍历器进行遍历
    // 实现一个节点处理器 要实现采集信息的方法
    /// 采集是所有join/group by/union
    /// 采集该表的where后面的字段
    // 根据采集结果进行判断
    /// 是否有复杂字段
    /// 如果没有复杂计算 把where过滤的字段和表分区字段进行比较
    ///- 如果都是分区字段则给差评

    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) {
        TDsTaskDefinition tDsTaskDefinition = assessParam.getTDsTaskDefinition();
        String sql = tDsTaskDefinition.getSql();
        if (StringUtils.isEmpty(sql) || sql.trim().isEmpty()) {
            return;
        }

        String schemaName = assessParam.getTableMetaInfo().getSchemaName();
        CheckSimpleDispatcher checkSimpleDispatcher = new CheckSimpleDispatcher();
        checkSimpleDispatcher.setDefaultSchemaName(schemaName);
        try {
            SqlParse.parse(sql,checkSimpleDispatcher);
        } catch (ParseException | SemanticException e) {
            throw new RuntimeException(e);
        }

        System.out.println();


    }

    static class CheckSimpleDispatcher implements Dispatcher {

        Set<String> complicationTokenSet = new HashSet<>();
        Set<String> fromTableSet = new HashSet<>();

        Set<String> filedAfterWhereTokenSet = new HashSet<>();



        final Set<Integer> complicationTypeSet = Sets.newHashSet( HiveParser.TOK_GROUPBY,       //  group by
                HiveParser.TOK_LEFTOUTERJOIN,       //  left join
                HiveParser.TOK_RIGHTOUTERJOIN,     //   right join
                HiveParser.TOK_FULLOUTERJOIN,     // full join
                HiveParser.TOK_FUNCTION,     //count(1)
                HiveParser.TOK_FUNCTIONDI,  //count(distinct xx)
                HiveParser.TOK_FUNCTIONSTAR, // count(*)
                HiveParser.TOK_SELECTDI,  // distinct
                HiveParser.TOK_UNIONALL   // union
        );


        @Setter
        String defaultSchemaName = "";


        @Override
        public Object dispatch(Node node, Stack<Node> stack, Object... objects) throws SemanticException {

            // 实现一个节点处理器 要实现采集信息的方法
            /// 采集是所有join/group by/union
            // 提取出来源表
            /// 采集该表的where后面的字段
             ASTNode astNode = (ASTNode)node;

             // 1 收集sql中复杂操作
             if (complicationTypeSet.contains(astNode.getType())) {
                 complicationTokenSet.add(astNode.getText());
             }

             // 2 提取来源表
            if (astNode.getType() == HiveParser.TOK_TABREF) {
                ASTNode tokenTableNameNode = (ASTNode)astNode.getChild(0);
                if (tokenTableNameNode.getChildCount() == 1) {
                     ASTNode tableNameNodeChild = (ASTNode)tokenTableNameNode.getChild(0);
                     fromTableSet.add(defaultSchemaName + "." + tableNameNodeChild.getText());
                }else if(tokenTableNameNode.getChildCount() == 2){
                    ASTNode schemaNameNodeChild = (ASTNode)tokenTableNameNode.getChild(0);
                    ASTNode tableNameNodeChild = (ASTNode)tokenTableNameNode.getChild(1);
                    fromTableSet.add(schemaNameNodeChild.getText() + "." + tableNameNodeChild.getText());
                }

                // 采集 该表的where后面的字段
            }else if (astNode.getType() == HiveParser.TOK_TABLE_OR_COL &&
               astNode.getAncestor(HiveParser.TOK_WHERE) != null
            ) {

                // 根据父级判断字段
                ///父级是"."，子级就是表名，要去兄弟节点
                if (astNode.getParent().getType() == HiveParser.DOT) {
                     ASTNode colNode = (ASTNode)astNode.getParent().getChild(1);
                     filedAfterWhereTokenSet.add(colNode.getText());
                }else {
                    // 否则取子级
                    filedAfterWhereTokenSet.add(astNode.getChild(0).getText());
                }


            }


            return null;
        }
    }
}
