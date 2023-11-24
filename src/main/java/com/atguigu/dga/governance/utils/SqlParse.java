package com.atguigu.dga.governance.utils;

import lombok.Data;
import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.*;

import java.util.Collections;
import java.util.List;
import java.util.Stack;

public class SqlParse {

    public static void parse(String sql, Dispatcher dispatcher) throws ParseException, SemanticException {
        // 创建一个sql解析工具
        ParseDriver parseDriver = new ParseDriver();
        // 用sql解析工具分析sql生成语法树
        ASTNode astNode = parseDriver.parse(sql);
        // 把语法树根节点剪除
        ASTNode startNode = (ASTNode) astNode.getChild(0);
        // 创建遍历器 同时把节点处理器放入遍历器
        DefaultGraphWalker defaultGraphWalker = new DefaultGraphWalker(dispatcher);
        // 遍历器进行节点遍历
        defaultGraphWalker.startWalking(Collections.singleton(startNode), null);
    }

    public static void main(String[] args) throws ParseException, SemanticException {
        String sql = "select tmp.id,tmp.name,t2.ti from tmp join t2 on tmp.id = t2.id";
        JoinDispatcher joinDispatcher = new JoinDispatcher();
        parse(sql,joinDispatcher);
        joinDispatcher.isHasJoin();
    }

    @Data
    static class JoinDispatcher implements Dispatcher {


        boolean hasJoin = false;

        @Override
        public Object dispatch(Node node, Stack<Node> stack, Object... objects) throws SemanticException {
            ASTNode astNode = (ASTNode) node;

            System.out.println(astNode.getText());
            if (astNode.getType() == HiveParser.TOK_JOIN) {
                hasJoin = true;
            }
            return null;
        }
    }
}
