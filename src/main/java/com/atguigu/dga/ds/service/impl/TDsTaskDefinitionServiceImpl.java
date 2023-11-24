package com.atguigu.dga.ds.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga.ds.bean.TDsTaskDefinition;
import com.atguigu.dga.ds.mapper.TDsTaskDefinitionMapper;
import com.atguigu.dga.ds.service.TDsTaskDefinitionService;
import com.atguigu.dga.governance.constant.CodeConstant;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author ff
 * @since 2023-11-22
 */
@Service
@DS("dl")
public class TDsTaskDefinitionServiceImpl extends ServiceImpl<TDsTaskDefinitionMapper, TDsTaskDefinition> implements TDsTaskDefinitionService {

    @Override
    public Map<String, TDsTaskDefinition> getTaskDefinitionMap(String assessDate) {
        // 当天运行最后一次实例对应的任务定义取出来 map<表名,任务定义>

//         select * from t_ds_task_definition where code in
//         (select task_code from t_ds_task_instance t1 where id = (
//          select max(id) from t_ds_task_instance t2 where t1.name = t2.name
//           and date_format(submit_time,'%Y-%m-d') = ''
//           and state = 7
        // ) )

//        QueryWrapper<TDsTaskDefinition> queryWrapper = new QueryWrapper<TDsTaskDefinition>().inSql("code", "select task_code from t_ds_task_instance t1 where id = (" +
//                "select max(id) from t_ds_task_instance t2 where t1.name = t2.name" +
//                "and date_format(submit_time,'%Y-%m-%d') = '" + assessDate +
//                "'  and state =" + CodeConstant.TASK_SUCCESS + ")"
//        );
//
//        queryWrapper.getSqlSelect();

        List<TDsTaskDefinition> taskDefinitions = this.baseMapper.getTaskDefinitionOfInstanceOfSuccessOnAssessDate(assessDate, CodeConstant.TASK_SUCCESS);

        for (TDsTaskDefinition taskDefinition : taskDefinitions) {
            String sql = "";
            try {
                sql = getSQLFromParam(taskDefinition.getTaskParams());
            }catch (IllegalArgumentException e) {
                sql = "";
            }
            taskDefinition.setSql(sql);
        }

        return taskDefinitions.stream().collect(Collectors.toMap(TDsTaskDefinition::getName, taskDefinition -> taskDefinition));


    }

    private String getSQLFromParam(String taskParams) {
        // 把taskParam转jsonOb
        JSONObject jsonObject = JSON.parseObject(taskParams);
        String rawScript = jsonObject.getString("rawScript");

        rawScript = rawScript.replaceAll("\n","   ");
        // 从shell中切割处sql
        // 起始点
        int startIdx = rawScript.indexOf("with");
        if (startIdx == -1) {
            // 未找到
            startIdx = rawScript.indexOf("insert");
        }

        if (startIdx == -1) {
            throw new IllegalArgumentException("");
        }


        // 结束点
        int endIndex = rawScript.indexOf(";",startIdx);
        if (endIndex == -1) {
            endIndex = rawScript.indexOf("\"",startIdx);
        }
        if (endIndex == -1) {
            throw new IllegalArgumentException("");
        }

        return rawScript.substring(startIdx,endIndex);


    }
}
