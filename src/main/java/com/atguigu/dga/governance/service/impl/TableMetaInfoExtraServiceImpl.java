package com.atguigu.dga.governance.service.impl;

import com.atguigu.dga.governance.bean.TableMetaInfo;
import com.atguigu.dga.governance.bean.TableMetaInfoExtra;
import com.atguigu.dga.governance.constant.CodeConstant;
import com.atguigu.dga.governance.mapper.TableMetaInfoExtraMapper;
import com.atguigu.dga.governance.service.TableMetaInfoExtraService;
import com.atguigu.dga.governance.service.TableMetaInfoService;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * <p>
 * 元数据表附加信息 服务实现类
 * </p>
 *
 * @author ff
 * @since 2023-11-20
 */
@Service
@DS("dga")
public class TableMetaInfoExtraServiceImpl extends ServiceImpl<TableMetaInfoExtraMapper, TableMetaInfoExtra> implements TableMetaInfoExtraService {

    @Autowired
    TableMetaInfoService tableMetaInfoService;

    public void initTableMetaInfoExtra(String assessDate) {
        // 要查询哪些表需要初始化辅助信息，只有没有辅助信息的表，才需要初始化
        QueryWrapper<TableMetaInfo> assessDate1 = new QueryWrapper<TableMetaInfo>()
                .eq("assess_date", assessDate)
                .notInSql("concat(schema_name ,',',table_name)", "select concat(schema_name,',',table_name) from table_meta_info_extra");

        List<TableMetaInfo> tableMetaInfoNonExistsExtraList = tableMetaInfoService.list(assessDate1);
        List<TableMetaInfoExtra> needInitInfoExtra = new ArrayList<>(tableMetaInfoNonExistsExtraList.size());

        for (TableMetaInfo tableMetaInfo : tableMetaInfoNonExistsExtraList) {
            TableMetaInfoExtra tableMetaInfoExtra = new TableMetaInfoExtra();
            tableMetaInfoExtra.setTableName(tableMetaInfo.getTableName());
            tableMetaInfoExtra.setSchemaName(tableMetaInfo.getSchemaName());

            tableMetaInfoExtra.setBusiOwnerUserName("未填写");
            tableMetaInfoExtra.setTecOwnerUserName("未填写");
            // 分层信息
            tableMetaInfoExtra.setDwLevel(getDwLevel(tableMetaInfo.getTableName()));
            // 安全级别
            tableMetaInfoExtra.setSecurityLevel(CodeConstant.SECURITY_LEVEL_UNSET);

            // 生命周期类型
            tableMetaInfoExtra.setLifecycleType(CodeConstant.LIFECYCLE_TYPE_UNSET);

            // 生命周期天数
            tableMetaInfoExtra.setLifecycleDays(-1L);

            tableMetaInfoExtra.setCreateTime(new Date());

            needInitInfoExtra.add(tableMetaInfoExtra);


        }
        saveOrUpdateBatch(needInitInfoExtra);
    }

    private String getDwLevel(String tableName) {
        if(tableName.startsWith("ods")){
            return "ODS";
        } else if (tableName.startsWith("dwd")) {
            return "DWD";
        }else if (tableName.startsWith("dim")) {
            return "DIM";
        }else if (tableName.startsWith("dws")) {
            return "DWS";
        }else if (tableName.startsWith("ads")) {
            return "ADS";
        }else if (tableName.startsWith("dm")) {
            return "DM";
        }else  {
            return "OTHER";
        }
    }

}
