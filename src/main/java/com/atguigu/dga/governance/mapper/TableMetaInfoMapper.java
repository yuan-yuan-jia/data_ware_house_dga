package com.atguigu.dga.governance.mapper;

import com.atguigu.dga.governance.bean.TableMetaInfo;
import com.atguigu.dga.governance.bean.vo.TableMetaInfoVo;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.ResultMap;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * <p>
 * 元数据表 Mapper 接口
 * </p>
 *
 * @author ff
 * @since 2023-11-18
 */
@Mapper
@DS("dga")
public interface TableMetaInfoMapper extends BaseMapper<TableMetaInfo> {

    @Select("${sql}")
    List<TableMetaInfoVo> getTableMetaInfoListForQuery(@Param("sql") String sql);

    @Select("${sql}")
    long getRecordCount(@Param("sql") String sql);

    @Select("select tm.id tm_id,te.id te_id,tm.create_time tm_create_time,tm.update_time tm_update_time,te.create_time te_create_time,te.update_time te_update_time,tm.*,te.*  " +
            " from table_meta_info tm join table_meta_info_extra te " +
     " on tm.table_name = te.table_name and tm.schema_name = te.schema_name" +
            " where assess_date=#{assessDate}"
    )
    @ResultMap("table_meta_rs_map")
    List<TableMetaInfo> getTableMetaInfoWithExtraList(@Param("assessDate") String assessDate);
}
