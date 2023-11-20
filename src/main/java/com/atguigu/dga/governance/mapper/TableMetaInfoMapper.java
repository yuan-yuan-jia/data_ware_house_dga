package com.atguigu.dga.governance.mapper;

import com.atguigu.dga.governance.bean.TableMetaInfo;
import com.atguigu.dga.governance.bean.vo.TableMetaInfoVo;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
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
}
