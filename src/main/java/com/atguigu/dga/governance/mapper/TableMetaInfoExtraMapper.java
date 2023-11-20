package com.atguigu.dga.governance.mapper;

import com.atguigu.dga.governance.bean.TableMetaInfoExtra;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/**
 * <p>
 * 元数据表附加信息 Mapper 接口
 * </p>
 *
 * @author ff
 * @since 2023-11-20
 */
@Mapper
@DS("dga")
public interface TableMetaInfoExtraMapper extends BaseMapper<TableMetaInfoExtra> {

}
