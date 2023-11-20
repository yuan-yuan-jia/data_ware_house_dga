package com.atguigu.dga.governance.service;

import com.atguigu.dga.governance.bean.TableMetaForQuery;
import com.atguigu.dga.governance.bean.TableMetaInfo;
import com.atguigu.dga.governance.bean.vo.TableMetaInfoVo;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;

/**
 * <p>
 * 元数据表 服务类
 * </p>
 *
 * @author ff
 * @since 2023-11-18
 */
public interface TableMetaInfoService extends IService<TableMetaInfo> {

    List<TableMetaInfoVo> getTableMetaListForQuery(TableMetaForQuery tableMetaForQuery);

    long getRecordCount(TableMetaForQuery tableMetaForQuery);
}
