package com.atguigu.dga.governance.service;

import com.atguigu.dga.governance.bean.TableMetaInfoExtra;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 * 元数据表附加信息 服务类
 * </p>
 *
 * @author ff
 * @since 2023-11-20
 */
public interface TableMetaInfoExtraService extends IService<TableMetaInfoExtra> {

    void initTableMetaInfoExtra(String assessDate);
}
