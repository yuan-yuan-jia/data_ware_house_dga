package com.atguigu.dga.governance.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SimplePropertyPreFilter;
import com.atguigu.dga.governance.bean.TableMetaInfo;
import com.atguigu.dga.governance.mapper.TableMetaInfoMapper;
import com.atguigu.dga.governance.service.TableMetaInfoService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;

/**
 * <p>
 * 元数据表 服务实现类
 * </p>
 *
 * @author ff
 * @since 2023-11-18
 */
@Service
public class TableMetaInfoServiceImpl extends ServiceImpl<TableMetaInfoMapper, TableMetaInfo> implements TableMetaInfoService, InitializingBean {


    @Value("${metasore.url}")
    private String metaStoreUrl;


    IMetaStoreClient iMetaStoreClient;

    private IMetaStoreClient initMetaStoreClient() {

        try {
            HiveConf hiveConf = new HiveConf();
            hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, metaStoreUrl);

            return new HiveMetaStoreClient(hiveConf);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("初始化hiveclient失败");
        }
    }


    public void initTableMeta(String assessDate, String dbName) {
        try {
            List<String> allTableNames = iMetaStoreClient.getAllTables(dbName);

            for (String tableName : allTableNames) {

                TableMetaInfo tableMetaInfo = new TableMetaInfo();
                tableMetaInfo.setTableName(tableName);
                tableMetaInfo.setSchemaName(dbName);

                //hive元数据抽取
                extractMetaFromHive(tableMetaInfo);
                extractMetaFromHdfs(tableMetaInfo);

                System.out.println(tableMetaInfo);
            }


            System.out.println(allTableNames);
        } catch (TException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }

    private void extractMetaFromHdfs(TableMetaInfo tableMetaInfo) {


    }


    private void extractMetaFromHive(TableMetaInfo tableMetaInfo) throws TException {
        Table table = iMetaStoreClient.getTable(tableMetaInfo.getSchemaName(), tableMetaInfo.getTableName());
        List<FieldSchema> cols = table.getSd().getCols();


        //字段
        /// 字段过滤器
        SimplePropertyPreFilter simplePropertyPreFilter = new SimplePropertyPreFilter("name", "type", "comment");
        String colsJSONString = JSON.toJSONString(cols, simplePropertyPreFilter);

        tableMetaInfo.setColNameJson(colsJSONString);


        // 分区信息
        tableMetaInfo.setPartitionColNameJson(JSON.toJSONString(table.getPartitionKeys()));

        //owner
        tableMetaInfo.setTableFsOwner(table.getOwner());


        // parameters,参数信息
        tableMetaInfo.setTableParametersJson(JSON.toJSONString(table.getParameters()));

        // 表备注
        tableMetaInfo.setTableComment(table.getParameters().get("comment"));

        // 表在hdfs路径
        String location = table.getSd().getLocation();
        tableMetaInfo.setTableFsPath(location);

        // 表输入和输出格式
        String inputFormat = table.getSd().getInputFormat();
        String outputFormat = table.getSd().getOutputFormat();
        tableMetaInfo.setTableInputFormat(inputFormat);
        tableMetaInfo.setTableOutputFormat(outputFormat);
        // 序列化器
        SerDeInfo serdeInfo = table.getSd().getSerdeInfo();
        String serializationLib = serdeInfo.getSerializationLib();
        tableMetaInfo.setTableRowFormatSerde(serializationLib);

        // 表的创建时间
        Date createDate = new Date(table.getCreateTime() * 1000L);
        String createDateString = DateFormatUtils.format(createDate, "yyyy-MM-dd");

        tableMetaInfo.setTableCreateTime(createDateString);

        // 表类型
        tableMetaInfo.setTableType(table.getTableType());

        // 分桶信息
        tableMetaInfo.setTableBucketNum((long) table.getSd().getNumBuckets());
        tableMetaInfo.setTableBucketColsJson(JSON.toJSONString(table.getSd().getBucketCols(), simplePropertyPreFilter));

        tableMetaInfo.setTableSortColsJson(JSON.toJSONString(table.getSd().getSortCols()));
    }


    @Override
    public void afterPropertiesSet() throws Exception {
        iMetaStoreClient = initMetaStoreClient();
    }
}
