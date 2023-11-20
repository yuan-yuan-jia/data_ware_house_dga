package com.atguigu.dga.governance.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SimplePropertyPreFilter;
import com.atguigu.dga.governance.bean.TableMetaInfo;
import com.atguigu.dga.governance.mapper.TableMetaInfoMapper;
import com.atguigu.dga.governance.service.TableMetaInfoService;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.extension.service.IService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.springframework.aop.framework.AopContext;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.LinkedList;
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
@DS("dga")
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
            List<TableMetaInfo> tableMetaInfos = new LinkedList<>();
            for (String tableName : allTableNames) {

                TableMetaInfo tableMetaInfo = new TableMetaInfo();
                tableMetaInfo.setTableName(tableName);
                tableMetaInfo.setSchemaName(dbName);

                //hive元数据抽取
                extractMetaFromHive(tableMetaInfo);
                //hdfs元数据抽取
                try {
                    extractMetaFromHdfs(tableMetaInfo);
                }catch (FileNotFoundException e) {
                    System.out.println("File Not be Found:" + e.getMessage());
                }
                // 补充时间信息
                tableMetaInfo.setAssessDate(assessDate);
                tableMetaInfo.setCreateTime(new Date());
                System.out.println(tableMetaInfo);
                tableMetaInfos.add(tableMetaInfo);

            }

            if (!tableMetaInfos.isEmpty()) {
                //Object o = AopContext.currentProxy();
                //((TableMetaInfoService)o).saveOrUpdateBatch(tableMetaInfos);
                saveOrUpdateBatch(tableMetaInfos);
            }

            // 补充辅助信息表

            System.out.println(allTableNames);
        } catch (TException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    private void extractMetaFromHdfs(TableMetaInfo tableMetaInfo) throws URISyntaxException, IOException, InterruptedException {

        //1. 准备阶段

        //1.2 访问工具：对不同文件系统的访问
        FileSystem fileSystem = FileSystem.get(new URI(tableMetaInfo.getTableFsPath()), new Configuration(), tableMetaInfo.getTableFsOwner());

        //1.1 递归的起点:一级子目录
        FileStatus[] firstChildFileStatus = fileSystem.listStatus(new Path(tableMetaInfo.getTableFsPath()));

        //1.3 收集数据的容器:@TableMetaInfo

        /// 进行递归操作
        getMetaFromHdfsRecurve(fileSystem, firstChildFileStatus, tableMetaInfo);
        System.out.println(tableMetaInfo.getTableName() + ":tableSize" + tableMetaInfo.getTableSize());

        //1.4 赋予环境信息
        /// 文件系统总容量
        tableMetaInfo.setFsCapcitySize(fileSystem.getStatus().getCapacity());
        tableMetaInfo.setFsRemainSize(fileSystem.getStatus().getRemaining());
        tableMetaInfo.setFsUsedSize(fileSystem.getStatus().getUsed());
    }

    // 递归过程
    private void getMetaFromHdfsRecurve(FileSystem fileSystem, FileStatus[] firstChildFileStatus, TableMetaInfo tableMetaInfo) throws IOException {

        //2. 遍历阶段
        /// 遍历一级子目录
        for (FileStatus childFileStatus : firstChildFileStatus) {



            if (childFileStatus.isDirectory()) {
                //2.1 遍历到的节点是中间节点：处理计算 收集数据  下探展开：递归的回调
                /// 本项目不需要收集数据和计算
                /// 下探展开

                try {
                FileStatus[] childrenFileStatus = fileSystem.listStatus(childFileStatus.getPath());

                    getMetaFromHdfsRecurve(fileSystem, childrenFileStatus, tableMetaInfo);
                }catch (FileNotFoundException e) {
                    System.out.println("warning :" + e.getMessage());
                }
            }else {
                //2.2 是叶子节点：处理计算 收集数据 返回
                // 结合副本数统计大小
                long fileLen = childFileStatus.getLen();
                tableMetaInfo.setTableSize(tableMetaInfo.getTableSize() == null?0L:tableMetaInfo.getTableSize() * childFileStatus.getReplication() + fileLen);
                // 最后修改时间
                long modificationTime = childFileStatus.getModificationTime();

                Date tableLastModifyTime = tableMetaInfo.getTableLastModifyTime();
                if (tableLastModifyTime == null || tableLastModifyTime.getTime() < modificationTime) {
                    tableMetaInfo.setTableLastModifyTime(new Date(modificationTime));
                }
                // 最后访问时间
                long accessTime = childFileStatus.getAccessTime();

                Date tableAccessTime = tableMetaInfo.getTableLastAccessTime();
                if (tableAccessTime == null || tableAccessTime.getTime() < accessTime) {
                    tableMetaInfo.setTableLastAccessTime(new Date(accessTime));
                }

            }




        }
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
