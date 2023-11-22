package com.atguigu.dga.governance.assess.security;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga.governance.assess.Assessor;
import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import com.atguigu.dga.governance.bean.TableMetaInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

@Component("FILE_ACCESS_PERMISSION")
public class FileAccessPermissionAssessor extends Assessor {

    // 提取相关元数据  参数 。。。
    // 准备递归遍历 递归工具：FileSystem,file_permission,dir_permission  递归起点 结果容器
    // 递归起点  一级子目录
    // 递归结果的容器：收集所有超过权限的文件/目录位置
    // 递归执行
    // 循环起点目录
    // 如果遇到的是中间节点是目录
    //   采集 处理（权限检查）  展开下一层次 递归回调
    // 如果是文件   采集 处理（权限检查）
    // 根据结果容器进行评价
    //  如果有容器有内容一定是差评
    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) {
        TableMetaInfo tableMetaInfo = assessParam.getTableMetaInfo();
        String metricParamsJson = assessParam.getGovernanceMetric().getMetricParamsJson();

        JSONObject paramJsonObject = JSON.parseObject(metricParamsJson);
        String filePermission = paramJsonObject.getString("file_permission");
        String dirPermission = paramJsonObject.getString("dir_permission");


        // 准备递归
        List<String> beyondPermissionFilePath = new ArrayList<>();
        try {
            beyondPermissionFilePath = checkPermission(tableMetaInfo,filePermission,dirPermission);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (!beyondPermissionFilePath.isEmpty()) {
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);

            governanceAssessDetail.setAssessProblem("存在超过的文件或目录:" + StringUtils.join(beyondPermissionFilePath,","));
        }

    }

    private List<String> checkPermission(TableMetaInfo tableMetaInfo, String filePermission,String dirPermission) throws Exception {
        // 递归工具
        FileSystem fileSystem = FileSystem.get(new URI(tableMetaInfo.getTableFsPath()), new Configuration(), tableMetaInfo.getTableFsOwner());
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path(tableMetaInfo.getTableFsPath()));

        // 递归处理结果
        List<String> beyondPermissionFileNames = new ArrayList<>();

        // 递归执行
        checkPermissionRec(fileSystem,fileStatuses,filePermission,dirPermission,beyondPermissionFileNames);
        return beyondPermissionFileNames;

    }

    private void checkPermissionRec(FileSystem fileSystem, FileStatus[] fileStatuses, String filePermission, String dirPermission, List<String> beyondPermissionFilePath)  {
        for (FileStatus childFileStatus : fileStatuses) {
            if (childFileStatus.isDirectory()) {
                // 如果遇到的是中间节点是目录
                //   采集 处理（权限检查）  展开下一层次 递归回调

                if (isBeyondPermissionForOneFile(childFileStatus,dirPermission)) {
                    beyondPermissionFilePath.add(
                            childFileStatus.getPath().toString()
                    );
                }

                try {
                    FileStatus[] childFileStatuses = fileSystem.listStatus(childFileStatus.getPath());
                    checkPermissionRec(fileSystem, childFileStatuses, filePermission, dirPermission, beyondPermissionFilePath);
                }catch (IOException ioException) {
                    ioException.printStackTrace();
                    System.out.println("warning:" + ioException.getMessage());
                }
            }else {
                // 如果是文件   采集 处理（权限检查）
                if (isBeyondPermissionForOneFile(childFileStatus,filePermission)) {
                    beyondPermissionFilePath.add(
                            childFileStatus.getPath().toString()
                    );
                }
            }
        }
    }

    /**
     * 针对一个文件或目录进行检查
     * @param childFileStatus 文件状态信息
     * @param standPermission 标准权限，需要比较的权限
     * @return
     */
    private boolean isBeyondPermissionForOneFile(FileStatus childFileStatus, String standPermission) {
        int userAction = childFileStatus.getPermission().getUserAction().ordinal();
        int groupAction = childFileStatus.getPermission().getGroupAction().ordinal();
        int otherAction = childFileStatus.getPermission().getOtherAction().ordinal();

        Integer userParam = Integer.valueOf(standPermission.substring(0, 1));
        Integer groupParam = Integer.valueOf(standPermission.substring(1, 2));
        Integer otherParam = Integer.valueOf(standPermission.substring(2));




       return userAction > userParam ||
               groupAction > groupParam ||
               otherAction > otherParam;
    }
}
