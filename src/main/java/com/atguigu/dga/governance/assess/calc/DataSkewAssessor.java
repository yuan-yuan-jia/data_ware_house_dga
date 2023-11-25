package com.atguigu.dga.governance.assess.calc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga.ds.bean.TDsTaskInstance;
import com.atguigu.dga.governance.assess.Assessor;
import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import com.atguigu.dga.governance.constant.CodeConstant;
import com.atguigu.dga.governance.utils.HttpUtil;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Component("DATA_SKEW")
public class DataSkewAssessor extends Assessor {

    @Value("${history-server-api-url}")
    String historyServerApiUrl;

    // 检查是否有数据倾斜，如果某个stage的最大任务耗时超过平均耗时任务耗时的{percent}%, 只检查耗时超过{stage_dur_seconds}秒的stage。
    //存在倾斜给0分，不存在给10分。

    // 需要取得最新的且成功的taskInstance
    // 从taskInstance提取appLink就是yarn app id
    // 提取成功任务的attempId
    //http://hadoop102:18080/api/v1/applications/application_1684083580862_0012
    // 获得stage列表的信息，提取stageId过滤掉只有一个task的stage，过滤掉时长特别短的stage
    //http://hadoop102:18080/api/v1/applications/application_1684083580862_0012/${attempId}/stages
    // 获得单个stage的具体信息，包括其下所有task的信息
    /// 从task中提取duration  -> stage的任务数据  最大任务耗时  其他任务耗时的平均值 = （其他任务耗时-最大任务耗时） / 任务数减一
    /// 最大的任务耗时 - 其他任务耗时的平均值  / 其他任务的平均值  如果大于参考占比则差评
    //http://hadoop102:18080/api/v1/applications/application_1684083580862_0012/${attempId}/stages/${stageid}

    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) {
        List<TDsTaskInstance> tDsTaskInstances = assessParam.getTDsTaskInstances();
        if (tDsTaskInstances == null || tDsTaskInstances.isEmpty()) {
            return;
        }
        TDsTaskInstance lastSuccessTaskInstance = null;
        for (TDsTaskInstance tDsTaskInstance : tDsTaskInstances) {
            if (tDsTaskInstance.getState().intValue() == CodeConstant.TASK_SUCCESS) {
                if (lastSuccessTaskInstance == null) {
                    lastSuccessTaskInstance = tDsTaskInstance;
                } else if (lastSuccessTaskInstance.getSubmitTime().compareTo(tDsTaskInstance.getSubmitTime()) < 0) {
                    lastSuccessTaskInstance = tDsTaskInstance;
                }
            }
        }

        if (lastSuccessTaskInstance == null) {
            governanceAssessDetail.setAssessComment("未发现成功实例");
            return;
        }


        // 提取appid
        String appId = lastSuccessTaskInstance.getAppLink();

        // 提取最新且成功attemptId
        String attemptIdByYarnId = getAttemptIdByYarnId(appId);
        if (StringUtils.isEmpty(attemptIdByYarnId)) {
            governanceAssessDetail.setAssessComment("未发现成功的任务");
            return;
        }

        //获得stage列表的信息，提取stageId过滤掉只有一个task的stage，过滤掉时长特别短的stage
        String metricParamsJson = assessParam.getGovernanceMetric().getMetricParamsJson();
        JSONObject paramJsonObject = JSON.parseObject(metricParamsJson);
        Integer stageDurSeconds = paramJsonObject.getInteger("stage_dur_seconds");
        List<String> stageIdsList = getStageIdList(appId, attemptIdByYarnId, stageDurSeconds);

        // 获得单个stage的具体信息，包括其下所有task的信息
        /// 从task中提取duration  -> stage的任务数据  最大任务耗时  其他任务耗时的平均值 = （其他任务耗时-最大任务耗时） / 任务数减一
        /// 最大的任务耗时 - 其他任务耗时的平均值  / 其他任务的平均值  如果大于参考占比则差评

        Integer paramPercent = paramJsonObject.getInteger("percent");
        List<StageInfo> stageInfoList = getStageInfoList(appId, attemptIdByYarnId, stageIdsList);
        List<String> beyondStageComments = new ArrayList<>();
        /// 最大的任务耗时 - 其他任务耗时的平均值  / 其他任务的平均值  如果大于参考占比则差评
        for (StageInfo stageInfo : stageInfoList) {
            int otherAvgDuration = (stageInfo.getTotalTaskDurationSec() - stageInfo.getMaxTaskDurationSec()) /
                    (stageInfo.getNumTask() - 1);
            Integer beyondDurationPercent = (stageInfo.getMaxTaskDurationSec() - otherAvgDuration) * 100 / otherAvgDuration;
            if (paramPercent < beyondDurationPercent) {
                beyondStageComments.add("stageId:" + stageInfo.getStageId() +
                        "超过平均其他任务时长："
                        + beyondDurationPercent +
                        ",最大任务耗时:" + stageInfo.getMaxTaskDurationSec()
                );
            }

        }

        if (!beyondStageComments.isEmpty()) {
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("存在数据倾斜:" + StringUtils.join(beyondStageComments,";"));
        }

    }

    private List<StageInfo> getStageInfoList(String appId, String attemptIdByYarnId1, List<String> stageIdsList) {

        List<StageInfo> stageInfoList = new ArrayList<>();
        for (String stageId : stageIdsList) {
            String stagesUrl = historyServerApiUrl + "/" + appId +
                    "/" +
                    attemptIdByYarnId1 +
                    "/stages/" + stageId;

            String responseBody = extractResponseBody(stagesUrl);

            List<JSONObject> stageTaskList = JSON.parseArray(responseBody, JSONObject.class);
            JSONObject stageTaskJsonObj = stageTaskList.get(0);
            JSONObject taskJsonObj = stageTaskJsonObj.getJSONObject("tasks");


            StageInfo stageInfo = new StageInfo();
            stageInfo.setStageId(stageId);


            for (Object taskJson : taskJsonObj.values()) {
                JSONObject task = (JSONObject) taskJson;
                Integer duration = task.getInteger("duration");
                stageInfo.setNumTask(stageInfo.getNumTask() + 1);
                stageInfo.setTotalTaskDurationSec(stageInfo.getTotalTaskDurationSec() + duration);
                Integer maxTaskDurationSec = stageInfo.getMaxTaskDurationSec();
                maxTaskDurationSec = maxTaskDurationSec > duration ? maxTaskDurationSec : duration;
                stageInfo.setMaxTaskDurationSec(maxTaskDurationSec);
            }

            stageInfoList.add(stageInfo);

        }
        return stageInfoList;

    }

    @Data
    static class StageInfo {
        String stageId;
        Integer TotalTaskDurationSec = 0;
        Integer maxTaskDurationSec = 0;
        Integer numTask = 0;
    }

    private List<String> getStageIdList(String appId, String attemptId, Integer stageDuration) {
        String stagesUrl = historyServerApiUrl + "/" + appId +
                "/" +
                attemptId +
                "/stages";


        String responseBody = extractResponseBody(stagesUrl);
        List<JSONObject> jsonObjects = JSON.parseArray(responseBody, JSONObject.class);
        return jsonObjects.stream()
                .filter(j -> {
                    Integer numTasks = j.getInteger("numTasks");
                    Integer executorDeserializeTime = j.getInteger("executorDeserializeTime");
                    Integer executorRunTime = j.getInteger("executorRunTime");
                    int totalTime = executorDeserializeTime + executorRunTime;
                    return (numTasks > 1 && totalTime > stageDuration);
                })
                .map(j -> {
                    return j.getString("stageId");
                }).collect(Collectors.toList());


    }


    private String extractResponseBody(String url) {
        String responseBody = null;
        try {
            responseBody = HttpUtil.get(url);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return responseBody;
    }

    private String getAttemptIdByYarnId(String yarnId) {
        String appAttemptUrl = historyServerApiUrl + "/" + yarnId;
        String responseBody = null;
        try {
            responseBody = HttpUtil.get(appAttemptUrl);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        JSONObject attempJsonObject = JSON.parseObject(responseBody);

        JSONArray attemptJsonArray = attempJsonObject.getJSONArray("attempts");
        String targetAttemptId = "";
        for (int i = 0; i < attemptJsonArray.size(); i++) {
            JSONObject attempt = attemptJsonArray.getJSONObject(i);
            if (attempt.getBoolean("completed")) { // 成功的尝试有且只有一个
                //String attemptId = attempt.getString("attemptId");
                return attempt.getString("attemptId");
//                if (targetAttemptId == null || targetAttemptId.isEmpty() || attemptId.compareTo(targetAttemptId) > 0) {
//                    targetAttemptId = attemptId;
//                }
            }
        }
        return targetAttemptId;
    }
}
