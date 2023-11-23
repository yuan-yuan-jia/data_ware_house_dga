package com.atguigu.dga.governance.assess.quality;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga.governance.assess.Assessor;
import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import com.atguigu.dga.governance.constant.CodeConstant;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Component;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.Date;

@Component("MONITOR_TABLE_DATA_QUALITY")
public class DailyTableDataQuality extends Assessor {
    @Override
    public void checkProblem(GovernanceAssessDetail governanceAssessDetail, AssessParam assessParam) {
        String lifecycleType = assessParam.getTableMetaInfo().getTableMetaInfoExtra().getLifecycleType();
        if (CodeConstant.LIFECYCLE_TYPE_DAY.equalsIgnoreCase(lifecycleType)) {


            String metricParamsJson = assessParam.getGovernanceMetric().getMetricParamsJson();
            JSONObject paramsJsonObject = JSON.parseObject(metricParamsJson);
            BigDecimal upperLimitRate = paramsJsonObject.getBigDecimal("upper_limit");
            BigDecimal lowerLimitRate = paramsJsonObject.getBigDecimal("lower_limit");
            Integer beforeDays = paramsJsonObject.getInteger("x");


            // 计算单个日分区
            // 计算assessDate产出的数据量
            String tableFsPath = assessParam.getTableMetaInfo().getTableFsPath();
            FileSystem fileSystem = null;
            Date assessDate = null;
            try {
                fileSystem = FileSystem.get(new URI(tableFsPath),new Configuration(),assessParam.getTableMetaInfo().getTableFsOwner());
                assessDate = DateUtils.parseDate(assessParam.getAssessDate(), "yyyy-MM-dd");
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
            String assessDateDailyFilePath = tableFsPath + "/dt=" + assessParam.getAssessDate();
            BigDecimal dataSizeOnAssessDate = computeDayDataSize(fileSystem, new Path(assessDateDailyFilePath));
            // 计算前x天的平均数据量
            BigDecimal beforeDaysDataSize = new BigDecimal("0");

            Date startDayOfBeforeDays = DateUtils.addDays(assessDate,-beforeDays);

            Date currentDayToComputeDataSize = startDayOfBeforeDays;

            while (currentDayToComputeDataSize.compareTo(assessDate) < 0) {
                BigDecimal currentDayDataSize = computeDayDataSize(fileSystem, new Path(tableFsPath + "/dt=" + DateFormatUtils.format(currentDayToComputeDataSize, "yyyy-MM-dd")));
                beforeDaysDataSize = beforeDaysDataSize.add(currentDayDataSize);
                currentDayToComputeDataSize = DateUtils.addDays(currentDayToComputeDataSize,1);
            }


            BigDecimal avgDataSizeOfBeforeDays = beforeDaysDataSize
                    .divide(new BigDecimal(beforeDays),2, RoundingMode.HALF_UP);

            if (dataSizeOnAssessDate.compareTo(avgDataSizeOfBeforeDays) > 0) {
                BigDecimal overPercent = dataSizeOnAssessDate
                        .subtract(avgDataSizeOfBeforeDays)
                        .divide(avgDataSizeOfBeforeDays, 2, RoundingMode.HALF_UP);
                if (overPercent.multiply(new BigDecimal("100")).compareTo(upperLimitRate) > 0) {
                    governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
                    governanceAssessDetail.setAssessProblem("当日产品超过:" + beforeDays + "天平均产量限定:" + overPercent + "%");
                }
            }else if(dataSizeOnAssessDate.compareTo(avgDataSizeOfBeforeDays) < 0){
                BigDecimal lowPercent = avgDataSizeOfBeforeDays
                        .subtract(dataSizeOnAssessDate)
                        .divide(avgDataSizeOfBeforeDays, 2, RoundingMode.HALF_UP);

                if (lowPercent.multiply(new BigDecimal("100")).compareTo(lowerLimitRate) < 0) {
                    governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
                    governanceAssessDetail.setAssessProblem("当日产品低于:" + beforeDays + "天平均产量限定:" + lowPercent + "%");
                }
            }




        }
    }


    private BigDecimal computeDayDataSize(FileSystem fileSystem,Path filePath) {
        FileStatus[] fileStatuses = null;
        try {
            fileStatuses = fileSystem.listStatus(filePath);
        } catch (FileNotFoundException fileNotFoundException) {
            return BigDecimal.ZERO;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        BigDecimal dailyDataSize = new BigDecimal("0");
        for (FileStatus dailyFile : fileStatuses) {
            if (dailyFile.isFile() && !dailyFile.isSymlink()) {
                dailyDataSize = dailyDataSize.add(new BigDecimal(dailyFile.getLen()));
            }
            if (dailyFile.isDirectory()) {
                dailyDataSize = dailyDataSize.add(computeDayDataSize(fileSystem,dailyFile.getPath()));
            }
        }

        return dailyDataSize;

    }



}
