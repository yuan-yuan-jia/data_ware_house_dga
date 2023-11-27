package com.atguigu.dga.governance.mapper;

import com.atguigu.dga.governance.bean.GovernanceAssessGlobal;
import com.atguigu.dga.governance.bean.GovernanceAssessTable;
import com.atguigu.dga.governance.bean.GovernanceAssessTecOwner;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * <p>
 * 表治理考评情况 Mapper 接口
 * </p>
 *
 * @author ff
 * @since 2023-11-25
 */
@Mapper
@DS("dga")
public interface GovernanceAssessTableMapper extends BaseMapper<GovernanceAssessTable> {

    @Select("SELECT assess_date,\n" +
            "      table_name,\n" +
            "      schema_name,\n" +
            "      tec_owner,\n" +
            "      avg(if(governance_type = 'SPEC',assess_score,NULL))* 10  score_spec_avg,\n" +
            "      avg(if(governance_type = 'STORAGE',assess_score,NULL))*10 score_storage_avg,\n" +
            "      avg(if(governance_type = 'CALC',assess_score,NULL))*10 score_calc_avg,\n" +
            "      avg(if(governance_type = 'QUALITY',assess_score,NULL))*10 score_quality_avg,\n" +
            "      avg(if(governance_type = 'SECURITY',assess_score,NULL))*10 score_security_avg,\n" +
            "      avg(if(governance_type = 'SPEC',assess_score,NULL) * gt.type_weight / 10)  +\n" +
            "      avg(if(governance_type = 'STORAGE',assess_score,NULL) * gt.type_weight / 10) +\n" +
            "      avg(if(governance_type = 'CALC',assess_score,NULL) * gt.type_weight / 10) +\n" +
            "      avg(if(governance_type = 'QUALITY',assess_score,NULL) * gt.type_weight / 10) +\n" +
            "      avg(if(governance_type = 'SECURITY',assess_score,NULL) * gt.type_weight / 10) score_on_type_weight,\n" +
            "      sum(if(assess_score<10,1,0)) problem_num,\n" +
            "       now() create_time\n" +
            "from governance_assess_detail gd\n" +
            "join governance_type gt on gd.governance_type = gt.type_code \n" +
            "where assess_date=#{assessDate}" +
            "group by assess_date,table_name,schema_name,tec_owner")
    List<GovernanceAssessTable> getAssessTableDetail(@Param("assessDate") String assessDate);


    @Select("SELECT assess_date,\n" +
            "       tec_owner,\n" +
            "       avg(score_spec_avg) score_spec,\n" +
            "       avg(score_storage_avg) score_storage,\n" +
            "       avg(score_calc_avg) score_calc,\n" +
            "       avg(score_security_avg) score_security,\n" +
            "       avg(score_quality_avg) score_quality,\n" +
            "       avg(score_on_type_weight) score,\n" +
            "       count(*) table_num,\n" +
            "       sum(problem_num) problem_num,\n" +
            "       now() create_time    \n" +
            "from governance_assess_table\n" +
            "where assess_date = '${assess_date}'\n" +
            "GROUP by tec_owner,assess_date")
    List<GovernanceAssessTecOwner> getAssessTecOwnerByTable(@Param("assess_date") String assess_date);


    @Select("SELECT assess_date,\n" +
            "       avg(score_spec_avg) score_spec,\n" +
            "       avg(score_storage_avg) score_storage,\n" +
            "       avg(score_calc_avg) score_calc,\n" +
            "       avg(score_security_avg) score_security,\n" +
            "       avg(score_quality_avg) score_quality,\n" +
            "       avg(score_on_type_weight) score,\n" +
            "       count(*) table_num,\n" +
            "       sum(problem_num) problem_num,\n" +
            "       now() create_time    \n" +
            "from governance_assess_table\n" +
            "where assess_date = '${assess_date}'")
    GovernanceAssessGlobal getAssessGlobalByTable(@Param("assess_date") String assess_date);

}
