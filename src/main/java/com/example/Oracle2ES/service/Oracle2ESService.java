package com.example.Oracle2ES.service;

import com.example.Oracle2ES.mapper.Oracle2ESMapper;
import com.example.Oracle2ES.util.DataStore;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * <p>Title: BONC -  Oracle2ESService</p>
 * <p>Description:  </p>
 * <p>Copyright: Copyright BONC(c) 2013 - 2025 </p>
 * <p>Company: 北京东方国信科技股份有限公司 </p>
 *
 * @author zhaojie
 * @version 1.0.0
 */
@Service
public class Oracle2ESService {

    @Autowired
    Oracle2ESMapper oracle2ESMapper;

    private static Logger logger = org.apache.log4j.Logger.getLogger(Oracle2ESService.class);

    public void testImport() throws Exception{
        List<HashMap<String,String>> dataList = new ArrayList<>();
        dataList = oracle2ESMapper.testFetch();
        String indexName = "testzj";
        String typeName = "kpicodemapping";
        String id = "KPI_Code";
        DataStore.createIndex(indexName);
        DataStore.createMapping(indexName,typeName,dataList.get(0));
        DataStore.bulkDataStorage(indexName,typeName,dataList,id);
        logger.info("success");
    }

    public void kpiMappingImp()throws Exception{
        List<HashMap<String,String>> dataList = new ArrayList<>();
        dataList = oracle2ESMapper.fetchKpiMapping();
//        String indexName = "es_dw3.0_v2";
        String indexName = "es_dw3.0_v2_is_minus";
        String typeName = "K";
        String id = "KPI_Code";
        if (null != dataList && !dataList.isEmpty()){
            DataStore.createIndex(indexName);
            DataStore.createMapping(indexName,typeName,dataList.get(0));
            DataStore.bulkDataStorage(indexName,typeName,dataList,id);
            logger.info("指标映射表入数成功");
        }else {
            logger.info("指标映射表oracle查询为空");
        }
    }

    public void subjectCodeImp()throws Exception{
        List<HashMap<String,String>> dataList = new ArrayList<>();
        dataList = oracle2ESMapper.fetchSubjectCode();
//        String indexName = "es_dw3.0_v2";
        String indexName = "es_dw3.0_v2_is_minus";
        String typeName = "T";
        String id = "Subject_Code";
        if (null != dataList && !dataList.isEmpty()){
            DataStore.createIndex(indexName);
            DataStore.createMapping(indexName,typeName,dataList.get(0));
            DataStore.bulkDataStorage(indexName,typeName,dataList,id);
            logger.info("专题码表入数成功");
        }else {
            logger.info("专题码表oracle查询为空");
        }
    }

    public void reportCodeImp()throws Exception{
        List<HashMap<String,String>> dataList = new ArrayList<>();
        dataList = oracle2ESMapper.fetchReportCode();
        String indexName = "es_dw3.0_v2";
        String typeName = "R";
        String id = "Report_Code";
        if (null != dataList && !dataList.isEmpty()){
            DataStore.createIndex(indexName);
            DataStore.createMapping(indexName,typeName,dataList.get(0));
            DataStore.bulkDataStorage(indexName,typeName,dataList,id);
            logger.info("报告码表入数成功");
        }else {
            logger.info("报告码表oracle查询为空");
        }
    }
}
