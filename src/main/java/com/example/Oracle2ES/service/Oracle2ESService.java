package com.example.Oracle2ES.service;

import com.example.Oracle2ES.mapper.Oracle2ESMapper;
import com.example.Oracle2ES.util.DataStore;
import com.example.Oracle2ES.util.GenerateClient;
import org.apache.log4j.Logger;
import org.elasticsearch.client.transport.TransportClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.net.UnknownHostException;
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
    private static final String IS_INDEX = "es_intelligent_search";
    private static final String KPI_TYPE = "K";
    private static final String SUBJECT_TYPE = "T";
    private static final String REPORT_TYPE = "R";
    private static final String REPORT_TABLE_TYPE = "RT";

    private static final String KPI_ID = "KPI_Code";
    private static final String SUBJECT_ID = "Subject_Code";
    private static final String REPORT_ID = "Report_Code";
    private static final String REPORT_TABLE_ID = "ReportTable_Code";

    private static final HashMap<String,String> defaultReportMap = new HashMap<>();
    static {
        defaultReportMap.put("Report_Code","1");
        defaultReportMap.put("Report_Name","1");
        defaultReportMap.put("Acct_Type","1");
        defaultReportMap.put("Report_Name_Length","1");

    }
    private static final HashMap<String,String> defaultReporTabletMap = new HashMap<>();
    static {
        defaultReporTabletMap.put("ReportTable_Code","1");
        defaultReporTabletMap.put("ReportTable_Name","1");
        defaultReporTabletMap.put("ReportTable_Desc","1");
        defaultReporTabletMap.put("Acct_Type","1");
        defaultReporTabletMap.put("ReportTable_Name_Length","1");

    }

    public void kpiMappingImp()throws Exception{
        List<HashMap<String,String>> dataList = new ArrayList<>();
        dataList = oracle2ESMapper.fetchKpiMapping();
        if (null != dataList && !dataList.isEmpty()){
            DataStore.createIndex(IS_INDEX);
            DataStore.createMapping(IS_INDEX,KPI_TYPE,dataList.get(0));
            DataStore.bulkDataStorage(IS_INDEX,KPI_TYPE,dataList,KPI_ID);
            logger.info("指标映射表入数成功");
        }else {
            logger.info("指标映射表oracle查询为空");
        }
    }

    public void subjectCodeImp()throws Exception{
        List<HashMap<String,String>> dataList = new ArrayList<>();
        dataList = oracle2ESMapper.fetchSubjectCode();
        if (null != dataList && !dataList.isEmpty()){
            DataStore.createIndex(IS_INDEX);
            DataStore.createMapping(IS_INDEX,SUBJECT_TYPE,dataList.get(0));
            DataStore.bulkDataStorage(IS_INDEX,SUBJECT_TYPE,dataList,SUBJECT_ID);
            logger.info("专题码表入数成功");
        }else {
            logger.info("专题码表oracle查询为空");
        }
    }

    public void reportCodeImp()throws Exception{
        List<HashMap<String,String>> dataList = new ArrayList<>();
        dataList = oracle2ESMapper.fetchReportCode();
        if (null != dataList && !dataList.isEmpty()){
            DataStore.createIndex(IS_INDEX);
            DataStore.createMapping(IS_INDEX,REPORT_TYPE,dataList.get(0));
            DataStore.bulkDataStorage(IS_INDEX,REPORT_TYPE,dataList,REPORT_ID);
            logger.info("报告码表入数成功");
        }else {
            logger.info("报告码表oracle查询为空");
        }
    }

    public void reportTableCodeImp()throws Exception{
        List<HashMap<String,String>> dataList = new ArrayList<>();
        dataList = oracle2ESMapper.fetchReportTableCode();
        if (null != dataList && !dataList.isEmpty()){
            DataStore.createIndex(IS_INDEX);
            DataStore.createMapping(IS_INDEX,REPORT_TABLE_TYPE,dataList.get(0));
            DataStore.bulkDataStorage(IS_INDEX,REPORT_TABLE_TYPE,dataList,REPORT_TABLE_ID);
            logger.info("报表码表入数成功");
        }else {
            logger.info("报表码表oracle查询为空");
        }
    }

    public void dimensionImport()throws Exception{
//        List<HashMap<String,String>> channelList = oracle2ESMapper.fetchChannel();
//        List<HashMap<String,String>> productList = oracle2ESMapper.fetchProduct();
//        List<HashMap<String,String>> serviceList = oracle2ESMapper.fetchService();
        List<HashMap<String,String>> dimensionList = oracle2ESMapper.fetchDimension();
        List<HashMap<String,String>> provList = oracle2ESMapper.fetchPro();
        List<HashMap<String,String>> areaList = oracle2ESMapper.fetchArea();
        String indexName = "dimension_search";
        String typeName = "dimension";
        String id = "code";
        DataStore.createZCIndex(indexName);
        DataStore.createZCMapping(indexName,typeName,areaList.get(0));
//        DataStore.bulkDataStorage(indexName,typeName,channelList,id);
//        DataStore.bulkDataStorage(indexName,typeName,productList,id);
//        DataStore.bulkDataStorage(indexName,typeName,serviceList,id);
        DataStore.bulkDataStorage(indexName,typeName,dimensionList,id);
        DataStore.bulkDataStorage(indexName,typeName,provList,id);
        DataStore.bulkDataStorage(indexName,typeName,areaList,id);
    }

    public void clearThenImport() throws Exception{
        TransportClient transportClient = null;
        try {
            transportClient = GenerateClient.getClient();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        transportClient.admin().indices().prepareDelete(IS_INDEX).execute().actionGet();
        logger.info("成功删除索引");
        List<HashMap<String,String>> kpiList = oracle2ESMapper.fetchKpiMapping();
        List<HashMap<String,String>> subjectList = oracle2ESMapper.fetchSubjectCode();
        List<HashMap<String,String>> reportList = oracle2ESMapper.fetchReportCode();
        List<HashMap<String,String>> reportTableList = oracle2ESMapper.fetchReportTableCode();

        DataStore.createIndex(IS_INDEX);
        if (null != kpiList && !kpiList.isEmpty()){
            DataStore.createMapping(IS_INDEX,KPI_TYPE,kpiList.get(0));
            DataStore.bulkDataStorage(IS_INDEX,KPI_TYPE,kpiList,KPI_ID);
        }
        if (null != subjectList && !subjectList.isEmpty()){
            DataStore.createMapping(IS_INDEX,SUBJECT_TYPE,subjectList.get(0));
            DataStore.bulkDataStorage(IS_INDEX,SUBJECT_TYPE,subjectList,SUBJECT_ID);
        }
        DataStore.createMapping(IS_INDEX,REPORT_TYPE,defaultReportMap);
        if (null != reportList && !reportList.isEmpty()){
            DataStore.bulkDataStorage(IS_INDEX,REPORT_TYPE,reportList,REPORT_ID);
        }
        DataStore.createMapping(IS_INDEX,REPORT_TABLE_TYPE,defaultReporTabletMap);
        if (null != reportTableList && !reportTableList.isEmpty()){
            DataStore.bulkDataStorage(IS_INDEX,REPORT_TABLE_TYPE,reportTableList,REPORT_TABLE_ID);
        }
    }
}
