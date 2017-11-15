package com.example.Oracle2ES.util;


import org.apache.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * <p>Title: BONC -  DataStore</p>
 * <p>Description:  </p>
 * <p>Copyright: Copyright BONC(c) 2013 - 2025 </p>
 * <p>Company: 北京东方国信科技股份有限公司 </p>
 *
 * @author zhaojie
 * @version 1.0.0
 */
public class DataStore {

    private static Logger logger = Logger.getLogger(DataStore.class);

    /**
     * bulk批量导入
     * @param indexName
     * @param typeName
     * @param dataList
     * @param id 指定 _id
     */
    public static void bulkDataStorage(String indexName,String typeName,List<HashMap<String,String>> dataList,String id){
        TransportClient transportClient = null;
        try {
            transportClient = GenerateClient.getClient();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        BulkProcessor bulkProcessor = BulkProcessor.builder(transportClient,
                new BulkProcessor.Listener(){
                    //可以从BulkRequest中获取请求信息request.requests()或者请求数量request.numberOfActions()。
                    @Override
                    public void beforeBulk(long executionId,
                                           BulkRequest request) {}
                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          BulkResponse response) {
                        logger.info("executionId "+executionId);
                        logger.info("提交" + response.getItems().length + "个文档，用时"
                                + response.getTookInMillis() + "毫秒"
                                + (response.hasFailures() ? " 有文档提交失败！" : ""));
                        logger.info("response"+response.buildFailureMessage());
                    }
                    //出错时执行
                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          Throwable failure) {
                        logger.info(" 有文档提交失败！after failure=" + failure);
                    }
                })
                //当请求超过10000个（default=1000）或者总大小超过1GB（default=5MB）时，触发批量提交动作。另外每隔5秒也会提交一次（默认不会根据时间间隔提交）。
                .setBulkActions(10000)
                .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(1)
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();
        long startTime = System.currentTimeMillis();
        int count = 0;
        for (HashMap<String,String> map:dataList){
            count++;
            try {
                XContentBuilder data = XContentFactory.jsonBuilder().startObject();
                Iterator iterator = map.entrySet().iterator();
                while (iterator.hasNext()){
                    Map.Entry entry = (Map.Entry)iterator.next();
                    if (((String) entry.getKey()).equals("KPI_Name_Length")){
                        data = data.field((String) entry.getKey(), map.get("KPI_Name").toString().length());
                    }else if(((String) entry.getKey()).equals("Subject_Name_Length")){
                        data = data.field((String) entry.getKey(), map.get("Subject_Name").toString().length());
                    }else if (((String) entry.getKey()).equals("Report_Name_Length")){
                        data = data.field((String) entry.getKey(), map.get("Report_Name").toString().length());
                    }else {
                        data = data.field((String) entry.getKey(),(String) entry.getValue());
                    }
                }
                String dataStr = data.endObject().string();
                //此处指定id
                IndexRequest indexrequest = new IndexRequest(indexName,typeName,map.get(id)).source(dataStr);
                bulkProcessor.add(indexrequest);
            } catch (IOException e) {
                e.printStackTrace();
            }
            if(count%10000==0)
            {
                logger.info("已提交："+count/10000);
                bulkProcessor.flush();
            }
        }
        long endTime = System.currentTimeMillis();
        logger.info("耗时" + (endTime - startTime) /1000 + "秒，包含" + count + "行");
        try {
            bulkProcessor.awaitClose(10, TimeUnit.MINUTES);
            bulkProcessor.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        transportClient.close();
    }

    /**
     * 创建映射
     * 创建mapping(feid("indexAnalyzer","ik")该字段分词IK索引 ；feid("searchAnalyzer","ik")该字段分词ik查询；具体分词插件请看IK分词插件说明)
     * @param indexName 索引名
     * @param typeName 类型名
     * @throws Exception
     */
    public static void createMapping(String indexName, String typeName, HashMap<String,String> paramMap){
        PutMappingRequest request = null;
        HashMap<String,Object> fieldMap = new HashMap<>();
        HashMap<String,String> pinyinMap=new HashMap<>();
        pinyinMap.put("type","text");
        pinyinMap.put("analyzer","pinyin_analyzer");
        HashMap<String,Object> keywordMap = new HashMap<>();
        keywordMap.put("type","text");
        keywordMap.put("analyzer","pinyin_analyzer");
        fieldMap.put("keyword",keywordMap);
        fieldMap.put("pinyin",pinyinMap);
        try {
            XContentBuilder mapping= XContentFactory.jsonBuilder().startObject();
            String mappingStr;
            mapping = mapping.startObject(typeName).startObject("_all").field("enabled",false).endObject().startObject("properties");
            Iterator iterator = paramMap.entrySet().iterator();
            while (iterator.hasNext()){
                Map.Entry entry = (Map.Entry)iterator.next();
                String key=(String)entry.getKey();
                if(key.substring(key.length()-4,key.length()).equals("Code")|| key.equals("Acct_Type")) {
                    mapping = mapping.startObject((String)entry.getKey()).field("type", "keyword").field("include_in_all", false).endObject();
                }else if(key.equals("KPI_Name_Length")||key.equals("Subject_Name_Length")||key.equals("Report_Name_Length")){
                    mapping = mapping.startObject((String) entry.getKey()).field("type", "integer").field("include_in_all", false).endObject();
                }else if(key.substring(key.length()-4,key.length()).equals("Name")){
                    mapping = mapping.startObject((String) entry.getKey()).field("type", "text").field("analyzer","ik_max_word").field("search_analyzer", "ik_max_word").field("fields", fieldMap).endObject();
//                    mapping = mapping.startObject((String) entry.getKey()).field("type", "text").field("analyzer","ik_max_word").field("search_analyzer", "ik_max_word").field("include_in_all", false).endObject();
                }else if (key.substring(key.length()-4,key.length()).equals("Desc")){
                    mapping = mapping.startObject((String) entry.getKey()).field("type", "text").field("analyzer","ik_max_word").field("search_analyzer", "ik_max_word").field("include_in_all", false).endObject();
                } else {
                    System.out.println("---------当前key是"+entry.getKey());
                    mapping = mapping.startObject((String) entry.getKey()).field("type", "text").field("include_in_all", false).endObject();
                }
            }
            mappingStr = mapping.endObject().endObject().endObject().string();
            request = Requests.putMappingRequest(indexName).type(typeName).source(mappingStr);
        } catch (IOException e) {
            e.printStackTrace();
        }
        TransportClient transportClient = null;
        try {
            transportClient = GenerateClient.getClient();
            transportClient.admin().indices().putMapping(request).actionGet();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }finally {
            transportClient.close();
        }
    }

    /**
     * 创建索引
     * @param indexName
     */
    public static void createIndex(String indexName) {
        TransportClient transportClient = null;
        HashMap<String,Object> setMap = new HashMap<>();
        HashMap<String,Object> indexHm = new HashMap<>();
        HashMap<String,Object> analysisHm = new HashMap<>();
        HashMap<String,Object> analyzerHm = new HashMap<>();
        HashMap<String,Object> pinyin_analyzerHm = new HashMap<>();
        HashMap<String,Object> tokenizerHm = new HashMap<>();
        HashMap<String,Object> my_pinyinHm = new HashMap<>();

        my_pinyinHm.put("type","pinyin");
        my_pinyinHm.put("keep_first_letter","false");
        my_pinyinHm.put("keep_none_chinese_in_first_letter","false");
        my_pinyinHm.put("keep_none_chinese","false");
        my_pinyinHm.put("keep_full_pinyin","false");
        my_pinyinHm.put("none_chinese_pinyin_tokenize","false");

        my_pinyinHm.put("keep_joined_full_pinyin","true");
        my_pinyinHm.put("keep_none_chinese_in_joined_full_pinyin","true");
        my_pinyinHm.put("keep_original","true");
        my_pinyinHm.put("lowercase","true") ;


        pinyin_analyzerHm.put("tokenizer","my_pinyin");
        analyzerHm.put("pinyin_analyzer",pinyin_analyzerHm);

        tokenizerHm.put("my_pinyin",my_pinyinHm);

        analysisHm.put("analyzer",analyzerHm);
        analysisHm.put("tokenizer",tokenizerHm);

        indexHm.put("analysis",analysisHm);

        setMap.put("index",indexHm);

        try {
            transportClient = GenerateClient.getClient();
            transportClient.admin().indices().create(new CreateIndexRequest(indexName).updateAllTypes(true).settings(setMap)).actionGet();
        } catch (ResourceAlreadyExistsException | IllegalStateException | UnknownHostException e) {
            e.printStackTrace();
            logger.info("该索引已经存在或客户端已经关闭！");
        }finally {
            transportClient.close();
        }
    }

    /**
     * 创建索引-4ZC
     * @param indexName
     */
    public static void createZCIndex(String indexName) {
        TransportClient transportClient = null;
        try {
            transportClient = GenerateClient.getClient();
            transportClient.admin().indices().create(new CreateIndexRequest(indexName).updateAllTypes(true)).actionGet();
        } catch (ResourceAlreadyExistsException | IllegalStateException | UnknownHostException e) {
            e.printStackTrace();
            logger.info("该索引已经存在或客户端已经关闭！");
        }finally {
            transportClient.close();
        }
    }

    /**
     * 创建映射-4ZC
     * 创建mapping
     * @param indexName 索引名
     * @param typeName 类型名
     * @throws Exception
     */
    public static void createZCMapping(String indexName, String typeName, HashMap<String,String> paramMap){
        PutMappingRequest request = null;
        try {
            XContentBuilder mapping= XContentFactory.jsonBuilder().startObject();
            String mappingStr;
            mapping = mapping.startObject(typeName).startObject("_all").field("enabled",false).endObject().startObject("properties");
            Iterator iterator = paramMap.entrySet().iterator();
            while (iterator.hasNext()){
                Map.Entry entry = (Map.Entry)iterator.next();
                String key=(String)entry.getKey();
                if(key.equals("searchword")) {
                    System.out.println("--------searchword");
                    mapping = mapping.startObject((String)entry.getKey()).field("type", "text").field("analyzer","ik_smart").field("search_analyzer", "ik_smart").field("include_in_all", false).endObject();
                }else {
                    mapping = mapping.startObject((String)entry.getKey()).field("type", "keyword").field("include_in_all", false).endObject();
                }
            }
            mappingStr = mapping.endObject().endObject().endObject().string();
            request = Requests.putMappingRequest(indexName).type(typeName).source(mappingStr);
        } catch (IOException e) {
            e.printStackTrace();
        }
        TransportClient transportClient = null;
        try {
            transportClient = GenerateClient.getClient();
            transportClient.admin().indices().putMapping(request).actionGet();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }finally {
            transportClient.close();
        }
    }



    /**
     * 获取Map中第一个键值对的value
     * @param paramMap
     * @return
     */
    public static String getFirstValue(HashMap<String,String> paramMap) {
        String firstValue = null;
        Iterator iterator = paramMap.entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry entry = (Map.Entry)iterator.next();
            firstValue = (String) entry.getValue();
            if (firstValue !=null){
                break;
            }
        }
        return firstValue;
    }

}
