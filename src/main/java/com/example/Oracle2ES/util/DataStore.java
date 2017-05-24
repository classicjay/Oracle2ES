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
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.example.Oracle2ES.Oracle2EsApplication.client;

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
     * Bulk批量导入
     * @param indexName
     * @param typeName
     * @param dataList
     */
    public static void bulkDataStorage(String indexName,String typeName,List<HashMap<String,String>> dataList){
        BulkProcessor bulkProcessor = BulkProcessor.builder(client,
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
                    data = data.field((String) entry.getKey(),(String) entry.getValue());
                }
                String dataStr = data.endObject().string();
                IndexRequest indexrequest = new IndexRequest(indexName, typeName,getFirstValue(map)).source(dataStr);
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
        client.close();
    }

    /**
     * 创建映射
     * 创建mapping(feid("indexAnalyzer","ik")该字段分词IK索引 ；feid("searchAnalyzer","ik")该字段分词ik查询；具体分词插件请看IK分词插件说明)
     * @param indexName 索引名
     * @param typeName 类型名
     * @throws Exception
     */
    public static void createMapping(String indexName, String typeName, HashMap<String,String> paramMap)throws Exception{
        XContentBuilder mapping= XContentFactory.jsonBuilder().startObject();
        String mappingStr;
        mapping = mapping.startObject(typeName).startObject("properties");
        Iterator iterator = paramMap.entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry entry = (Map.Entry)iterator.next();
            mapping = mapping.startObject((String)entry.getKey()).field("type", "text").endObject();
        }
        mappingStr = mapping.endObject().endObject().endObject().string();
        PutMappingRequest request = Requests.putMappingRequest(indexName).type(typeName).source(mappingStr);
        client.admin().indices().putMapping(request).actionGet();
    }

    /**
     * 创建索引
     * @param indexName
     */
    public static void createIndex(String indexName) {
        try {
            client.admin().indices().create(new CreateIndexRequest(indexName)).actionGet();
        } catch (ResourceAlreadyExistsException e) {
            logger.info("该索引已经存在！");
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
