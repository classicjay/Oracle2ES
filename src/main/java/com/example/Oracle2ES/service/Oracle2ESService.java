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

    public List<HashMap<String,String>> testFetch(){
        List<HashMap<String,String>> resultList = new ArrayList<>();
        resultList = oracle2ESMapper.testFetch();
        return resultList;
    }
    public void testImport() throws Exception{
        List<HashMap<String,String>> dataList = new ArrayList<>();
        dataList = oracle2ESMapper.testFetch();
        String indexName = "testzj";
        String typeName = "kpicodemapping";
        DataStore.createIndex(indexName);
        DataStore.createMapping(indexName,typeName,dataList.get(0));
        DataStore.bulkDataStorage(indexName,typeName,dataList);
        logger.info("success");
    }
}
