package com.example.Oracle2ES.controller;

import com.example.Oracle2ES.service.Oracle2ESService;
import io.swagger.annotations.Api;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * <p>Title: BONC -  Oracle2ESController</p>
 * <p>Description:  </p>
 * <p>Copyright: Copyright BONC(c) 2013 - 2025 </p>
 * <p>Company: 北京东方国信科技股份有限公司 </p>
 *
 * @author zhaojie
 * @version 1.0.0
 */
@RestController
@CrossOrigin(origins = "*")
@Api(value="ElasticSearch",description="Oracle-->ElasticSearch")
@RequestMapping("/oracle2es")
public class Oracle2ESController {

    @Autowired
    Oracle2ESService oracle2ESService;

    @PostMapping("/zhouxy")
    public List<HashMap<String,String>> testFetch(){
        List<HashMap<String,String>> resultList = new ArrayList<>();
        resultList = oracle2ESService.testFetch();
        return resultList;
    }

    /**
     * 测试从库里取数后入到ES
     */
    @PostMapping("/dataImport")
    public void dataImport(){
        try {
            oracle2ESService.testImport();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 从16库里取数入到ES
     */
    @PostMapping("/prodImport")
    public void prodImport(){
       try {
           oracle2ESService.prodImport();
       }catch (Exception e){
           e.printStackTrace();
       }
    }

}
