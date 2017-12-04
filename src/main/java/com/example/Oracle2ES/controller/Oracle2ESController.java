package com.example.Oracle2ES.controller;

import com.example.Oracle2ES.service.Oracle2ESService;
import io.swagger.annotations.Api;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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

    /**
     * 从16库里取指标映射表入到ES
     */
    @PostMapping("/kpiMappingImport")
    public void kpiMappingImport(){
       try {
           oracle2ESService.kpiMappingImp();
       }catch (Exception e){
           e.printStackTrace();
       }
    }

    /**
     * 从16库里取专题码表入到ES
     */
    @PostMapping("/subjectCodeImport")
    public void subjectCodeImport(){
        try {
            oracle2ESService.subjectCodeImp();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 从16库里取报告码表入到ES
     */
    @PostMapping("/reportCodeImport")
    public void reportCodeImport(){
        try {
            oracle2ESService.reportCodeImp();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 从16库里取报表码表入到ES
     */
    @PostMapping("/reportTableCodeImport")
    public void reportTableCodeImport(){
        try {
            oracle2ESService.reportTableCodeImp();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 从16库里取省份+地市+三种维度码表入到ES
     */
    @PostMapping("/dimensionImport")
    public void dimensionImport(){
        try {
            oracle2ESService.dimensionImport();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @PostMapping("/clearThenImport")
    public void clearThenImport(){
        try {
            oracle2ESService.clearThenImport();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
