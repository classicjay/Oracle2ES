package com.example.Oracle2ES.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 * <p>Title: BONC -  SwaggerConfiguration</p>
 * <p>Description:  </p>
 * <p>Copyright: Copyright BONC(c) 2013 - 2025 </p>
 * <p>Company: 北京东方国信科技股份有限公司 </p>
 *
 * @author zhaojie
 * @version 1.0.0
 */
@Configuration
@EnableSwagger2
public class SwaggerConfiguration {
    @Bean
    public Docket buildDocket() {
        return new Docket(DocumentationType.SWAGGER_2).apiInfo(buildApiInf()).select()
                .apis(RequestHandlerSelectors.basePackage("com.example.Oracle2ES.controller"))// 要扫描的API(Controller)基础包
                .paths(PathSelectors.any()).build();
    }

    private ApiInfo buildApiInf() {
        return new ApiInfoBuilder().title("Oracle2ElasticSearch").contact("zj").version("1.0").build();
    }
}
