package com.example.Oracle2ES.mapper;

import org.apache.ibatis.annotations.Mapper;

import java.util.HashMap;
import java.util.List;

/**
 * <p>Title: BONC -  Oracle2ESMapper</p>
 * <p>Description:  </p>
 * <p>Copyright: Copyright BONC(c) 2013 - 2025 </p>
 * <p>Company: 北京东方国信科技股份有限公司 </p>
 *
 * @author zhaojie
 * @version 1.0.0
 */
@Mapper
public interface Oracle2ESMapper {
    public List<HashMap<String,String>> testFetch();
}
