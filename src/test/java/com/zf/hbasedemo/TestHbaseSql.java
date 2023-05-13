package com.zf.hbasedemo;

import com.alibaba.fastjson2.JSON;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import java.util.Map;


/**
 * @author zhang fan
 * @date 2023/5/13 18:55
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
@WebAppConfiguration
public class TestHbaseSql {
    @Autowired
    private HBaseService hBaseService;
    private Logger log = LoggerFactory.getLogger(TestHbaseSql.class);
    //测试创建表，插入数据，查询数据
    @Test
    public void test(){
        String tableName = "test_person";
        String cf = "cf";
        //1.创建表
        hBaseService.createTable(tableName,cf);
        //2,插入数据
        hBaseService.putData(tableName,"01",cf,new String[]{"id","name"},new String[]{"01","张三"});
        hBaseService.putData(tableName,"02",cf,new String[]{"id","name"},new String[]{"02","李四"});
        hBaseService.putData(tableName,"03",cf,new String[]{"id","name"},new String[]{"03","王五"});
        //3.根据 rowKey查询
        Map<String, String> result = hBaseService.get(tableName, cf, "01");
        log.info(JSON.toJSONString(result));
        //4.根据rowkey范围查询
        Map<String, Map<String, String>> resultScanner = hBaseService.getResultScanner(tableName, "01", "03");
        resultScanner.forEach((k,value)->{
            log.info(k+":"+value);
        });
    }

}
