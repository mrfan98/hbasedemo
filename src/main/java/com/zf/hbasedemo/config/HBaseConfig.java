package com.zf.hbasedemo.config;

import com.zf.hbasedemo.HBaseService;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


/**
 * @author zhang fan
 * @date 2023/5/13 12:01
 */
@Configuration
public class HBaseConfig {
    @Value("${HBase.nodes}")
    private String nodes;
    @Value("${HBase.maxsize}")
    private String maxsize;
    @Bean
    public HBaseService getHbaseService(){
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum",nodes);
        conf.set("hbase.client.keyvalue.maxsize",maxsize);
        return new HBaseService(conf);
    }
}
