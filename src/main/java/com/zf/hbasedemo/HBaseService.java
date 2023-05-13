package com.zf.hbasedemo;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zhang fan
 * @date 2023/5/13 12:19
 */
public class HBaseService {
    private Logger log = LoggerFactory.getLogger(HBaseService.class);
    private Configuration conf = null;
    private Connection connection = null;

    public HBaseService(Configuration conf) {
        this.conf = conf;
        try{
            connection = ConnectionFactory.createConnection(conf);
        }catch (IOException e){
            log.error("获取HBase连接失败");
        }
    }
    private void close(Admin admin, ResultScanner rs, Table table){
        if(admin!=null){
            try{
                admin.close();
            }catch (IOException e){
                log.error("关闭Admin失败",e);
            }
        }
        if(rs!=null){
            rs.close();
        }
        if(table!=null){
            try{
                table.close();
            }catch (IOException e){
                log.error("关闭Table失败",e);
            }
        }
    }
    //创建表
    public boolean createTable(String tablename,String cf){
        Admin admin = null;
        try{
            //获取表管理类
            admin = connection.getAdmin();
            //定义表
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tablename));
            //定义列簇
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            //将列簇添加到表中
            hTableDescriptor.addFamily(hColumnDescriptor);
            //执行建表操作
            admin.createTable(hTableDescriptor);
            return true;
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }finally {
            close(admin,null,null);
        }
    }
    //添加数据
    public void putData(String tableName,String rowKey,String cf,String[] columns,String[] values){
        Table table = null;
        try{
            //获取表对象
            table = connection.getTable(TableName.valueOf(tableName));
            //创建 put对象
            Put put = new Put(rowKey.getBytes());
            //添加列
            for(int i = 0;i<columns.length;i++){
                put.addColumn(cf.getBytes(),columns[i].getBytes(),values[i].getBytes());
                //向表格添加put对象
                table.put(put);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            close(null,null,table);
        }
    }
    //查询数据
    public Map<String,String> get(String tableName,String cf,String rowKey){
        //将返回的键值对存储在名为result的Map结构中
        HashMap<String, String> result = new HashMap<>();
        Table table = null;
        try{
            table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            Result hTableResult = table.get(get);
            if(hTableResult!=null && !hTableResult.isEmpty()){
                for (Cell cell : hTableResult.listCells()) {
                    result.put(Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength()),Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength()));
                }
            }
        }catch (IOException e){
            log.error(MessageFormat.format("查询一行的数据失败，tableName:{0},rowKey:{1}",tableName,rowKey),e);
        }finally {
            close(null,null,table);
        }
        return result;
    }
    //根据startRowKey和stopRowKey遍历查询指定表中的所有数据
    public Map<String,Map<String,String>> getResultScanner(String tableName,String startRowKey,String stopRowKey){
        Scan scan = new Scan();//通过Scan实现范围查询
        if(StringUtils.isNoneBlank(startRowKey)&& StringUtils.isNoneBlank(stopRowKey)){
            scan.withStartRow(Bytes.toBytes(startRowKey));
            scan.withStopRow(Bytes.toBytes(stopRowKey));
        }
        return this.queryData(tableName,scan);
    }
    //通过表名及过滤条件查询数据
    private Map<String,Map<String,String>> queryData(String tableName,Scan scan){
        //将返回的键值对存储在名为result的Map<RowKey,RowKey>对应的行数据结构中
        HashMap<String, Map<String, String>> result = new HashMap<>();
        ResultScanner rs = null;
        //获取表
        Table table = null;
        try{
            table = connection.getTable(TableName.valueOf(tableName));
            rs = table.getScanner(scan);
            for (Result r : rs) {
                //获取的每一行数据
                Map<String,String> columnMap = new HashMap<>();
                String rowKey = null;
                for (Cell cell : r.listCells()) {
                    if(rowKey==null){
                        rowKey = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                    }
                    columnMap.put(Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength()),Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength()));
                }
                if(rowKey!=null){
                    result.put(rowKey,columnMap);
                }
            }
        }catch (IOException e){
            log.error(MessageFormat.format("范围查询失败，tableName:[0]",tableName),e);
        }finally {
            close(null,rs,table);
        }
        return result;
    }
}
