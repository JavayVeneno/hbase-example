package com.imooc.bigdata.hbase.api;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;
import java.util.List;

public class HBaseUtil {
    /**
     *
     * @Description: 创建HBase表
     *
     * @author: Amei
     * @date: 2019/9/24 15:00
     * @param: [tableName 表明, cfs 列族的数组]
     * @return: boolean 是否创建成功
     */
    public static boolean createTable(String tableName,String[] cfs){
        try(HBaseAdmin admin = (HBaseAdmin) HBaseConn.getHBaseConn().getAdmin()){
                if (admin.tableExists((tableName))){
                return false;
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            Arrays.stream(cfs).forEach(cf->{
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf);
                columnDescriptor.setMaxVersions(1);
                tableDescriptor.addFamily(columnDescriptor);
            });
            admin.createTable(tableDescriptor);
        }catch (Exception e){
            e.printStackTrace();
        }

        return true;
    }

    public static boolean deleteTable(String tableName){
        try(HBaseAdmin admin = (HBaseAdmin)HBaseConn.getHBaseConn().getAdmin()){
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }catch ( Exception e){
            e.printStackTrace();
        }
        return true;
    }
    /**
     *
     * @Description: HBase中插入一条数据
     *
     * @author: Amei
     * @date: 2019/9/24 15:24
     * @param: [tableName表名, rowkey主键, cfName列族名, qualifier列标识, data数据]
     * @return: boolean
     */
    public static boolean putRow(String tableName,String rowkey,String cfName,String qualifier,String data){

        try(Table table = HBaseConn.getTable(tableName)){
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(cfName),Bytes.toBytes(qualifier),Bytes.toBytes(data));
        table.put(put);
        }catch (Exception e){
            e.printStackTrace();
        }
        return true;
    }

    public static boolean putRows(String tableName, List<Put> puts){
        try(Table table = HBaseConn.getTable(tableName)){
            table.put(puts);
        }catch (Exception e){
            e.printStackTrace();
        }
        return true;
    }
    
    /**
     *
     * @Description: 获取单条数据
     *
     * @author: Amei
     * @date: 2019/9/24 15:32
     * @param: [tableName表名, rowkey主键]
     * @return: org.apache.hadoop.hbase.client.Result 返回结果
     */
    public static Result getRow(String tableName, String rowkey){
        try(Table table = HBaseConn.getTable(tableName)){
        Get get = new Get(Bytes.toBytes(rowkey));
        return table.get(get);
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    public static Result getRow(String tableName, String  rowkey, FilterList filterList){
        try(Table table = HBaseConn.getTable(tableName)){
            Get get = new Get(Bytes.toBytes(rowkey));
            get.setFilter(filterList);
            return table.get(get);
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    public static ResultScanner getScanner(String tableName){
        try(Table table = HBaseConn.getTable(tableName)){
            Scan scan = new Scan();
            scan.setCaching(1000);
            return table.getScanner(scan);
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
    public static ResultScanner getScanner(String tableName,String startRowkey,String endRowkey){
        try(Table table = HBaseConn.getTable(tableName)){
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(startRowkey));
            scan.setStopRow(Bytes.toBytes(endRowkey));
            scan.setCaching(1000);
            return table.getScanner(scan);
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    /**
     *
     * @Description:
     *
     * @author: Amei
     * @date: 2019/9/24 16:16
     * @param: [tableName, startRowkey 起始rowkey, endRowkey 结束rowkey]
     * @return: org.apache.hadoop.hbase.client.ResultScanner
     */
    public static ResultScanner getScanner(String tableName,String startRowkey,String endRowkey,FilterList filterList){
        try(Table table = HBaseConn.getTable(tableName)){
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(startRowkey));
            scan.setStopRow(Bytes.toBytes(endRowkey));
            scan.setFilter(filterList);
            scan.setCaching(1000);
            return table.getScanner(scan);
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    public static boolean deleteRow(String tableName,String rowkey){
        try(Table table = HBaseConn.getTable(tableName)) {
            Delete delete = new Delete(Bytes.toBytes(rowkey));
            table.delete(delete);
        }catch (Exception e){
            e.printStackTrace();
        }
            return true;
    }

    public static boolean deleteColumnFamily(String tableName,String cfName){
        try(HBaseAdmin admin = (HBaseAdmin) HBaseConn.getHBaseConn().getAdmin()){
            admin.deleteColumn(tableName,cfName);
        }catch (Exception e){
            e.printStackTrace();
        }
        return true;
    }

    public static boolean deleteQualifier(String tableName,String rowkey,String cfName,String qualifier){
        try(Table table = HBaseConn.getTable(tableName)) {
            Delete delete = new Delete(Bytes.toBytes(rowkey));
            delete.addColumn(Bytes.toBytes(cfName),Bytes.toBytes(qualifier));
            table.delete(delete);
        }catch (Exception e){
            e.printStackTrace();
        }
        return true;
    }
}
