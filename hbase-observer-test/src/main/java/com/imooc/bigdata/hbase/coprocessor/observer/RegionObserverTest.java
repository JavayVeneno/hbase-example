package com.imooc.bigdata.hbase.coprocessor.observer;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 *
 * @Description:
 *
 * @author: Amei
 * @date: 2019/10/8 11:15
 * @param:
 * @return:
 */
public class RegionObserverTest extends BaseRegionObserver {

    private byte[] columnFamily = Bytes.toBytes("cf");
    private byte[] countCol = Bytes.toBytes("countCol");
    private byte[] unDeleteCol = Bytes.toBytes("unDeleteCol");
    private RegionCoprocessorEnvironment environment;

    // region server 打开region前执行
    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        environment = (RegionCoprocessorEnvironment) e;
    }

    // region server 关闭region前调用
    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {

    }

    /**
     *
     * 1. cf:countCol 进行累加操作。 每次插入的是都东欧要与之前的值进行相加。
     *
     */
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)throws IOException{
        if(put.has(columnFamily,countCol)){
            // 获取old countCol value
            Result rs = e.getEnvironment().getRegion().get(new Get(put.getRow()));
            int oldNum = 0;
            for (Cell cell : rs.rawCells()) {
                if (CellUtil.matchingColumn(cell,columnFamily,countCol)){
                   oldNum = Integer.valueOf(Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            // 获取new countCol value
            List<Cell> cells = put.get(columnFamily,countCol);
            int newNum = 0;
            for (Cell cell : cells) {
                if (CellUtil.matchingColumn(cell,columnFamily,countCol)){
                    newNum = Integer.valueOf(Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            // sum AND update Put实例
            put.addColumn(columnFamily,countCol,Bytes.toBytes(String.valueOf(oldNum+newNum)));
        }
    }
    /**
     *
     * 2. 不能直接删除unDeleteCol 。 删除countCol的时候将unDeleteCol一同删除。
     *
     */

    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability)throws IOException{

        // 判断是偶操作cf列族
        List<Cell> cells = delete.getFamilyCellMap().get(columnFamily);
        if(null == cells || 0 == cells.size()){
            return;
        }
        boolean deleteFlag = false;
        for (Cell cell : cells) {
            byte[] qualifer = CellUtil.cloneQualifier(cell);
            if(Arrays.equals(qualifer,unDeleteCol)){
                throw new IOException("can not delete unDel column");
            }
            if(Arrays.equals(qualifer,countCol)){
                deleteFlag = true;
            }
            if(deleteFlag){
               delete.addColumn(columnFamily,unDeleteCol);
            }
        }
    }
}
