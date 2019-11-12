import com.imooc.bigdata.hbase.api.HBaseUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class HBaseUtilTest {

    @Test
    public void createTable(){
        HBaseUtil.createTable("FileTable",new String[]{"fileInfo","saveInfo"});
    }

    @Test
    public void addFileDetails(){
        HBaseUtil.putRow("FileTable","rowkey1","fileInfo","name","file1.txt");
        HBaseUtil.putRow("FileTable","rowkey1","fileInfo","type","txt");
        HBaseUtil.putRow("FileTable","rowkey1","fileInfo","size","1024");
        HBaseUtil.putRow("FileTable","rowkey1","saveInfo","creator","tom");
        HBaseUtil.putRow("FileTable","rowkey2","fileInfo","name","file2.jpg");
        HBaseUtil.putRow("FileTable","rowkey2","fileInfo","type","jpg");
        HBaseUtil.putRow("FileTable","rowkey2","fileInfo","size","2048");
        HBaseUtil.putRow("FileTable","rowkey2","saveInfo","creator","jerry");
    }

    @Test
    public void getFileDetails(){
        Long t = System.currentTimeMillis();
        Result result = HBaseUtil.getRow("FileTable","rowkey1");
        System.out.println(System.currentTimeMillis() - t);
        if(null!=result){

            System.out.println("rowkey=" + Bytes.toString(result.getRow()));
            System.out.println("fileName=" + Bytes.toString(result.getValue(
                    Bytes.toBytes("fileInfo"),Bytes.toBytes("name")
            )));

        }
    }

    @Test
    public void scanFileDetail(){
        ResultScanner rs = HBaseUtil.getScanner("FileTable","rowkey2","rowkey2");
        if(null != rs){
            rs.forEach(r ->{
                System.out.println("rowkey=" + Bytes.toString(r.getRow()));
                System.out.println("fileName=" + Bytes.toString(r.getValue(Bytes.toBytes("fileInfo"), Bytes.toBytes("name"))));
            });
            rs.close();
        }
    }
    @Test
    public void deleteRow(){
        HBaseUtil.deleteRow("FileTable","rowkey1");
    }

    @Test
    public void deleteTable(){
        HBaseUtil.deleteTable("FileTable");
    }
}
