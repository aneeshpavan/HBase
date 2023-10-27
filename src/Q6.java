import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

public class Q6 {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);

       TableName hbaseTableName = TableName.valueOf("Covid19tweets");
       Table hbaseTable = connection.getTable(hbaseTableName);

        Scan scan2 = new Scan();
        scan2.addColumn(Bytes.toBytes("Users"), Bytes.toBytes("user_name"));
        scan2.addColumn(Bytes.toBytes("Users"), Bytes.toBytes("user_followers"));
        scan2.addColumn(Bytes.toBytes("Users"), Bytes.toBytes("user_verified"));
        
        // Define a filter for users with more than 6 digits in user_followers count
        Filter filter = new SingleColumnValueFilter(
            Bytes.toBytes("Users"),
            Bytes.toBytes("user_followers"),
            CompareFilter.CompareOp.GREATER_OR_EQUAL,
            new BinaryComparator(Bytes.toBytes(100000)) // 6 digits = 100,000
        );
        scan2.setFilter(filter);

        ResultScanner scanner2 = hbaseTable.getScanner(scan2);
        
        int verifiedCount =0;
        int notVerifiedCount = 0;
        for (Result result : scanner2) {
            Cell nameCell = result.getColumnLatestCell(Bytes.toBytes("Users"), Bytes.toBytes("user_name"));
            Cell user_followers = result.getColumnLatestCell(Bytes.toBytes("Users"), Bytes.toBytes("user_followers"));
            Cell user_verified = result.getColumnLatestCell(Bytes.toBytes("Users"), Bytes.toBytes("user_verified"));
            
            String userName = Bytes.toString(CellUtil.cloneValue(nameCell));
            int followersCount = Bytes.toInt(CellUtil.cloneValue(user_followers));
            String verifiedStatus = Bytes.toString(CellUtil.cloneValue(user_verified));
            if (nameCell != null) {
                
                System.out.println("Popular username: " + userName + ", and their followers count " + followersCount);

                
                if(verifiedStatus.equalsIgnoreCase("TRUE")) {
                	verifiedCount++;
                }
                else {
                	notVerifiedCount++;
                }
            }
        
        }
        
                System.out.println("Popular Verified user count: " + verifiedCount + " Popular Non verified user count: " + notVerifiedCount);

        }
}
