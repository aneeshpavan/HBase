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

public class Q7 {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);

       TableName tableName = TableName.valueOf("Covid19tweets");
       Table table = connection.getTable(tableName);

        int tweetcount =0;
        byte[] Tweets = Bytes.toBytes("Tweets");
        byte[] text = Bytes.toBytes("text");
        Scan scantweet = new Scan();
        ResultScanner scannertweet = table.getScanner(scantweet);
        for (Result result : scannertweet) {   
            NavigableMap<byte[], byte[]> tweetMap = result.getFamilyMap(Tweets);
            String tweetText = Bytes.toString(tweetMap.get(text));
           String tt =  (String) tweetText.subSequence(0, 8);
            if((tt.toLowerCase()).equals("#covid19")) {
            	System.out.println(tweetText);
            	tweetcount++;
            }
        }
        
        System.out.println("Tweets containing #covid19" + tweetcount);

        Scan scan2 = new Scan();
        scan2.addColumn(Bytes.toBytes("Users"), Bytes.toBytes("user_followers"));
        scan2.addColumn(Bytes.toBytes("Users"), Bytes.toBytes("user_verified"));
        scan2.addColumn(Bytes.toBytes("Tweets"), Bytes.toBytes("text"));
        
        // Define a filter for users with more than 6 digits in user_followers count
        Filter filter = new SingleColumnValueFilter(
            Bytes.toBytes("Users"),
            Bytes.toBytes("user_followers"),
            CompareFilter.CompareOp.GREATER_OR_EQUAL,
            new BinaryComparator(Bytes.toBytes(100000)) // 6 digits = 100,000
        );
        scan2.setFilter(filter);

        ResultScanner scanner1 = table.getScanner(scan2);
        
        int vc =0;
        int nvc = 0;
        int tweetcountverified = 0;
        for (Result result : scanner1) {
            Cell user_followers = result.getColumnLatestCell(Bytes.toBytes("Users"), Bytes.toBytes("user_followers"));
            Cell user_verified = result.getColumnLatestCell(Bytes.toBytes("Users"), Bytes.toBytes("user_verified"));
            Cell texttweet = result.getColumnLatestCell(Bytes.toBytes("Tweets"), Bytes.toBytes("text"));
            
            int userfollow = Bytes.toInt(CellUtil.cloneValue(user_followers));
            String verification = Bytes.toString(CellUtil.cloneValue(user_verified));
                String tweetText = Bytes.toString(CellUtil.cloneValue(texttweet));
                String tt =  (String) tweetText.subSequence(0, 8);
                    if((tt.toLowerCase()).equals("#covid19") && verification.equalsIgnoreCase("TRUE")) {
                    	System.out.println(tweetText);
                    	tweetcountverified++;
                    }
            
        }
        
        System.out.println("Tweets starting with  #covid19 " + tweetcount);
        System.out.println("Tweets that start with #covid19 verified " + tweetcountverified);


        }
}