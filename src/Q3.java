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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

public class Q3 {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);

       TableName hbaseTableName = TableName.valueOf("Covid19tweets");
       Table hbaseTable = connection.getTable(hbaseTableName);

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("Users"), Bytes.toBytes("user_location"));
    
        Map<String, Integer> locationCountMap = new HashMap<>();

        // Iterate through the rows in the table
        ResultScanner scanner = hbaseTable.getScanner(scan);
        for (Result result : scanner) {
            // Get the user_location column
            byte[] locationBytes = result.getValue(Bytes.toBytes("Users"), Bytes.toBytes("user_location"));
            
            if (locationBytes != null) {
                String location = Bytes.toString(locationBytes);
                locationCountMap.put(location, locationCountMap.getOrDefault(location, 0) + 1);
            }
        }

        // Close the table and connection
        hbaseTable.close();
        connection.close();

        // Print the count of tweets from each unique location
        for (Map.Entry<String, Integer> entry : locationCountMap.entrySet()) {
            System.out.println("User's Location: " + entry.getKey() + ", Number of tweets: " + entry.getValue());

        }
        
    }

}
