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

public class Q4 {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);

       TableName hbaseTableName = TableName.valueOf("Covid19tweets");
       Table hbaseTable = connection.getTable(hbaseTableName);
 
        
        byte[] family = Bytes.toBytes("Users");
        byte[] qualifier = Bytes.toBytes("user_created");
        byte[] verified = Bytes.toBytes("user_verified");
        
        Scan scan1 = new Scan();
        ResultScanner scanner = hbaseTable.getScanner(scan1);

        Map<String, Integer> userCountByYear = new HashMap<>();
        Map<String, Integer> userCountByYearVerified = new HashMap<>();
        
        Map<String, Integer> userCountOf2020 = new HashMap<>();
        Map<String, Integer> userCountOf2020Verified = new HashMap<>();

        
        for (Result result : scanner) {
            NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(family);
            if (familyMap.containsKey(qualifier)) {
                String userCreated = Bytes.toString(familyMap.get(qualifier));
                String userVerified = Bytes.toString(familyMap.get(verified));               

                String year = userCreated.split("-")[2].split(" ")[0]; 
                if(year.trim().equals("2020")) {
                	String month = userCreated.split("-")[1];                	
                	userCountOf2020.put(month, userCountOf2020.getOrDefault(month, 0) + 1);
                }
                
                userCountByYear.put(year, userCountByYear.getOrDefault(year, 0) + 1);
                
                if(userVerified.equalsIgnoreCase("TRUE")) {
                	userCountByYearVerified.put(year, userCountByYearVerified.getOrDefault(year, 0) + 1);
                }
                
                if(year.trim().equals("2020") && userVerified.equalsIgnoreCase("TRUE")) {
                	String month = userCreated.split("-")[1];
                	userCountOf2020Verified.put(month, userCountOf2020Verified.getOrDefault(month, 0) + 1);
                }
                
            }
        }
        
        for (Map.Entry<String, Integer> entry : userCountByYear.entrySet()) {
            System.out.println("Number of users created in " + entry.getKey() + " : " + entry.getValue());
        }
        System.out.println("-----------------------------------------------------------------------------------------------------------------");
        
        for (Map.Entry<String, Integer> entry : userCountOf2020.entrySet()) {
            System.out.println("Number of users created in year 2020 in Month " + entry.getKey() + " : "+ entry.getValue());
        }
        
        System.out.println("-----------------------------------------------------------------------------------------------------------------");
        
        for (Map.Entry<String, Integer> entry : userCountByYearVerified.entrySet()) {
            System.out.println("Number of verified users created in " + entry.getKey() + " : " + entry.getValue());
        }
        System.out.println("-----------------------------------------------------------------------------------------------------------------");

        for (Map.Entry<String, Integer> entry : userCountOf2020Verified.entrySet()) {
            System.out.println("Number of verified users created in year 2020 in Month " + entry.getKey() + " : " + entry.getValue());
        }
                
        }
}
