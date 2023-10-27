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

public class Q5 {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);

       TableName hbaseTableName = TableName.valueOf("Covid19tweets");
       Table hbaseTable = connection.getTable(hbaseTableName);
       
       byte[] family = Bytes.toBytes("Users");
       byte[] verified = Bytes.toBytes("user_verified");
       byte[] userName = Bytes.toBytes("user_name");
       
       Scan scan1 = new Scan();
       ResultScanner scanner = hbaseTable.getScanner(scan1);

       Set<String> usersWithSpecialCharacters = new HashSet<>();
       Set<String> usersWithSpecialCharactersVerified = new HashSet<>();
       
       for (Result result : scanner) {
           NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(family);
           if (familyMap.containsKey(userName)) {
        	   String userVerified = Bytes.toString(familyMap.get(verified));
               String userNameli = Bytes.toString(familyMap.get(userName));
               if (userNameli.matches(".*[^a-zA-Z0-9\\s].*")) {
                   usersWithSpecialCharacters.add(userNameli);
               }
               
               if (userNameli.matches(".*[^a-zA-Z0-9\\s].*")  && userVerified.equalsIgnoreCase("TRUE")) {
               	usersWithSpecialCharactersVerified.add(userNameli);
               }
           }
       
       	}
       System.out.println("Users with Special Characters are: ");
       for (String user : usersWithSpecialCharacters) {
           System.out.println("User with special character: "+user);
       }

       System.out.println("Total number of Users with Special Characters are: " + usersWithSpecialCharacters.size());

       System.out.println("Users with Special Characters and verified are: ");
       for (String user : usersWithSpecialCharactersVerified) {
           System.out.println("Verified user with special character are: "+user);
       }
       System.out.println("Total number of Users with Special Characters and verified are: " + usersWithSpecialCharactersVerified.size());
       
    	}
    }
