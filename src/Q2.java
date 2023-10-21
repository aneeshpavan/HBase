import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;


public class Q2 {

public static String tableName = "Covid19tweets";
	
	@SuppressWarnings("deprecation")
	public static void main(String[] argv) throws Exception {
		Configuration conf = HBaseConfiguration.create();        
		@SuppressWarnings({ "resource" })
		HTable hbaseTable = new HTable(conf, tableName);
		
		int rowKey = 5;
		//initialize a put with row key as tweet_url
		Put data = new Put(Bytes.toBytes(rowKey));
		
		put.add(Bytes.toBytes("Users"), Bytes.toBytes("user_name"), Bytes.toBytes("type1"));
		put.add(Bytes.toBytes("Users"), Bytes.toBytes("user_name"), Bytes.toBytes("type2"));
		hbaseTable.put(data);
	 
		//initialize a ge with row key as tweet_url
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addColumn(Bytes.toBytes("Users"), Bytes.toBytes("user_name"));
		get.setMaxVersions(3);
		
		//insert additional data
		Result result = hbaseTable.get(get);
		
		List<KeyValue> allResults = result.getColumn(Bytes.toBytes("Users"), Bytes.toBytes("user_name"));
		int i = 0;
		for(KeyValue kv: allResults) {
			
			System.out.println(new String(kv.getValue())+"----------------------------"+i++);
		}
	}
}