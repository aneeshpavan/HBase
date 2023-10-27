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
		long timestamp1 = System.currentTimeMillis();
		long timestamp2 = timestamp1 - 1000;
		Configuration conf = HBaseConfiguration.create();        
		@SuppressWarnings({ "resource" })
		HTable hbaseTable = new HTable(conf, tableName);
		
		int rowKey = 10;
		//initialize a put with row key as tweet_url
		Put data1 = new Put(Bytes.toBytes(rowKey));
		data1.addColumn(Bytes.toBytes("Users"), Bytes.toBytes("user_name"), timestamp1, Bytes.toBytes("Name1"));
		hbaseTable.put(data1);

		Put data2 = new Put(Bytes.toBytes(rowKey));
		data2.addColumn(Bytes.toBytes("Users"), Bytes.toBytes("user_name"), timestamp2, Bytes.toBytes("Name2"));
		hbaseTable.put(data2);

	 
		//initialize a ge with row key as tweet_url
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addColumn(Bytes.toBytes("Users"), Bytes.toBytes("user_name"));
		get.setMaxVersions(3);
		
		//insert additional data
		Result result = hbaseTable.get(get);
		
		List<KeyValue> allResults = result.getColumn(Bytes.toBytes("Users"), Bytes.toBytes("user_name"));
		int i = 0;
		for(KeyValue kv: allResults) {
			
			System.out.println(new String(kv.getValue())+ " : "+i++);

		}
	}
}
