import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileNotFoundException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


public class InsertData  extends Configured implements Tool{
  
public String Table_Name = "Covid19tweets";
@SuppressWarnings("deprecation")
@Override
public int run(String[] argv) throws IOException, CsvValidationException {
    Configuration conf = HBaseConfiguration.create();        
    
    try (Connection connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin()) {
    @SuppressWarnings("resource")    
   
    TableName tableName = TableName.valueOf(Table_Name);
    boolean isExists = admin.tableExists(tableName);
    
    if(isExists == false) {
        HTableDescriptor htb=new HTableDescriptor(Table_Name);
        HColumnDescriptor UsersFamily = new HColumnDescriptor("Users");
        HColumnDescriptor TweetsFamily = new HColumnDescriptor("Tweets");
        HColumnDescriptor ExtraFamily = new HColumnDescriptor("Extra");
        
        htb.addFamily(UsersFamily);
        htb.addFamily(TweetsFamily);
        htb.addFamily(ExtraFamily);
        admin.createTable(htb);
    }
    
    
    try(Table table = connection.getTable(tableName);
    		CSVReader csvReader = new CSVReaderBuilder(new FileReader("covid19_tweets.csv"))
            .withSkipLines(1)
            .withCSVParser(new CSVParserBuilder().build())
                .build()){

        String[] line;

	    int row_count=0;
        while ((line = csvReader.readNext()) != null) {
        	   
        	if(line.length == 0)continue;
        	    	
	    	try {
		    	String user_name = (line[0] != null) ? line[0] : "";
		    	String user_location =(line[1] != null) ? line[1] : ""; 
		    	String user_description = (line[2] != null) ? line[2] : "";
		    	String user_created = (line[3] != null) ? line[3] : "";

		    	int user_followers = (line[4] != null) ? (Integer.parseInt(line[4].trim())) : 0;
		    	int user_friends = (line[5] != null) ? Integer.parseInt(line[5].trim()) : 0;
		    	int user_favourites = (line[6] != null) ? Integer.parseInt(line[6].trim()) : 0;
		    	String user_verified = (line[7] != null) ? line[7] : "";
		    	
		    	String text = (line[9] != null) ? line[9] : "";
		    	String hashtags = (line[10] != null) ? line[10] : "";
		    	String is_retweet = (line[12] != null) ? line[12] : "";
		    	
		    	String source = (line[11] != null) ? line[11] : "";
		    	String date = (line[8] != null) ? line[8] : "";
		    
	            Put put = new Put(Bytes.toBytes(row_count));
	            
	            put.addColumn(Bytes.toBytes("Users"), Bytes.toBytes("user_name"), Bytes.toBytes(user_name));
	            put.addColumn(Bytes.toBytes("Users"), Bytes.toBytes("user_location"), Bytes.toBytes(user_location));
	            put.addColumn(Bytes.toBytes("Users"), Bytes.toBytes("user_description"), Bytes.toBytes(user_description));
	            put.addColumn(Bytes.toBytes("Users"), Bytes.toBytes("user_created"), Bytes.toBytes(user_created));
	            put.addColumn(Bytes.toBytes("Users"), Bytes.toBytes("user_followers"), Bytes.toBytes(user_followers));
	            put.addColumn(Bytes.toBytes("Users"), Bytes.toBytes("user_friends"), Bytes.toBytes(user_friends));
	            put.addColumn(Bytes.toBytes("Users"), Bytes.toBytes("user_favourites"), Bytes.toBytes(user_favourites));
	            put.addColumn(Bytes.toBytes("Users"), Bytes.toBytes("user_verified"), Bytes.toBytes(user_verified));
	            
	            put.addColumn(Bytes.toBytes("Tweets"), Bytes.toBytes("text"), Bytes.toBytes(text));
	            put.addColumn(Bytes.toBytes("Tweets"), Bytes.toBytes("hashtags"), Bytes.toBytes(hashtags));
	            put.addColumn(Bytes.toBytes("Tweets"), Bytes.toBytes("is_retweet"), Bytes.toBytes(is_retweet));
	            
	            put.addColumn(Bytes.toBytes("Extra"), Bytes.toBytes("source"), Bytes.toBytes(source));
	          put.addColumn(Bytes.toBytes("Extra"), Bytes.toBytes("date"), Bytes.toBytes(date));
	            

		    	table.put(put);
		    	row_count++;
		    	if(row_count % 10000 == 0) System.out.println("Inserted Rows: "+ row_count);
 	    	}catch (ArrayIndexOutOfBoundsException e) {
	    		System.out.println("On Row: Arrayexcept" + row_count + " First Element:" + line[0]);
	    	} catch (NumberFormatException e) { 
	    		System.out.println("On Row: " + row_count + " First Element:" + line[0]);
	    	} catch (FileNotFoundException e) {
	        	e.printStackTrace();
	        } catch (IOException e) {
	        	e.printStackTrace();
	        }
	    	table.close(); 
	      	}
    		System.out.println("Inserted Row: "+ row_count);
    		System.out.println("Inserted all rows");
        }
    } catch (IOException e) {
        e.printStackTrace();
    }

  return 0;
}

    
    public static void main(String[] argv) throws Exception {
        int ret = ToolRunner.run(new InsertData (), argv);
        System.exit(ret);
    }
}