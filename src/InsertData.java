import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class InsertData {

    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        int lineCount = 0;

        String[] headers = {"user_name", "user_location", "user_description", "user_created", "user_followers",
                "user_friends", "user_favourites", "user_verified", "date", "text", "hashtags", "source", "is_retweet"};

        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            TableName tableName = TableName.valueOf("tweets");

            if (!admin.tableExists(tableName)) {
                HTableDescriptor descriptor = new HTableDescriptor(tableName);

                descriptor.addFamily(new HColumnDescriptor("Users"));
                descriptor.addFamily(new HColumnDescriptor("Tweets"));
                descriptor.addFamily(new HColumnDescriptor("Extra"));
                
                admin.createTable(descriptor);
            }

            try (Table table = connection.getTable(tableName);
                 BufferedReader br = new BufferedReader(new FileReader("/home/ap43n/eclipse-workspace/HBase/covid19_tweets.csv"))) {

                String line;
                boolean isFirstLine = true;
                while ((line = br.readLine()) != null) {
                    if (isFirstLine) {
                        isFirstLine = false;
                        continue;
                    }

                    String[] columns = line.split("\t", -1);
                    Put p = new Put(Bytes.toBytes(lineCount + "_" + (columns.length > 0 ? columns[0] : "unknown")));

                    for (int i = 0; i < columns.length && i < headers.length; i++) {
                        String family;
                        if (i < 8) {
                            family = "Users";
                        } else if (i >= 8 && i < 11) {
                            family = "Tweets";
                        } else {
                            family = "Extra";
                        }

                        p.addColumn(Bytes.toBytes(family), Bytes.toBytes(headers[i]), Bytes.toBytes(columns[i]));
                    }

                    table.put(p);
                    lineCount++;
                    if (lineCount % 1000 == 0) {  // Print every 1000 lines to help diagnose the issue
                        System.out.println("Processed " + lineCount + " lines.");
                    }
                }
            }

            System.out.println("Total number of lines inserted: " + lineCount);
        }
    }
}
