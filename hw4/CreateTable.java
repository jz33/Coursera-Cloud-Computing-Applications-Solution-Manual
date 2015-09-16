import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.conf.Configuration;

public class CreateTable {

    public static void main(String[] args) throws IOException {

        Configuration config = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(config);

        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("emp"));

        tableDescriptor.addFamily(new HColumnDescriptor("personal"));
        tableDescriptor.addFamily(new HColumnDescriptor("professional"));

        admin.createTable(tableDescriptor);
        System.out.println("Table created");
    }
}
