import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class ListTables {

    public static void main(String args[]) throws MasterNotRunningException, IOException {

        Configuration config = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(config);

        HTableDescriptor[] tableDescriptor = admin.listTables();

        for(int i=0; i < tableDescriptor.length;i++){
            System.out.println(tableDescriptor[i].getNameAsString());
        }
    }
}
