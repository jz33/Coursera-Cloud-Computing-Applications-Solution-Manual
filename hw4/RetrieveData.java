import java.io.IOException;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class RetrieveData{

    public static void main(String[] args) throws IOException, Exception{

        Configuration config = HBaseConfiguration.create();
        HTable table = new HTable(config, "emp");

        for(Integer i = 1;i <= 2;i++)
        {
            Get g = new Get(Bytes.toBytes("row" + i.toString()));
            Result result = table.get(g);
            byte [] names = result.getValue(Bytes.toBytes("personal"),Bytes.toBytes("name"));
            byte [] cities = result.getValue(Bytes.toBytes("personal"),Bytes.toBytes("city"));
            System.out.println("name: " + Bytes.toString(names) + " city: " + Bytes.toString(cities));
        }
    }
}
