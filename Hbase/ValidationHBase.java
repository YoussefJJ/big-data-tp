
package tn.insat.validation;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.util.*;
import java.io.IOException;

public class ValidationHBase {

    private Table table1;
    private String tableName = "cab_ride";
    private String family1 = "cab_name";

    public void createHbaseTable() throws IOException {
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
        Admin admin = connection.getAdmin();

        HTableDescriptor ht = new HTableDescriptor(TableName.valueOf(tableName));
        ht.addFamily(new HColumnDescriptor(family1));
        System.out.println("connecting");

        System.out.println("Creating Table");
        createOrOverwrite(admin, ht);
        System.out.println("Done......");

        table1 = connection.getTable(TableName.valueOf(tableName));
	try {
	File myObj = new File("tn/insat/validation/outputKafka.txt");
	Scanner myReader = new Scanner(myObj);	
	int i = 0;
	while (myReader.hasNextLine()) {
	    String data = myReader.nextLine();
	    String[] keyValue = data.split(",");
	    System.out.println(keyValue[0] + " , " + keyValue[1]);
            byte[] row1 = Bytes.toBytes("cab_ride" + i);
            Put p = new Put(row1);

            p.addColumn(family1.getBytes(), "name".getBytes(), Bytes.toBytes(keyValue[0]));
            p.addColumn(family1.getBytes(), "distance".getBytes(), Bytes.toBytes(keyValue[1]));
            table1.put(p);
		i++;

        }
	myReader.close();
	} 
	catch (Exception e) {
            e.printStackTrace();
        }
	table1.close();
            connection.close();
    }

    public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }

    public static void main(String[] args) throws IOException {
        ValidationHBase admin = new ValidationHBase();
        admin.createHbaseTable();
    }
}
