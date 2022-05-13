package tn.insat.validation;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.*;

public class ReadHBase {
	public static void main(String args[]) throws IOException{
		Configuration c = HBaseConfiguration.create();        // Instantiate Configuration class
		HTable table = new HTable(c, "cab_ride");        // Instantiate HTable class
		Scan scan = new Scan();      // Instantiate the Scan class
		scan.addColumn(Bytes.toBytes("cab_name"), Bytes.toBytes("name"));
		scan.addColumn(Bytes.toBytes("cab_name"), Bytes.toBytes("distance"));  // Scan the required columns
		ResultScanner scanner = table.getScanner(scan);      // Get scan result
		// Reading values from scan result
		for (Result result = scanner.next(); result != null; result = scanner.next()) {
			byte [] nameBytes = result.getValue(Bytes.toBytes("cab_name"),Bytes.toBytes("name"));
			byte [] distanceBytes = result.getValue(Bytes.toBytes("cab_name"),Bytes.toBytes("distance"));
			String name = Bytes.toString(nameBytes);
      			String distance = Bytes.toString(distanceBytes);

			System.out.println("name: " + name + " | distance: " + distance);
		}
		scanner.close();      //close the scanner
	}
}
