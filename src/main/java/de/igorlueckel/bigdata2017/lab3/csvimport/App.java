package de.igorlueckel.bigdata2017.lab3.csvimport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by igorl on 04.06.2017.
 */
public class App {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CsvMapper(), args);
        System.exit(res);
    }
}
