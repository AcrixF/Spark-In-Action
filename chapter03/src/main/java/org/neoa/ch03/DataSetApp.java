package org.neoa.ch03;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class DataSetApp {

    public static void main(String[] args) {
        DataSetApp app = new DataSetApp();
        app.start();
    }

    private void start() {

        SparkSession spark = SparkSession.builder()
                .appName("Array to Dataset<String>")
                .master("local[*]")
                .getOrCreate();

        String [] strings = new String [] {"Jean", "Liz", "Pierre", "Lauric"};

        List<String> data = Arrays.asList(strings);

        Dataset<String> ds = spark.createDataset(data, Encoders.STRING());

        ds.show();

        ds.printSchema();

        Dataset<Row> df = ds.toDF();

        df.show();
        
        df.printSchema();
    }
}
