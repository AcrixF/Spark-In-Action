package org.neoa.ch01;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CsvToDataframeApp {

    public static void main(String[] args) {
        CsvToDataframeApp csvToDataframeApp = new CsvToDataframeApp();
        csvToDataframeApp.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder().appName("CSV to Dataset")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .load("/Users/acrixf/Documents/repositories/Spark-In-Action/spark-in-action/chapter01/data/books.csv");

        df.show();
    }
}
