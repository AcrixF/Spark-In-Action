package org.neoa.ch02;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

public class CsvToRelationalDatabaseApp {


    public static void main(String[] args) {
        CsvToRelationalDatabaseApp app = new CsvToRelationalDatabaseApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("CSV to DB")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true")
                .load("/Users/acrixf/Documents/repositories/Spark-In-Action/spark-in-action/chapter02/data/authors.csv");

        df = df.withColumn("name",
                concat(df.col("lname"),
                        lit(", "),
                        df.col("fname")));


        String dbConnectionUrl = "jdbc:mysql://localhost:3306/spark_labs";

        Properties properties = new Properties();
        properties.setProperty("user", "root");
        properties.setProperty("password", "Neoa2912");

        df.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dbConnectionUrl, "ch02", properties);

        System.out.println("Process Complete");

    }
}
