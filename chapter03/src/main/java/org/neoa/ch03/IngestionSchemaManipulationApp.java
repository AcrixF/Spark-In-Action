package org.neoa.ch03;

import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

public class IngestionSchemaManipulationApp {


    public static void main(String[] args) {
        IngestionSchemaManipulationApp app = new IngestionSchemaManipulationApp();
        app.start();
    }


    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Restaurants in Wake County, NC")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .load("/Users/acrixf/Documents/repositories/Spark-In-Action/spark-in-action/chapter03/data/Restaurants_in_Wake_County.csv");

        System.out.println("We have " + df.count() + " records.");

        System.out.println("Initial schema...");

        df.printSchema();

        df = df.withColumn("county", lit("Wake"))
                .withColumnRenamed("HSISID", "datasetId")
                .withColumnRenamed("NAME", "name")
                .withColumnRenamed("ADDRESS1", "address1")
                .withColumnRenamed("ADDRESS2", "address2")
                .withColumnRenamed("CITY", "city")
                .withColumnRenamed("STATE", "state")
                .withColumnRenamed("POSTALCODE", "zip")
                .withColumnRenamed("PHONENUMBER", "tel")
                .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
                .withColumnRenamed("FACILITYTYPE", "type")
                .withColumnRenamed("X", "geoX")
                .withColumnRenamed("Y", "geoY")
                .drop("OBJECTID")
                .drop("PERMITID")
                .drop("GEOCODESTATUS");

        df = df.withColumn("id",
                concat(df.col("state"),
                        lit("_"),
                        df.col("county"),
                        lit("_"),
                        df.col("datasetId")));

        System.out.println("*** Dataframe transformed...");
        df.show(5);
        df.printSchema();

        System.out.println("*** looking at partitions");
        Partition[] partitions = df.rdd().partitions();

        int partitionCount = partitions.length;
        System.out.println("Partition count before repartition: " + partitionCount);

        df = df.repartition(4);

        System.out.println("Partition count after repartition: " + df.rdd().partitions().length);

        StructType schema = df.schema();

        System.out.println("\n\n *** Schema as a tree: \n");
        schema.printTreeString();

        String schemaAsString = schema.mkString();
        System.out.println("\n\n *** Schema as String: \n" + schemaAsString);

        String schemaJson = schema.prettyJson();
        System.out.println("\n\n *** Schema as JSON: \n" + schemaJson);
    }
}
