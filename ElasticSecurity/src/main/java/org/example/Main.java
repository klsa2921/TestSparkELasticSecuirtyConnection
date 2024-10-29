package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) {
        // Define the path to the CSV file and Elasticsearch configuration
        String csvFilePath = "src/main/resources/sample.csv";
        String elasticsearchIndex = "test_security_index"; // Change as needed
        String elasticsearchHost = "https://es01:9301"; // Adjust to your setup

        // Create Spark session
        SparkSession spark = SparkSession.builder()
                .appName("CSV to Elasticsearch")
                .config("spark.master", "local[*]") // Set master based on your cluster setup
                .getOrCreate();

        // Read the CSV file into a DataFrame
        Dataset<Row> df = spark.read()
                .option("header", "true") // Use first line as header
                .option("inferSchema", "true") // Automatically infer data types
                .csv(csvFilePath);

        // Configure Elasticsearch connection settings
        Map<String, String> esConfig = new HashMap<>();
        esConfig.put("es.nodes", "https://localhost:9301,https://localhost:9302,https://localhost:9303");
//        esConfig.put("es.port", "9301");
        esConfig.put("es.net.ssl", "true");
        esConfig.put("es.net.ssl.keystore", "/home/klsa/IdeaProjects/ElasticSecurity/src/main/resources/certs/es01/es01_keystore.p12"); // Path to your PKCS#12 file
        esConfig.put("es.net.ssl.keystore.password", "elastic"); // Password you used during the conversion
        esConfig.put("es.net.http.auth.user", "elastic"); // Username (default admin)
        esConfig.put("es.net.http.auth.pass", "elastic"); // Elasticsearch password
        esConfig.put("es.nodes.wan.only", "true"); // Set to true if using WAN

        // Write the DataFrame to Elasticsearch
        JavaEsSparkSQL.saveToEs(df, elasticsearchIndex, esConfig);

        // Stop the Spark session
        spark.stop();
    }
        //openssl pkcs12 -export -in /home/klsa/IdeaProjects/ElasticSecurity/src/main/resources/certs/es01/es01.crt -inkey /home/klsa/IdeaProjects/ElasticSecurity/src/main/resources/certs/es01/es01.key -out es01_keystore.p12 -name es01 -password pass:elastic
    }
