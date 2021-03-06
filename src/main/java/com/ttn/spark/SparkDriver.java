package com.ttn.spark;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

public class SparkDriver {

    public static void main(String[] args) {

//        args = new String[2];
//        args[0] = "src/main/resources/input.txt";
//        args[1] = "src/main/resources/output.txt";

        String input = args[0];
        String output = args[1];

        System.out.println("input is " + input);
        System.out.println("output is " + output);

        SparkSession sparkSession = SparkSession
                .builder()
                .master("local")
                .getOrCreate();

        Dataset<Row> df = sparkSession
                .read()
                .text(input).as(Encoders.STRING())
                .coalesce(1)
                .flatMap(
                    line -> Arrays.asList(line.split(" ")).iterator(),
                    Encoders.STRING()
                )
                .groupBy("value")
                .count();

        df.selectExpr("concat(value, ' ', count)").as("result")
                .write()
                .text(output);

    }

}
