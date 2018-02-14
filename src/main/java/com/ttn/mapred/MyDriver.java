package com.ttn.mapred;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class MyDriver {

    public static void main(String[] args) throws IOException {

//        args = new String[2];
//        args[0] = "src/main/resources/input.txt";
//        args[1] = "src/main/resources/output.txt";

        String input = args[0];
        String output = args[1];

        System.out.println("input is " + input);
        System.out.println("output is " + output);

        JobConf conf = new JobConf(MyDriver.class);
        conf.setJobName("example-hadoop-job");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(MyMapper.class);
        conf.setReducerClass(MyReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.addInputPath(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        JobClient.runJob(conf);

    }

}
