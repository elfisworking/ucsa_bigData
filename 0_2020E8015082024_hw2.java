/**
 * @file Hw2Part1.java
 * @author  2020E8015082024
 * @version 0.1
 * @time 2021/04/30
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Modified by Shimin Chen to demonstrate functionality for Homework 2
// April-May 2015

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Hw2Part1 {

    // This is the Mapper class
    // reference: http://hadoop.apache.org/docs/r2.6.0/api/org/apache/hadoop/mapreduce/Mapper.html
    //
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, DoubleWritable>{
        //        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
//            StringTokenizer itr = new StringTokenizer(value.toString());
//            Token token = callTokenizer(value.toString());
            String[] token = value.toString().split("\\s+");
            if(token.length==3){
                // set key value
                word.set(token[0]+" "+token[1]);
                context.write(word, new DoubleWritable(Double.valueOf(token[2])));
        }
        }
    }
//    public static Token callTokenizer(String msg){
//        String[] token = msg.split("\\s+");
//        if(token.length == 3){
//            double time = Double.parseDouble(token[2]);
//            String call = token[0]+" "+token[1];
//            return new Token(call,time);
//        }
//        return null;
//    }
//    public static class Token{
//        public String call;
//        public double time;
//
//        public Token(String call, double time) {
//            this.call = call;
//            this.time = time;
//        }
//
//        public String getCall() {
//            return call;
//        }
//
//        public void setCall(String call) {
//            this.call = call;
//        }
//
//        public double getTime() {
//            return time;
//        }
//
//        public void setTime(double time) {
//            this.time = time;
//        }
//    }


//    public static class DoubleSumCombiner
//            extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
//        private DoubleWritable result = new DoubleWritable();
//
//        public void reduce(Text key, Iterable<DoubleWritable> values,
//                           Context context
//        ) throws IOException, InterruptedException {
//            double sum = 0;
//            int num = 0;
//            for (DoubleWritable val : values) {
//                sum += val.get();
//                num++;
//            }
//            Double average_time = sum/num;
//            result.set(average_time);
//            context.write(key, result);
//        }
//    }

    // This is the Reducer class
    // reference http://hadoop.apache.org/docs/r2.6.0/api/org/apache/hadoop/mapreduce/Reducer.html
    //
    // We want to control the output format to look at the following:
    //
    // count of word = count
    //
    public static class IntSumReducer
            extends Reducer<Text,DoubleWritable,Text,Text> {

        private Text result_key= new Text();
        private Text result_value= new Text();
        private byte[] prefix;
        private byte[] suffix;

        protected void setup(Context context) {
            try {
                prefix= Text.encode(" ").array();
                suffix= Text.encode(" ").array();
            } catch (Exception e) {
                prefix = suffix = new byte[0];
            }
        }

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sum = 0;
            int num = 0;
            // sum
            for (DoubleWritable val : values) {
                sum += val.get();
                num++;
            }
            // average
            double res = sum/num;
            // format  output
            String value = Integer.toString(num) +" "+String.format("%.3f",res);
            result_key.set(key.getBytes(), 0, key.getLength());
            result_value.set(value);
            // write result
            context.write(result_key, result_value);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "word count");

        job.setJarByClass(Hw2Part1.class);

        job.setMapperClass(TokenizerMapper.class);
//        job.setCombinerClass(DoubleSumCombiner.class);
        job.setReducerClass(IntSumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // add the input paths as given by command line
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        // add the output path as given by the command line
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
