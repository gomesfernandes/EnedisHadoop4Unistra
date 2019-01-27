import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DegreSommets {

    public static class CoupleToNodeMapper extends Mapper<LongWritable, Text, Text, Text>{
        /**
         *
         */
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            continue
        }
    }

    public static class CoupleDegreeReducer extends Reducer<Text,Text,Text,Text> {

        private Text result = new Text();
        /**
         *
         */
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            continue
        }
    }
}

public static class IDMapper extends Mapper<LongWritable, Text, Text, Text>{

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        continue
    }

}

public static class FinalReducer extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {

        continue
    }
}

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage : hadoop jar Enedis.jar Enedis input output");
            System.exit(0);
        }

        Configuration conf = new Configuration();

        Job job1 = new Job(conf, "FirstRun");
        job1.setJarByClass(DegreSommets.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setMapperClass(CoupleToNodeMapper.class);
        job1.setReducerClass(CoupleDegreeReducer.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);

        System.out.println("----------------------------------------------");
        System.out.println("END OF FIRST JOB");
        System.out.println("----------------------------------------------");

        /*
        Job job2 = new Job(conf, "SecondRun");
        job2.setJarByClass(DegreSommets.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setMapperClass(IDMapper.class);
        job2.setReducerClass(FinalReducer.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        String inFile = new String(args[1]+"/part-r-00000");
        FileInputFormat.addInputPath(job2, new Path(inFile));

        String outfinal = new String(args[1]+"-final");
        FileOutputFormat.setOutputPath(job2, new Path(outfinal));

        job2.waitForCompletion(true);
        */

    }

}