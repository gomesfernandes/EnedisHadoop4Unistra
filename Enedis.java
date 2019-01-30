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

public class Enedis {

    public static String[] SECTORLABELS = {"Residentiel",
                                            "Professionnel",
                                            "Agriculture",
                                            "Industrie",
                                            "Tertiaire",
                                            "Autres"};

    public static String getMaxSector(Float[] avgs) {
        int maxIndex = 0;
        for (int i = 1; i < avgs.length; i++) {
            Float newnumber = avgs[i];
            if ((newnumber > avgs[maxIndex])) {
                maxIndex = i;
            }
        }
        return SECTORLABELS[maxIndex];
    }

    public static Float[] extractSectorAvgs(String[] cols) {
        Float avg_residence = Float.parseFloat(cols[12]);

        Float avg_pro = Float.parseFloat(cols[15]);

        Float nb_agriculture = Float.parseFloat(cols[16]);
        Float total_agriculture = Float.parseFloat(cols[17]);
        Float avg_agriculture = total_agriculture / nb_agriculture ;

        Float nb_industry = Float.parseFloat(cols[18]);
        Float total_industry = Float.parseFloat(cols[19]);
        Float avg_industry = total_industry / nb_industry ;

        Float nb_tertiary = Float.parseFloat(cols[20]);
        Float total_tertiary = Float.parseFloat(cols[21]);
        Float avg_tertiary = total_tertiary / nb_tertiary ;

        Float nb_other = Float.parseFloat(cols[22]);
        Float total_other = Float.parseFloat(cols[23]);
        Float avg_other = total_other / nb_other ;

        Float[] avgs = {avg_residence, avg_pro, avg_agriculture, avg_industry, avg_tertiary, avg_other};

        return avgs;
    }

    public static class PerDepartmentMapper extends Mapper<LongWritable, Text, Text, Text>{
        /**
         * Per line, calculate the average consumption per site for each of the 6 sectors.
         * Return the name of the sector with the biggest consumption.
         */
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] cols = value.toString().split(";");
            String year = cols[0];
            String departement = cols[7];

            Float[] avgs = extractSectorAvgs(cols);

            String max_sector = getMaxSector(avgs);

            context.write(new Text(departement), new Text(max_sector));
        }
    }

    public static class PerSectorCountReducer extends Reducer<Text,Text,Text,Text> {

        /**
         * Per department and per sector, return the number of communes for which the sector consumed the most.
         */
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Map<String,Integer> count_per_sector = new HashMap<String,Integer>();

            for (int i=0; i < SECTORLABELS.length ; i++)
                count_per_sector.put(SECTORLABELS[i],0);

            for (Text sector : values) {
                Integer oldvalue = count_per_sector.get(sector.toString());
                count_per_sector.put(sector.toString(),oldvalue + 1);
            }

            StringBuilder output_value = new StringBuilder();
            output_value.append("");
            String sector = new String();

            for (int i=0; i < SECTORLABELS.length ; i++) {
                sector = SECTORLABELS[i];
                output_value.append(sector);
                output_value.append(":");
                output_value.append(count_per_sector.get(sector));
                if (i!=SECTORLABELS.length-1)
                    output_value.append(",");
            }

            context.write(key, new Text(output_value.toString()));
        }
    }


    public static class ExtractMaxSectorMapper extends Mapper<LongWritable, Text, Text, Text>{

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String department = value.toString().split("\t")[0];
            String counts = value.toString().split("\t")[1];
            String[] sector_counts = counts.split(",");

            Integer max = 0;
            String maxSector = "";

            for (String entry : sector_counts) {
                String sector = entry.split(":")[0];
                Integer count = Integer.parseInt(entry.split(":")[1]);
                if (count > max) {
                    max = count;
                    maxSector = sector;
                }
            }

            StringBuilder output_value = new StringBuilder();
            output_value.append(department);
            output_value.append(":");
            output_value.append(max.toString());

            context.write(new Text(maxSector), new Text(output_value.toString()));
        }

    }

    /*
    public static class Reducer2 extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            continue
        }
    }
    */

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage : hadoop jar Enedis.jar Enedis input output");
            System.exit(0);
        }

        Configuration conf = new Configuration();

        Job job1 = new Job(conf, "FirstRun");
        job1.setJarByClass(Enedis.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setMapperClass(PerDepartmentMapper.class);
        job1.setReducerClass(PerSectorCountReducer.class);

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
        job2.setJarByClass(Enedis.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setMapperClass(ExtractMaxSectorMapper.class);
        job2.setReducerClass(Reducer2.class);

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