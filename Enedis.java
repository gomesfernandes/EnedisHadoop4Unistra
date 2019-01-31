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

    public static String[] SECTORLABELS =
            {"Residentiel",
            "Professionnel",
            "Agriculture",
            "Industrie",
            "Tertiaire",
            "Autres"};

    public static String[] SURFACELABELS =
            {"Surface petite",
            "Surface moyenne",
            "Surface large"};

    public static String[] RESIDENCYLABELS =
            {"Résidence ancienne",
            "Résidence âge moyenne",
            "Résidence récente"};

    public static String[] CONSOLABELS =
            {"Conso très basse",
            "Conso basse",
            "Conso modérée",
            "Conso élevée"};

    public static String[] HEATINGLABELS =
            {"Taux chauffage électr. très bas",
            "Taux chauffage électr. bas",
            "Taux chauffage électr. moyen",
            "Taux chauffage électr. élevé"};

    public static final double EPSILON = 0.01;

    public static Float global_min_conso = Float.MAX_VALUE;
    public static Float global_max_conso = Float.MIN_VALUE;
    public static Float global_min_heating = Float.MAX_VALUE;
    public static Float global_max_heating = Float.MIN_VALUE;

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

    /**
     * Calculate the sum of the values for "Superficie des logements 80 à 100 m2" and
     * "Superficie des logements > 100 m2".
     *
     * @param cols: one line from the dataset, split into a list of Strings
     * @return percentage of apartments with a surface > 80 m2
     */
    public static Float extractLargeSurfacePercentage(String[] cols) {
        return Float.parseFloat(cols[31]) + Float.parseFloat(cols[32]);
    }

    /**
     * Calculate the sum of the values for "Résidences principales avant 1919" and
     * "Résidences principales de 1919 à 1945".
     *
     * @param cols: one line from the dataset, split into a list of Strings
     * @return percentage of residencies build before 1945
     */
    public static Float extractOldResidencePercentage(String[] cols) {
        return Float.parseFloat(cols[33]) + Float.parseFloat(cols[34]);
    }

     /**
     * Calculate the percentage of apartments with < 40 m2 , between 40 and 80 m2, and over 80 m2.
     * Return the label for the interval with the largest percentage.
     *
     * @param cols: one line from the dataset, split into a list of Strings
     * @return label of the surface size with max percentage
     */
    public static String extractSurfaceWithMaxPercentages(String[] cols) {
        Float small_surface_percentage = Float.parseFloat(cols[27]) + Float.parseFloat(cols[28]);

        Float medium_surface_percentage = Float.parseFloat(cols[29]) + Float.parseFloat(cols[30]);

        Float large_surface_percentage = Float.parseFloat(cols[31]) + Float.parseFloat(cols[32]);

        Float max = Math.max(small_surface_percentage, Math.max(medium_surface_percentage,large_surface_percentage));

        if (Math.abs(max - small_surface_percentage) < EPSILON) {
            return SURFACELABELS[0];
        } else if (Math.abs(max - medium_surface_percentage) < EPSILON) {
            return SURFACELABELS[1];
        } else {
            return SURFACELABELS[2];
        }
    }

    /**
     * Calculate the percentage of apartments built before 1945 , between 1946 and 1990, and after 1991.
     * Return the label for the interval with the largest percentage.
     *
     * @param cols: one line from the dataset, split into a list of Strings
     * @return label of the interval with max percentage
     */
    public static String extractResidenceWithMaxPercentages(String[] cols) {
        Float old_residence_percentage = Float.parseFloat(cols[33]) + Float.parseFloat(cols[34]);

        Float medium_residence_percentage = Float.parseFloat(cols[35]) + Float.parseFloat(cols[36]);

        Float new_residence_percentage = Float.parseFloat(cols[37]) + Float.parseFloat(cols[38]) + Float.parseFloat(cols[39]);

        Float max = Math.max(old_residence_percentage, Math.max(medium_residence_percentage,new_residence_percentage));

        if (Math.abs(max - old_residence_percentage) < EPSILON) {
            return RESIDENCYLABELS[0];
        } else if (Math.abs(max - medium_residence_percentage) < EPSILON) {
            return RESIDENCYLABELS[1];
        } else {
            return RESIDENCYLABELS[2];
        }
    }

    public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text>{

        /**
         * Get a line from the dataset and write the following to the context:
         * key = commune_code
         * value = average_residence_consumption:surface_label:residence_label:electric_heating_rate
         *
         * @param key: line number
         * @param value: one line from the dataset
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] cols = value.toString().split(";");
            String commune = cols[2];

            if (Float.parseFloat(cols[10]) == 0.0)
                return; //pas de sites ENEDIS

            Float avg_residence = Float.parseFloat(cols[12]);

            String taux_chauffage = cols[40];

            StringBuilder output_value = new StringBuilder();
            output_value.append(avg_residence.toString());
            output_value.append(":");
            output_value.append(extractSurfaceWithMaxPercentages(cols));
            output_value.append(":");
            output_value.append(extractResidenceWithMaxPercentages(cols));
            output_value.append(":");
            output_value.append(taux_chauffage);

            context.write(new Text(commune), new Text(output_value.toString()));
        }
    }

    public static class Reduce1 extends Reducer<Text,Text,Text,Text> {

        /**
         * Calculate the average yearly consumption for the residence sector for the comune given by *key*.
         * Set global variables to find min and max consumption and electric heating rate.
         *
         * Write the following to the context:
         * key = average_energy_consumption
         * value = surface_label:residence_label:electric_heating_rate
         *
         * @param key: commune code
         * @param values: list of values mapped by Mapper1
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            List<Float> list_consumption = new ArrayList<Float>();
            String surface = new String();
            String residence = new String();
            String heating_string = new String();

            for (Text v : values) {
                String[] content = v.toString().split(":");
                Float consumption = Float.parseFloat(content[0]);
                list_consumption.add(consumption);

                surface = content[1];
                residence = content[2];
                heating_string = content[3];
            }

            Float heating = Float.parseFloat(heating_string);

            Float avg = new Float(0);
            for(Float entry : list_consumption) {
                avg += entry;
            }
            avg= avg/list_consumption.size();

            if (avg > global_max_conso) {
                global_max_conso = avg;
            }
            if (avg < global_min_conso) {
                global_min_conso = avg;
            }
            if (heating > global_max_heating) {
                global_max_heating = heating;
            }
            if (heating < global_min_heating) {
                global_min_heating = heating;
            }

            StringBuilder output = new StringBuilder();
            output.append(surface);
            output.append(":");
            output.append(residence);
            output.append(":");
            output.append(heating_string);

            context.write(new Text(avg.toString()), new Text(output.toString()));
        }
    }

    /**
     * Split the interval between the min consumption and max consumption of the dataset into 4 parts.
     * Determine to which interval the input consumption belongs, and return its label.
     *
     * @param conso: average yearly consumption for the residency sector of a commune
     * @return the label of the interval
     */
    public static String categoriseByConsoRange(Float conso) {
        Float middle = (global_max_conso-global_min_conso)/2;
        Float quarter = (middle-global_min_conso)/2;
        Float three_quarter = (global_max_conso-quarter);

        if (conso < quarter)
            return CONSOLABELS[0];
        else if (conso < middle)
            return CONSOLABELS[1];
        else if (conso < three_quarter)
            return CONSOLABELS[2];
        else
            return CONSOLABELS[3];
    }

    /**
     * Split the interval between the min and max electricat heating rate of the dataset into 4 parts.
     * Determine to which interval the input consumption belongs, and return its label.
     *
     * @param heating: rate of apartments using electrical heating
     * @return the label of the interval
     */
    public static String categoriseByHeatingRange(Float heating) {
        Float middle = (global_max_heating-global_min_heating)/2;
        Float quarter = (middle-global_min_heating)/2;
        Float three_quarter = (global_max_heating-quarter);

        if (heating < quarter)
            return HEATINGLABELS[0];
        else if (heating < middle)
            return HEATINGLABELS[1];
        else if (heating < three_quarter)
            return HEATINGLABELS[2];
        else
            return HEATINGLABELS[3];
    }

    public static class Mapper2 extends Mapper<LongWritable, Text, Text, Text>{

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            Float conso = Float.parseFloat(value.toString().split("\t")[0]);
            String[] values = value.toString().split("\t")[1].split(":");

            String conso_category = categoriseByConsoRange(conso);
            String heating_category = categoriseByHeatingRange(Float.parseFloat(values[2]));

            StringBuilder output = new StringBuilder();
            output.append(values[0]);
            output.append(":");
            output.append(values[1]);
            output.append(":");
            output.append(heating_category);

            context.write(new Text(conso_category),new Text(output.toString()));
        }

    }

    public static String getMaxCategory(Map<String, Integer> map){
        Map.Entry<String, Integer> maxEntry = null;
        Integer max = Collections.max(map.values());

        for(Map.Entry<String, Integer> entry : map.entrySet()) {
            Integer value = entry.getValue();
            if(null != value && max == value) {
                maxEntry = entry;
            }
        }
        return maxEntry.getKey();
    }

    public static class Reducer2 extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            Map<String,Integer> surface_map = new HashMap<String, Integer>();
            Map<String,Integer> residence_map = new HashMap<String, Integer>();
            Map<String,Integer> heating_map = new HashMap<String, Integer>();
            int i;
            for(i=0; i<SURFACELABELS.length; i++)
                surface_map.put(SURFACELABELS[i],0);

            for(i=0; i<RESIDENCYLABELS.length; i++)
                residence_map.put(RESIDENCYLABELS[i],0);

            for(i=0; i< HEATINGLABELS.length; i++)
                heating_map.put(HEATINGLABELS[i],0);

            for (Text entry : values){
                String[] value = entry.toString().split(":");
                surface_map.put(value[0],surface_map.get(value[0]) + 1);
                residence_map.put(value[1],residence_map.get(value[1]) + 1);
                heating_map.put(value[2],heating_map.get(value[2]) + 1);
            }

            String max_surface_categ = getMaxCategory(surface_map);
            String max_residence_categ = getMaxCategory(residence_map);
            String max_heating_categ = getMaxCategory(heating_map);

            context.write(key,new Text(max_surface_categ+","+max_residence_categ+","+max_heating_categ));
        }
    }

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
        job1.setMapperClass(Mapper1.class);
        job1.setReducerClass(Reduce1.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);

        System.out.println("----------------------------------------------");
        System.out.println("END OF FIRST JOB");
        System.out.println("----------------------------------------------");

        Job job2 = new Job(conf, "SecondRun");
        job2.setJarByClass(Enedis.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setMapperClass(Mapper2.class);
        job2.setReducerClass(Reducer2.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        String inFile = new String(args[1]+"/part-r-00000");
        FileInputFormat.addInputPath(job2, new Path(inFile));

        String outfinal = new String(args[1]+"-final");
        FileOutputFormat.setOutputPath(job2, new Path(outfinal));

        job2.waitForCompletion(true);

    }

}