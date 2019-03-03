import java.io.IOException;
import java.util.*;
import java.text.DecimalFormat;
import java.math.RoundingMode;
import java.io.File;
import java.util.Scanner;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * 2 Map-Reduce passes
 *
 * Map 1 : Get input line and extract info on consumption, surface, heating, collective housing, population
 * Reduce 1 : Per category, calculate average per commune
 *            -> Fix max and min values for consumption, heating, population
 *            -> Use these values to dynamically split the corresponding categories
 * Map 2 : Assign to consumption category and output the rest
 * Reduce 2 : Per category, count values in each interval and get max
 */

public class EnedisByConsumption {

    public static String[] SURFACELABELS =
            {"Surfaces petites(<30m2)",
            "Surfaces moyennes(>=30,<100m2)",
            "Surfaces larges(>=100m2)"};

    public static String[] RESIDENCYLABELS =
            {"Résidences anciennes(<=1970)",
            "Résidences âge moyen(>1970, <=2010)",
            "Résidences récentes(>2010)"};

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

    public static String[] COLLECTIVEHOUSINGLABELS =
            {"Taux logements collectifs très bas",
            "Taux logements collectifs bas",
            "Taux logements collectifs moyen",
            "Taux logements collectifs élevé"};

    public static String[] POPULATIONLABELS =
            {"Nombre habitants bas",
            "Nombre habitants moyen",
            "Nombre habitants élevé"};

    public static final double EPSILON = 0.01;

    public static Float global_25_conso = Float.MIN_VALUE;
    public static Float global_50_conso = Float.MIN_VALUE;
    public static Float global_75_conso = Float.MIN_VALUE;

    public static Float global_25_heating = Float.MIN_VALUE;
    public static Float global_50_heating = Float.MIN_VALUE;
    public static Float global_75_heating = Float.MIN_VALUE;

    public static Float global_25_housing = Float.MIN_VALUE;
    public static Float global_50_housing = Float.MIN_VALUE;
    public static Float global_75_housing = Float.MIN_VALUE;

    public static Float global_33_population = Float.MIN_VALUE;
    public static Float global_66_population = Float.MIN_VALUE;

    public static class ValuesMapper extends Mapper<LongWritable, Text, Text, Text>{

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String conso = value.toString().split("\t")[0];
            String[] values = value.toString().split("\t")[1].split(":");

            context.write(new Text("conso"), new Text(conso));
            context.write(new Text("housing"), new Text(values[0]));
            context.write(new Text("heating"), new Text(values[3]));
            context.write(new Text("population"), new Text(values[4]));
        }
    }
    public static class MinMaxReducer extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            List<Float> values_list = new ArrayList<Float>();

            for (Text entry : values){
                Float value = Float.parseFloat(entry.toString());
                values_list.add(value);
            }

            Float min = Collections.min(values_list);
            Float max = Collections.max(values_list);
            Collections.sort(values_list);

            if (Objects.equals(key.toString(),"population")) {
                int i33 = values_list.size()/3;
                Float pop33 = values_list.get(i33);
                Float pop66 = values_list.get(i33*2);
                context.write(key, new Text(min+"\t"+max+"\t"+pop33+"\t"+pop66));
            } else {
                int i25 = values_list.size()/4;
                Float val25 = values_list.get(i25);
                Float val50 = values_list.get(i25*2);
                Float val75 = values_list.get(i25*2 + i25);
                context.write(key, new Text(min+"\t"+max+"\t"+val25+"\t"+val50+"\t"+val75));
            }
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

            String avg_residence = cols[12];
            String population = cols[24];
            String collective_housing_rate = cols[25];
            String electric_heating_rate = cols[40];

            StringBuilder output_value = new StringBuilder();
            output_value.append(avg_residence.toString());
            output_value.append(":");
            output_value.append(collective_housing_rate);
            output_value.append(":");
            output_value.append(extractSurfaceWithMaxPercentages(cols));
            output_value.append(":");
            output_value.append(extractResidenceWithMaxPercentages(cols));
            output_value.append(":");
            output_value.append(electric_heating_rate);
            output_value.append(":");
            output_value.append(population);

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
            String housing_string = new String();
            String population_string = new String();

            for (Text v : values) {
                String[] content = v.toString().split(":");
                Float consumption = Float.parseFloat(content[0]);
                list_consumption.add(consumption);

                housing_string = content[1];
                surface = content[2];
                residence = content[3];
                heating_string = content[4];
                population_string = content[5];
            }

            Float heating = Float.parseFloat(heating_string);
            Float housing = Float.parseFloat(housing_string);
            Float population = Float.parseFloat(population_string);
            Float avg = new Float(0);
            for(Float entry : list_consumption) {
                avg += entry;
            }
            avg= avg/list_consumption.size();

            StringBuilder output = new StringBuilder();
            output.append(housing);
            output.append(":");
            output.append(surface);
            output.append(":");
            output.append(residence);
            output.append(":");
            output.append(heating_string);
            output.append(":");
            output.append(population_string);

            context.write(new Text(avg.toString()), new Text(output.toString()));
        }
    }
    public static class Mapper2 extends Mapper<LongWritable, Text, Text, Text>{

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            Float conso = Float.parseFloat(value.toString().split("\t")[0]);
            String[] values = value.toString().split("\t")[1].split(":");

            String conso_category = categoriseByConsoRange(conso);
            String housing_category = categoriseByCollectiveHousingRange(Float.parseFloat(values[0]));
            String heating_category = categoriseByHeatingRange(Float.parseFloat(values[3]));
            String population_category = categoriseByPopulationRange(Float.parseFloat(values[4]));

            StringBuilder output = new StringBuilder();
            output.append(housing_category);
            output.append(":");
            output.append(values[1]);
            output.append(":");
            output.append(values[2]);
            output.append(":");
            output.append(heating_category);
            output.append(":");
            output.append(population_category);

            context.write(new Text(conso_category),new Text(output.toString()));
        }

    }
    public static class Reducer2 extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            Map<String,Integer> surface_map = new HashMap<String, Integer>();
            Map<String,Integer> residence_map = new HashMap<String, Integer>();
            Map<String,Integer> heating_map = new HashMap<String, Integer>();
            Map<String,Integer> housing_map = new HashMap<String, Integer>();
            Map<String,Integer> population_map = new HashMap<String, Integer>();
            int i;
            for(i=0; i<SURFACELABELS.length; i++)
                surface_map.put(SURFACELABELS[i],0);

            for(i=0; i<RESIDENCYLABELS.length; i++)
                residence_map.put(RESIDENCYLABELS[i],0);

            for(i=0; i< HEATINGLABELS.length; i++)
                heating_map.put(HEATINGLABELS[i],0);

            for(i=0; i< COLLECTIVEHOUSINGLABELS.length; i++)
                housing_map.put(COLLECTIVEHOUSINGLABELS[i],0);

            for(i=0; i< POPULATIONLABELS.length; i++)
                population_map.put(POPULATIONLABELS[i],0);

            for (Text entry : values){
                String[] value = entry.toString().split(":");
                housing_map.put(value[0],housing_map.get(value[0]) + 1);
                surface_map.put(value[1],surface_map.get(value[1]) + 1);
                residence_map.put(value[2],residence_map.get(value[2]) + 1);
                heating_map.put(value[3],heating_map.get(value[3]) + 1);
                population_map.put(value[4],population_map.get(value[4]) + 1);
            }

            String max_surface_categ = getMaxCategory(surface_map);
            String max_residence_categ = getMaxCategory(residence_map);
            String max_heating_categ = getMaxCategory(heating_map);
            String max_housing_categ = getMaxCategory(housing_map);
            String max_population_categ = getMaxCategory(population_map);

            context.write(key, new Text(
                    "\n\t"
                            + max_housing_categ + "\n\t"
                            + max_surface_categ + "\n\t"
                            + max_residence_categ + "\n\t"
                            + max_heating_categ + "\n\t"
                            + max_population_categ)
            );
        }
    }

    public static void readMinMax(String output) {
        try {
            File file = new File(output+"-minmax/part-r-00000");
            Scanner input = new Scanner(file);

            while (input.hasNextLine()) {
                String[] tokens = input.nextLine().split("\t");
                if (Objects.equals(tokens[0],"housing")) {
                    global_25_housing = Float.parseFloat(tokens[3]);
                    global_50_housing = Float.parseFloat(tokens[4]);
                    global_75_housing = Float.parseFloat(tokens[5]);
                } else if (Objects.equals(tokens[0],"conso")) {
                    global_25_conso = Float.parseFloat(tokens[3]);
                    global_50_conso = Float.parseFloat(tokens[4]);
                    global_75_conso = Float.parseFloat(tokens[5]);
                } else if (Objects.equals(tokens[0],"heating")) {
                    global_25_heating = Float.parseFloat(tokens[3]);
                    global_50_heating = Float.parseFloat(tokens[4]);
                    global_75_heating = Float.parseFloat(tokens[5]);
                } else {
                    global_33_population = Float.parseFloat(tokens[3]);
                    global_66_population = Float.parseFloat(tokens[4]);
                }
            }
            input.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
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
     * Calculate the percentage of apartments with < 30 m2 , between 30 and 100 m2, and over 100 m2.
     * Return the label for the interval with the largest percentage.
     *
     * @param cols: one line from the dataset, split into a list of Strings
     * @return label of the surface size with max percentage
     */
    public static String extractSurfaceWithMaxPercentages(String[] cols) {
        Float small_surface_percentage = Float.parseFloat(cols[27]);

        Float medium_surface_percentage = Float.parseFloat(cols[28])+
                Float.parseFloat(cols[29]) +
                Float.parseFloat(cols[30]) +
                Float.parseFloat(cols[31]);

        Float large_surface_percentage = Float.parseFloat(cols[32]);

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
     * Calculate the percentage of apartments built before 1970 , between 1971 and 2010, and after 2011.
     * Return the label for the interval with the largest percentage.
     *
     * @param cols: one line from the dataset, split into a list of Strings
     * @return label of the interval with max percentage
     */
    public static String extractResidenceWithMaxPercentages(String[] cols) {
        Float old_residence_percentage = Float.parseFloat(cols[33]) + Float.parseFloat(cols[34]) + Float.parseFloat(cols[35]);

        Float medium_residence_percentage =  Float.parseFloat(cols[36]) + Float.parseFloat(cols[37]) + Float.parseFloat(cols[38]);

        Float new_residence_percentage =  Float.parseFloat(cols[39]);

        Float max = Math.max(old_residence_percentage, Math.max(medium_residence_percentage,new_residence_percentage));

        if (Math.abs(max - old_residence_percentage) < EPSILON) {
            return RESIDENCYLABELS[0];
        } else if (Math.abs(max - medium_residence_percentage) < EPSILON) {
            return RESIDENCYLABELS[1];
        } else {
            return RESIDENCYLABELS[2];
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
        if (conso < global_25_conso)
            return CONSOLABELS[0];
        else if (conso < global_50_conso)
            return CONSOLABELS[1];
        else if (conso < global_75_conso)
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

        if (heating < global_25_heating) {
            return HEATINGLABELS[0];
        } else if (heating < global_50_heating) {
            return HEATINGLABELS[1];
        } else if (heating < global_75_heating) {
            return HEATINGLABELS[2];
        } else {
            return HEATINGLABELS[3];
        }
    }

    public static String categoriseByCollectiveHousingRange(Float housing) {
        if (housing < global_25_housing) {
            return COLLECTIVEHOUSINGLABELS[0];
        } else if (housing < global_50_housing) {
            return COLLECTIVEHOUSINGLABELS[1];
        } else if (housing < global_75_housing) {
            return COLLECTIVEHOUSINGLABELS[2];
        } else {
            return COLLECTIVEHOUSINGLABELS[3];
        }
    }

    public static String categoriseByPopulationRange(Float population) {
        if (population < global_33_population) {
            return POPULATIONLABELS[0];
        } else if (population < global_66_population) {
            return POPULATIONLABELS[1];
        } else {
            return POPULATIONLABELS[2];
        }
    }

    public static void update_category_labels() {
        DecimalFormat df = new DecimalFormat("##.##");
        df.setRoundingMode(RoundingMode.DOWN);

        CONSOLABELS[0] = CONSOLABELS[0]+"(<"+df.format(global_25_conso)+"MWh)";
        CONSOLABELS[1] = CONSOLABELS[1]+"(>="+df.format(global_25_conso)
                                        +",<"+df.format(global_50_conso)+"MWh)";
        CONSOLABELS[2] = CONSOLABELS[2]+"(>="+df.format(global_50_conso)
                                        +",<"+df.format(global_75_conso)+"MWh)";
        CONSOLABELS[3] = CONSOLABELS[3]+"(>="+df.format(global_75_conso)+"MWh)";

        COLLECTIVEHOUSINGLABELS[0] = "Taux logements coll. très bas(<" + df.format(global_25_housing) + "%)";
        COLLECTIVEHOUSINGLABELS[1] = "Taux logements coll. bas(>=" +  df.format(global_25_housing)
                                    + ",<" + df.format(global_50_housing) + "%)";
        COLLECTIVEHOUSINGLABELS[2] = "Taux logements coll. moyen(>=" + df.format(global_50_housing)
                                    + ",<" + df.format(global_75_housing) + "%)";
        COLLECTIVEHOUSINGLABELS[3] = "Taux logements coll. élevé(>=" + df.format(global_75_housing) + "%)";

        HEATINGLABELS[0] = "Taux chauff. électr. très bas(<" + df.format(global_25_heating) + "%)";
        HEATINGLABELS[1] = "Taux chauff. électr. bas(>=" + df.format(global_25_heating)
                                + ",<" + df.format(global_50_heating) + "%)";
        HEATINGLABELS[2] = "Taux chauff. électr. moyen(>=" + df.format(global_50_heating)
                                 + ",<" +df.format(global_75_heating)+"%)";
        HEATINGLABELS[3] = "Taux chauff. électr. élevé(>="+df.format(global_75_heating)+"%)";

        POPULATIONLABELS[0] = "Nb habitants bas(<" + df.format(global_33_population) + ")";
        POPULATIONLABELS[1] = "Nb habitants moyen(>="+df.format(global_33_population)+
                                                ",<" + df.format(global_66_population) + ")";
        POPULATIONLABELS[2] = "Nb habitants élevé(>=" + df.format(global_66_population) + ")";
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


    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage : hadoop jar EnedisByConsumption.jar EnedisByConsumption input output");
            System.exit(0);
        }

        Configuration conf = new Configuration();

        Job job0 = new Job(conf, "FirstRun");
        job0.setJarByClass(EnedisByConsumption.class);
        job0.setOutputKeyClass(Text.class);
        job0.setOutputValueClass(Text.class);
        job0.setMapperClass(Mapper1.class);
        job0.setReducerClass(Reduce1.class);
        job0.setInputFormatClass(TextInputFormat.class);
        job0.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job0, new Path(args[0]));
        FileOutputFormat.setOutputPath(job0, new Path(args[1]));

        job0.waitForCompletion(true);

        System.out.println("----------------------------------------------");
        System.out.println("END OF FIRST JOB");
        System.out.println("----------------------------------------------");

        Job job1 = new Job(conf, "MinMaxRun");
        job1.setJarByClass(EnedisByConsumption.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setMapperClass(ValuesMapper.class);
        job1.setReducerClass(MinMaxReducer.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(args[1]+"/part-r-00000"));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]+"-minmax"));

        job1.waitForCompletion(true);

        System.out.println("----------------------------------------------");
        System.out.println("END OF MIN MAX JOB");
        System.out.println("----------------------------------------------");

        readMinMax(args[1]);
        update_category_labels();

        Job job2 = new Job(conf, "SecondRun");
        job2.setJarByClass(EnedisByConsumption.class);
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