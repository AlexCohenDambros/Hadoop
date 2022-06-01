package tde1.questao7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class Questao7 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path("in/transactions_amostra.csv");

        // arquivo de saida
        Path output = new Path("output/Resultadotde1QT7.csv");

        Path outputFinal = new Path("output/SaidaFinalQT7.csv");

        // criacao do job e seu nome
        Job j = new Job(c, "hadoopqt7");

        // Registro de classes (main, map, reduce)
        j.setJarByClass(Questao7.class);
        j.setMapperClass(MapForWordCount.class);
        j.setReducerClass(ReduceForWordCount.class);
        j.setCombinerClass(CombineForAverage.class);

        // Definir tipos de saida (map e reduce)
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(DoubleWritable.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        // Cadastro de arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        //Executar rotina
        j.waitForCompletion(false);

        // lanca o job e aguarda sua execucao
        j.waitForCompletion(true);

        // criacao do job e seu nome
        j = new Job(c, "hadoopqt7B");

        // Registro de classes (main, map, reduce)
        j.setJarByClass(Questao7.class);
        j.setMapperClass(MapForWordCount2.class);
        j.setReducerClass(ReduceForWordCount2.class);
        j.setCombinerClass(CombineForAverage2.class);

        // Definir tipos de saida (map e reduce)
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(Q7Writable.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Q7Writable.class);

        // Cadastro de arquivos de entrada e saida
        FileInputFormat.addInputPath(j, output);
        FileOutputFormat.setOutputPath(j, outputFinal);


        //Executar rotina
        j.waitForCompletion(false);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

            // convertendo para string
            String line = value.toString();

            String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);


            double amount = Double.parseDouble(columns[8]);
            String commodity = String.valueOf(columns[3]);
            String flowType = String.valueOf(columns[4]);
            String year = columns[1];

            //Questão 7:
            if (year.equals("2016")) {
                con.write(new Text(commodity + "," + flowType), new DoubleWritable(amount));
            }
        }
    }

    public static class CombineForAverage extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context con) throws IOException, InterruptedException {

            double totalN = 0;

            for (DoubleWritable valor : values) {
                totalN += valor.get();
            }

            con.write(key, new DoubleWritable(totalN));
        }
    }

    public static class ReduceForWordCount extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context con) throws IOException, InterruptedException {

            double totalN = 0;

            for (DoubleWritable valor : values) {
                totalN += valor.get();
            }

            con.write(key, new DoubleWritable(totalN));

        }
    }

    public static class MapForWordCount2 extends Mapper<LongWritable, Text, Text, Q7Writable> {

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

            // convertendo para string
            String line = value.toString();

            String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            String[] columns2 = columns[1].split("\t");

            String comm = columns[0];
            String type = columns2[0];
            double qty = Double.parseDouble(columns2[1]);

            con.write(new Text(type), new Q7Writable(comm, qty));
        }
    }

    public static class CombineForAverage2 extends Reducer<Text, Q7Writable, Text, Q7Writable> {
        public void reduce(Text key, Iterable<Q7Writable> values, Context con) throws IOException, InterruptedException {

            double currentMax = 0.0;
            String commodity = null;

            for (Q7Writable valor : values) {
                if (valor.qty > currentMax) {
                    currentMax = valor.qty;
                    commodity = valor.commodity;
                }
            }

            con.write(key, new Q7Writable(commodity, currentMax));
        }
    }

    public static class ReduceForWordCount2 extends Reducer<Text, Q7Writable, Text, Q7Writable> {

        public void reduce(Text key, Iterable<Q7Writable> values, Context con) throws IOException, InterruptedException {

            double currentMax = 0.0;
            String commodity = null;

            for (Q7Writable valor : values) {
                if (valor.qty > currentMax) {
                    currentMax = valor.qty;
                    commodity = valor.commodity;
                }
            }

            con.write(key, new Q7Writable(commodity, currentMax));
        }
    }
}