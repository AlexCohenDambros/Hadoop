package tde1.questao6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
import tde1.hadoop.Hadoop;

import java.io.IOException;

public class Questao6 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path("in/transactions_amostra.csv");

        // arquivo de saida
        Path output = new Path("output/Resultadotde1QT6.csv");

        // criacao do job e seu nome
        Job j = new Job(c, "hadoopqt6");

        // Registro de classes (main, map, reduce)
        j.setJarByClass(Questao6.class);
        j.setMapperClass(MapForWordCount.class);
        j.setReducerClass(ReduceForWordCount.class);
        j.setCombinerClass(CombineForAverage.class);

        // Definir tipos de saida (map e reduce)
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(QuestaoSeis.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);

        // Cadastro de arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        //Executar rotina
        j.waitForCompletion(false);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, QuestaoSeis> {

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

            // convertendo para string
            String line = value.toString();

            String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

            String year = columns[1];
            double price = Double.parseDouble(columns[5]);
            String unit = columns[7];

            //Questão 6:
            con.write(new Text(unit + " " + year), new QuestaoSeis(1, price, 0, 0));
        }
    }

    public static class CombineForAverage extends Reducer<Text, QuestaoSeis, Text, QuestaoSeis> {
        public void reduce(Text key, Iterable<QuestaoSeis> values, Context con) throws IOException, InterruptedException {

            int totalN = 0;
            double totalSoma = 0;
            double max = Double.MIN_VALUE;
            double min = Double.MAX_VALUE;


            for (QuestaoSeis v : values) {
                totalSoma += v.getValores();
                totalN += v.getN();
                if (v.getValores() < min) {
                    min = v.getValores();
                }

                if (v.getValores() > max) {
                    max = v.getValores();
                }

            }

            con.write(key, new QuestaoSeis(totalN, totalSoma, max, min));

        }
    }

    public static class ReduceForWordCount extends Reducer<Text, QuestaoSeis, Text, Text> {

        public void reduce(Text key, Iterable<QuestaoSeis> values, Context con) throws IOException, InterruptedException {

            double totalN = 0;
            double totalSoma = 0;
            double max = Double.MIN_VALUE;
            double min = Double.MAX_VALUE;


            for (QuestaoSeis v : values) {
                totalSoma += v.getValores();
                totalN += v.getN();

                if (v.getMin() < min) {
                    min = v.getMin();
                }

                if (v.getMax() > max) {
                    max = v.getMax();
                }
            }

            double mediaTotal = totalSoma / totalN;

            con.write(key, new Text(max + "  " + min + "  " + mediaTotal));

        }
    }
}
