// Questões 1 até a 5


package tde1.hadoop;

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
import java.util.Objects;

public class Hadoop {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path("in/transactions_amostra.csv");

        // arquivo de saida
        Path output = new Path("output/Resultadotde1.csv");

        // criacao do job e seu nome
        Job j = new Job(c, "hadoop");

        // Registro de classes (main, map, reduce)
        j.setJarByClass(Hadoop.class);
        j.setMapperClass(MapForWordCount.class);
        j.setReducerClass(ReduceForWordCount.class);
        j.setCombinerClass(CombineForAverage.class);

        // Definir tipos de saida (map e reduce)
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(AverageCommodity.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        // Cadastro de arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        //Executar rotina
        j.waitForCompletion(false);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, AverageCommodity> {

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

            // convertendo para string
            String line = value.toString();

            String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

            String year = columns[1];
            String flowType = columns[4];
            double price = Double.parseDouble(columns[5]);
            String unit = columns[7];
            String category = columns[9];
            String pais = columns[0];

            // Questao 1:
            if (pais.compareTo("Brazil") == 0){
                con.write(new Text("Questão 1: " + pais), new AverageCommodity(1, 0));
            }
            //Questão 2:
            con.write(new Text("Questão 2: " + year), new AverageCommodity(1, 0));

            // Questão 3:
            con.write(new Text("Questão 3: " + year + " " + flowType), new AverageCommodity(1, 0));

            // Questão 4:
            con.write(new Text("Questão 4: " + year), new AverageCommodity(1, price));

            // Questao 5:
            if (pais.compareTo("Brazil") == 0 && flowType.compareTo("Export") == 0) {
                con.write(new Text("Questão 5: " + unit + " " + year + " " + category + " " + flowType + " " + pais), new AverageCommodity(1, price));
            }

        }
    }


    public static class CombineForAverage extends Reducer<Text, AverageCommodity, Text, AverageCommodity> {
        public void reduce(Text key, Iterable<AverageCommodity> values, Context con) throws IOException, InterruptedException {

            int totalN = 0;
            double totalSoma = 0;

            for (AverageCommodity v : values) {
                totalSoma += v.getSoma();
                totalN += v.getN();
            }

            con.write(key, new AverageCommodity(totalN, totalSoma));


        }
    }

    public static class ReduceForWordCount extends Reducer<Text, AverageCommodity, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<AverageCommodity> values, Context con) throws IOException, InterruptedException {

            double totalN = 0;
            double totalSoma = 0;

            for (AverageCommodity v : values) {
                totalSoma += v.getSoma();
                totalN += v.getN();
            }
            double mediaTotal = totalSoma / totalN;

            // para diferenciar qual con.write sera utilizado
            // apenas as questoes 4 e 5 possuem valores no atributo de soma
            // caso a mediaTotal seja diferente de 0 sera feito um con.write com a mediaTotal (Questões 4 e 5)
            // senão irá fazer o con.write com o totalN (Questões 1, 2 e 3)

            if (mediaTotal != 0) {
                con.write(key, new DoubleWritable(mediaTotal));
            } else {
                con.write(key, new DoubleWritable(totalN));
            }

        }
    }
}