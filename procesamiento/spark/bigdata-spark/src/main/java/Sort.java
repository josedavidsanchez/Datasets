import java.util.Arrays;
import java.util.Iterator;
import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import scala.Tuple2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class Sort {

    private static final FlatMapFunction<String, String> WORDS_EXTRACTOR
            = new FlatMapFunction<String, String>() {
        @Override
        public Iterator<String> call(String s) throws Exception {
            /*
            String strTotal = "";

            String[] arrayString = s.split("\n");
            for (String line : arrayString) {

                strTotal += line.split(",")[2] + ",";
            }

            return Arrays.asList(strTotal.split(","));*/
            return Arrays.asList(s.split(" ")).iterator();
        }
    };

    private static final PairFunction<String, String, String> WORDS_MAPPER
            = new PairFunction<String, String, String>() {
        @Override
        public Tuple2<String, String> call(String s) throws Exception {
            return new Tuple2<String, String>(s.toUpperCase(), "");
        }
    };

    private static final Function2<String, String, String> WORDS_REDUCER
            = new Function2<String, String, String>() {
        @Override
        public String call(String a, String b) throws Exception {
            return "";
        }
    };

    public static void descargar_log(JavaSparkContext javasc, String logs_folder) {
        try {
            String appId = javasc.getConf().getAppId();

            Configuration hadoopConf = javasc.hadoopConfiguration();
            FileSystem hdfsFileSystem = FileSystem.get(hadoopConf);
            String result = "";

            String local_log_path = logs_folder + "/" + javasc.appName() + "_spark.log";
            String hdfs_log_path = hdfsFileSystem.getUri() + "/spark/logs/" + appId;

            Path local = new Path(local_log_path);
            Path hdfs = new Path(hdfs_log_path);

            if (hdfsFileSystem.exists(hdfs)) {

                hdfsFileSystem.copyToLocalFile(false, hdfs, local);
                result = "File " + hdfs.toString() + " copied to local machine on location: " + local_log_path;

            } else {
                result = "File " + hdfs.toString() + " does not exist on HDFS";
                hdfs_log_path = hdfs_log_path + ".inprogress";
                hdfs = new Path(hdfs_log_path);
                if (hdfsFileSystem.exists(hdfs)) {
                    hdfsFileSystem.copyToLocalFile(false, hdfs, local);
                    result = "File " + hdfs.toString() + " copied to local machine on location: " + local_log_path;
                } else {
                    result = "File " + hdfs.toString() + " does not exist on HDFS";
                }
            }
            System.out.println(result);

        } catch (IOException ex) {
            System.out.println("Excepcion " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if (args.length < 5) {
            System.err.println("Please provide the input file full path as argument");
            System.exit(0);
        }
        String hdfs_host = args[0];
        String hdfs_port = args[1];
        String input_path = args[2];
        String output_path = args[3];
        final String logs_folder = args[4];

        SparkConf conf = new SparkConf().setAppName("sort");
        JavaSparkContext context = new JavaSparkContext(conf);
        Configuration hadoopConf = context.hadoopConfiguration();
        hadoopConf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
        hadoopConf.setStrings("fs.default.name", "hdfs://" + hdfs_host + ":" + hdfs_port + "/");

        final SparkContext sc = JavaSparkContext.toSparkContext(context);

        sc.addSparkListener(new SparkListener() {
            public void onApplicationStart(SparkListenerApplicationStart appStart) {
            }

            public void onApplicationEnd(SparkListenerApplicationEnd appEnd) {

                JavaSparkContext context_tmp = JavaSparkContext.fromSparkContext(sc);
                descargar_log(context_tmp, logs_folder);
            }
        });

        String filename = "hdfs://" + hdfs_host + ":" + hdfs_port + input_path;

        JavaRDD<String> file = context.textFile(filename);

        JavaRDD<String> words = file.flatMap(WORDS_EXTRACTOR);
        JavaPairRDD<String, String> pairs = words.mapToPair(WORDS_MAPPER);
        JavaPairRDD<String, String> reducer = pairs.reduceByKey(WORDS_REDUCER);
        JavaRDD<String> sort = reducer.sortByKey().keys();

        sort.saveAsTextFile("hdfs://" + hdfs_host + ":" + hdfs_port + output_path);

    }
}
