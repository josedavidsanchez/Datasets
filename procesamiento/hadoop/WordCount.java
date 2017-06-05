import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), " ");

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }

            /*CONVER TO UPPER CASE
            String tmp = "";
            while (itr.hasMoreTokens()) {
                tmp = itr.nextToken();
                word.set(tmp.toUpperCase());
                context.write(word, one);
            }
            */
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static String get_logfile_path(FileSystem hdfsFileSystem, String appid, String usuario) {

        try {
            //Date fechaActual = new Date();
            //DateFormat formatoFecha = new SimpleDateFormat("yyyy/MM/dd");
            String hdfs_log = "/tmp/hadoop-yarn/staging/history/done_intermediate/"
                    + usuario + "/";
            //formatoFecha.format(fechaActual) + "/000000";
            FileStatus[] fileStatus = hdfsFileSystem.listStatus(
                    new Path(hdfs_log));

            for (FileStatus file : fileStatus) {
                if (file.getPath().getName().contains(appid)
                        && file.getPath().getName().contains(".jhist")) {
                    return hdfs_log + file.getPath().getName();
                }
            }
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }

        return null;
    }

    public static void descargar_log(Job job, Configuration conf, String log_path) {

        try {
            FileSystem hdfsFileSystem = FileSystem.get(conf);
            String result = "";

            String local_log_path = log_path + "/" + job.getJobName() + "_hadoop.log";
            String hdfs_log_path = get_logfile_path(hdfsFileSystem,
                    job.getStatus().getJobID().toString(), job.getStatus().getUsername());

            Path local = new Path(local_log_path);
            Path hdfs = new Path(hdfs_log_path);

            String fileName = hdfs.getName();

            if (hdfsFileSystem.exists(hdfs)) {

                hdfsFileSystem.copyToLocalFile(false, hdfs, local);
                result = "File " + fileName + " copied to local machine on location: " + local_log_path;

            } else {
                result = "File " + fileName + " does not exist on HDFS on location: " + local_log_path;
            }
            System.out.println(result);
        } catch (IOException | InterruptedException ex) {
            System.out.println(ex.getMessage());
        }

    }

    public static void main(String[] args) throws Exception {

        Path input_path = new Path(args[0]);
        Path output_path = new Path(args[1]);
        String log_path = args[2];

        Configuration conf = new Configuration();
        conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
        Job job = Job.getInstance(conf, "wordcount");

        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, input_path);
        FileOutputFormat.setOutputPath(job, output_path);

        boolean completion = job.waitForCompletion(true);

        if (job.isSuccessful()) {
            descargar_log(job, conf, log_path);
        }

        System.exit(completion ? 0 : 1);

    }
}
