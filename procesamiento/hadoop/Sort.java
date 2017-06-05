
import java.io.IOException;
import java.util.StringTokenizer;

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

public class Sort {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), " ");

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                one.set(one.get() + 1);
                context.write(word, one);
            }
            
            /* CONVERT TO UPPER CASE 
            String tmp = "";
            while (itr.hasMoreTokens()) {
                tmp = itr.nextToken();
                word.set(tmp.toUpperCase());
                one.set(one.get() + 1);
                context.write(word, one);
            }
            */
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, Text> {

        private final static IntWritable counter = new IntWritable(0);

        public void reduce(Text key, Iterable<IntWritable> values,
                Context context
        ) throws IOException, InterruptedException {

            counter.set(counter.get() + 1);
            Text str = new Text("" + counter.get());
            context.write(str, key);
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

    public static void descargar_log(Job job, Configuration conf, String logs_folder) {

        try {
            FileSystem hdfsFileSystem = FileSystem.get(conf);
            String result = "";

            String local_log_path = logs_folder + "/" + job.getJobName() + "_hadoop.log";
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
        String logs_folder = args[2];

        Configuration conf = new Configuration();
        conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
        Job job = Job.getInstance(conf, "sort");

        job.setJarByClass(Sort.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, input_path);
        FileOutputFormat.setOutputPath(job, output_path);

        boolean completion = job.waitForCompletion(true);

        if (job.isSuccessful()) {
            descargar_log(job, conf, logs_folder);
        }

        System.exit(completion ? 0 : 1);
    }
}
