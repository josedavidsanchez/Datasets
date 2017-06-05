package uvrs;

//import util.List, util.Properties
import java.util.*;

//import FileInputStream, IOException, InputStream
import java.io.*;

//import JavaSparkContext, JavaRDD to setup Spark
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkException;

//import library for ElasticSearch
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

//import library for logs
import org.apache.log4j.Logger;


public class Indexer {
    
    //Instance logger for the class Indexer
    final static Logger logger = Logger.getLogger(Indexer.class);    

    //This method get the values to configure app
    private Properties getProperties() {

        Properties properties = new Properties();
        InputStream input = null;
        
        try {

            input = new FileInputStream("/home/hduser/spark/spark-indexer/conf.properties");

            //Load properties file
            properties.load(input);

            //Get properties
            logger.info("Configuration charged");
            logger.info("LOG_FILE" + properties.getProperty("LOG_FILE"));
            logger.info("ELASTICSEARCH_HOST" + properties.getProperty("ELASTICSEARCH_HOST"));
            logger.info("ELASTICSEARCH_PORT" + properties.getProperty("ELASTICSEARCH_PORT"));
            logger.info("INDEX_NAME" + properties.getProperty("INDEX_NAME"));
            logger.info("INDEX_TYPE" + properties.getProperty("INDEX_TYPE"));
            logger.info("FOLDER_NAME" + properties.getProperty("FOLDER_NAME"));

        } catch (IOException exceptionFile) {
            logger.error("Error en archivo de configuracion de la app", exceptionFile);
            exceptionFile.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException exceptionClose) {
                    logger.error("No puede cerrar el archivo de conf", exceptionClose);
                    exceptionClose.printStackTrace();
                }
            }
            return properties;
        }
    }

    //This method index documents in ElasticSearch
    public void index_documents() {

        logger.info("Accessing the configuration file");
        Properties properties = this.getProperties();
        
        //Configure Spark Context
        logger.info("Setup Spark context");
        SparkConf conf = new SparkConf().setAppName("Indexing Application");
        conf.set("es.index.auto.create", "true");
        conf.set("es.nodes",
                properties.getProperty("ELASTICSEARCH_HOST","localhost"));
        conf.set("es.es.port",
                properties.getProperty("ELASTICSEARCH_PORT","9200")); 
        JavaSparkContext sc = new JavaSparkContext(conf);
        Configuration hadoopConf = sc.hadoopConfiguration();
        hadoopConf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
        
        logger.info("Spark Parameters configured");
        
        try{
            //Get files from folder
            logger.info("Accessing the repository");
            String folder_name = properties.getProperty("FOLDER_NAME");
            JavaRDD<String> files = sc.textFile(folder_name);
            logger.info("ARCHIVOS TOTALES PARA INDEXAR" + files.count());
            List<String> docs = files.collect();
            
            //Index documents
            logger.info("Prepare to index documents");
            String index = properties.getProperty("INDEX_NAME") + "/"
                    + properties.getProperty("INDEX_TYPE");
            JavaRDD<String> javaRDD = sc.parallelize(docs);
            JavaEsSpark.saveJsonToEs(javaRDD, index);
            logger.info("Documents indexed");
            logger.info("Application Finish successfully");
        }
        catch(RuntimeException sparkException){
            logger.error("Falla en procesamiento de documentos en RDD Spark",sparkException);
        }
        catch(Exception e){
            logger.error("error accediendo al byd",e);
            logger.error("err" + e.getClass().getName() + " "+e.getCause());
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        
        Indexer indexer = new Indexer();
        indexer.index_documents();

    }

}
