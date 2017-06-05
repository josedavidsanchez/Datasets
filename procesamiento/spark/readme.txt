Esta carpeta contiene los archivos necesarios para correr el programa sort y wordcount en spark.


La carpeta bigdata-spark corresponde a un proyecto de Maven. El archivo bigdata-spark/pom.xml se definen las dependencias de spark necesarias para ejecutar los programas.

En este proyecto esta existen dos programas sort y wordcount escritos en java. El codigo fuente se encuentra en bigdata-spark/src/main/java/


Los dos programas tienen la misma estructura.

1) Reciben 5 argumentos para correr.
arg1: Hostname or ip de hdfs para leer y escribir los datos.
arg2: Puerto de hdfs
arg3: Directorio hdfs de dataset de entrada para leer datos.
arg4: Directorio hdsf donde se guardará los resultados del procesamiento.
arg5: Directorio local donde se copiará el archivo de log de la ejecución.

2) Crea un nuevo objeto sparkContext (aplicación spark) y configurar lo necesario para su ejecución correcta.
Lee los archivos de entrada y realiza procesamiento siguiendo modelo map-reduce. Por último, copia el archivo de log de la ejecución en el folder que se indica por el arg5.

3) El programa genera 2 archivos de salida
folder_resultado: Directorio en hdfs que contiene los resultados de las palabras ordenadas u contadas. El path está definido por el arg4.
<programa>_spark.log: Archivo log en formato json que contiene los registros de la ejecución. Se crea en el folder que se indicó en arg5.


Para compilar el proyecto de maven
cd bigdata-spark
mvn compile

Para generar archivo jar del proyecto
cd bigdata-spark
mvn package

Para ejecutar el programa
/usr/local/spark/bin/spark-submit --class Sort --master local target/bigdata-spark-1.0.jar <host_hdfs> <port_hdfs> <hdfs_input_folder> <hdfs_output_folder> <local_logs_folder>

/usr/local/spark/bin/spark-submit --class WordCount --master local target/bigdata-spark-1.0.jar <host_hdfs> <port_hdfs> <hdfs_input_folder> <hdfs_output_folder> <local_logs_folder>


Ejemplo para correr los programas

Modo Local
cd bigdata-spark
/usr/local/spark/bin/spark-submit --class Sort --master local target/bigdata-spark-1.0.jar <host_hdfs> <port:hdfs> /repository/test/Mb100 /spark/output/sort/Mb100/ /bidata/logs/Kb100/


Modo Cluster
cd bigdata-spark
/usr/local/spark/bin/spark-submit --class Word --master spark://jdavidsg1.eisc.univalle.edu.co:6066 --deploy-mode cluster target/bigdata-spark-1.0.jar pc 8020 /repository/Kb100/ /spark/wc/Kb100/ /bigData/logs/Kb100/
