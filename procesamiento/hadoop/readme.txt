Esta carpeta contiene los archivos necesarios para correr el programa sort y wordcount en hadoop.

INICIAR LOGS SERVER
${HADOOP_HOME}/sbin/mr-jobhistory-daemon.sh  start historyserver
${HADOOP_HOME}/sbin/mr-jobhistory-daemon.sh  stop  historyserver
Web app: master:50072


Cada carpeta sort y wordcount contienen el codigo fuente (archivo .java) y el archivos compilados (.class) de cada programa.


Los dos programas tienen la misma estructura.

1) Reciben 3 argumentos para correr.

arg1: Directorio hdfs de dataset de entrada para leer datos.
arg2: Directorio hdsf donde se guardará los resultados del procesamiento.
arg3: Directorio local donde se copiará el archivo de log de la ejecución.

2) Crea un nuevo job (aplicación hadoop) y configurar lo necesario para su ejecución correcta.
Lee los archivos de entrada y realiza procesamiento siguiendo modelo map-reduce. Por último, si la ejecución fue exitosa copia el archivo de log de la ejecución en el folder que se indica por el arg3.

3) El programa genera 2 archivos de salida
folder_resultado: Directorio en hdfs que contiene los resultados de las palabras ordenadas u contadas. El path está definido por el arg2.
<programa>_hadoop.log: Archivo log en formato json que contiene los registros de la ejecución. Se crea en el folder que se indicó en arg3.


Para compilar el proyecto de maven
cd wordcount
javac WordCount.java

Para generar archivo jar del proyecto
cd wordcount
jar cf WordCount.jar WordCount*.class

Para ejecutar el programa
hadoop jar <jar_file> <Class_name> <hdfs_input_folder> <hdfs_output_folder> <local_log_folder>

Ejemplo para correr los programas

cd sort
hadoop jar Sort.jar Sort /repository/Kb100/ /hadoop/sort/Kb100/ /bigdata/logs/Kb100/

cd wordcount
hadoop jar WordCount.jar WordCount /repository/Kb100/ /hadoop/wc/Kb100/ /bigdata/logs/Kb100/
