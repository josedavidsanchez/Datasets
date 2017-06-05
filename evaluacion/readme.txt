Esta carpeta contiene los archivos necesarios para calcular las medidas de rendimiento y escalabilidad de un programa mapreduce ejecutado en Hadoop y Spark a partir de logs generados por los frameworks
Estas medidas son runtime, used memory, speedup, efficiency e isoefficiency.

Ejecucion
python evaluator.py arg1

Para ejecutar el programa evaluator se debe almacenar los logs de los programas secuencial, hadoop y spark en un mismo folder.
1) Reciben 1 argumento como parametro para ejecutar
arg1: Ubicacion de los logs de los programas secuencial, hadoop y spark
