Esta carpeta contiene los archivos necesarios para correr el programa sort y wordcount de manera secuencial.

En la carpeta input existe un archivo por cada tamaño de datasets.

Los dos programas escritos en python tienen la misma estructura.

1) Reciben dos argumentos para correr.
arg1: archivo que contiene el path de los datasets que debe procesar el programa. Cada linea un dataset
arg2: Folder donde se guarda el resultados y las metricas del proceso

2) Lee cada dataset de manera individual para conformar el datasets completo. y luego hace procesamiento correspondiente, ya sea sort o wordcount. Por ultimo, calcula las metricas del procesamiento.

3) Crea 3 archivos
wordcount.txt: Archivo que contiene los resultados. Palabras sort o wordcount.
wordcount.log: Archivo formato json que contiene las metricas del programa.
wordcount.trace: Archivo que registra el loggin del programa.

Ejemplo para correr los programas

python mergesort.py /bigdata/secuencial/input/Kb100 /bigdata/secuencial/output/Kb100/
python wordcount.py /bigdata/secuencial/input/Kb100 /bigdata/secuencial/output/Kb100/
