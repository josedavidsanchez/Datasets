import sys, os
import ast
import collections
#from xlwt import Workbook
import xlsxwriter
from hadoop_log_reader import get_metrics as get_metrics_hadoop
from spark_log_reader import get_metrics as get_metrics_spark

def get_tiempo_secuencial(log_path):

	log_file = open(log_path, 'r')
	logs_app = dict()
	logs_app = ast.literal_eval(log_file.readline())

	tiempo_secuencial =  logs_app['elapsed_time_seg']

	return tiempo_secuencial


def ordenar_by_size(d):

	d_ordenado = []
	d = collections.OrderedDict(sorted(d.items()))
	for k, v in d.items():
		v['dataset_size_gb'] = k
		d_ordenado.append(v)

	return d_ordenado


#CREAR XLSX
def crear_excel_file(metricas_hadoop_sort, metricas_spark_sort, metricas_hadoop_wordcount, metricas_spark_wordcount):



	#Crear archivo excel. Hoja 1 para metricas de sort. Hoja 2 para metricas hadoop
	book = xlsxwriter.Workbook('metricas_bigdata.xlsx')
	ws1 = book.add_worksheet('metricas_sort')
	ws2 = book.add_worksheet('metricas_wordcount')

	#Contenido hoja 1 programa sort: Dos tablas. Metricas para sort en hadoop y metricas para sort en spark

	#Tabla de metricas de sort en hadoop
	list_metricas = metricas_hadoop_sort[0].keys()

	ws1.write(0, 0, 'HADOOP')
	ws1.write(0+20, 0, 'SPARK')
	ws2.write(0, 0, 'HADOOP')
	ws2.write(0+20, 0, 'SPARK')
	print len(metricas_hadoop_sort)
	row = 2
	
	for metrica in list_metricas:
		column = 1

		if metrica != "dataset_size_gb":
			ws1.write(row, 0, metrica)
			ws1.write(row+20, 0, metrica)
			ws2.write(row, 0, metrica)
			ws2.write(row+20, 0, metrica)
		else:
			row -= 1
		
		#for item in metricas_hadoop_sort:
		for i in range(len(metricas_hadoop_sort)):

			if metrica == "dataset_size_gb":
				ws1.write(1, column, metricas_hadoop_sort[i][metrica])
				ws1.write(1+20, column, metricas_spark_sort[i][metrica])

				ws2.write(1, column, metricas_hadoop_wordcount[i][metrica])
				ws2.write(1+20, column, metricas_spark_wordcount[i][metrica])
			else:
				ws1.write(row, column, metricas_hadoop_sort[i][metrica])
				ws1.write(row+20, column, metricas_spark_sort[i][metrica])

				ws2.write(row, column, metricas_hadoop_wordcount[i][metrica])
				ws2.write(row+20, column, metricas_spark_wordcount[i][metrica])
			column+=1
		row += 1

	book.close()


def read_logs(logs_folder):

	metricas_hadoop_sort = dict()
	metricas_spark_sort = dict()
	metricas_hadoop_wordcount = dict()
	metricas_spark_wordcount = dict()

	for root, dirs, files in os.walk(logs_folder):
		for dir in dirs:

			current_folder = os.path.join(root, dir)
			print "Procesando folder " + current_folder
			
			tiempo_secuencial_sort = get_tiempo_secuencial(current_folder + '/sort.log')
			tiempo_secuencial_wordcount = get_tiempo_secuencial(current_folder + '/wordcount.log')

			metricas_hadoop_sort[dir] = get_metrics_hadoop(current_folder + '/sort_hadoop.log',tiempo_secuencial_sort)
			metricas_spark_sort[dir] = get_metrics_spark(current_folder + '/sort_spark.log',tiempo_secuencial_sort)

			metricas_hadoop_wordcount[dir] = get_metrics_hadoop(current_folder + '/wordcount_hadoop.log',tiempo_secuencial_wordcount)
			metricas_spark_wordcount[dir] = get_metrics_spark(current_folder + '/wordcount_spark.log',tiempo_secuencial_wordcount)

	metricas_hadoop_sort = ordenar_by_size(metricas_hadoop_sort)
	metricas_spark_sort = ordenar_by_size(metricas_spark_sort)
	metricas_hadoop_wordcount = ordenar_by_size(metricas_hadoop_wordcount)
	metricas_spark_wordcount = ordenar_by_size(metricas_spark_wordcount)

	crear_excel_file(metricas_hadoop_sort, metricas_spark_sort, metricas_hadoop_wordcount, metricas_spark_wordcount)
	

if __name__ == '__main__':
	read_logs(sys.argv[1])



 