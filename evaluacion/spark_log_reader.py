import time
import ast
import os


def read_log(file_name="log.txt"):
    lines = []

    log_file = open(file_name, 'r')

    for line in log_file.readlines():

        line = line.replace('false', 'False')
        line = line.replace('true', 'True')

        if line.startswith("{"):
            lines.append(ast.literal_eval(line))

    log_file.close()
    return lines


def show_list(my_list):
    print "dict"
    for item in my_list:
        print item['Event']


def calculate_metrics_spark(spark_logs, secuencial_time):
    app_name = ''
    app_id = ''
    start_time = 0.0
    finish_time = 0.0
    total_task = 0.0
    physical_mem = []
    virtual_mem = 0.0
    input_size = 0.0
    output_size = 0.0

    for log in spark_logs:
        if log['Event'] == 'SparkListenerApplicationStart':
            app_name = log['App Name']
            app_id = log['App ID']
        if log['Event'] == 'SparkListenerApplicationStart':

            # get start time
            start_time = log['Timestamp']

        elif log['Event'] == 'SparkListenerApplicationEnd':

            # get finish time
            finish_time = log['Timestamp']

        elif log['Event'] == 'SparkListenerStageCompleted':

            # get total tasks
            total_task += log['Stage Info']['Number of Tasks']

        elif log['Event'] == 'SparkListenerTaskEnd':

            # get total memory
            accumulables = log['Task Info']['Accumulables']
            for accumulable in accumulables:
                if 'Name' in accumulable.keys() and \
                                accumulable['Name'] == 'internal.metrics.peakExecutionMemory':
                    physical_mem.append(float(accumulable['Update']))

            if log['Task Type'] == 'ShuffleMapTask':
              input_size += float(log['Task Metrics']['Input Metrics']['Bytes Read'])

            """
            if 'Input Metrics' in log['Task Metrics'].keys() and \
                            'Hadoop' == log['Task Metrics']['Input Metrics'][
                            'Data Read Method']:
                # get input size
                input_size += float(log['Task Metrics']['Input Metrics']['Bytes Read'])
            """

    total_time = float((finish_time - start_time) / 1000)  # segundos
    throughput = float(total_task / total_time)

    speedup = float(secuencial_time / total_time)

    processor_total = 8  # Numero de procesadores para ejecutar la aplicacion
    eficiencia = float(speedup / processor_total)

    t = processor_total * (total_time - secuencial_time)  # funcion overhead T(W,p) del sistema
    k = eficiencia / (1 - eficiencia)  # K
    isoeficiencia = k * t

    dps = input_size/total_time  # data gb per second

    metrics = {'app_name': app_name,
               'app_id': app_id,
               'start_time': start_time,
               'start_time_format': time.strftime('%d %B %Y %H:%M:%S',
                                                  time.gmtime(
                                                      start_time / 1000)),
               'finish_time': finish_time,
               'finish_time_format': time.strftime('%d %B %Y %H:%M:%S',
                                                   time.gmtime(
                                                       finish_time / 1000)),
               'tiempo de respuesta_seg': total_time,
               'total_task': total_task,
               'phy_mem_gb': round(max(physical_mem)/1024/1024/1024,4),
               'virtual_mem_gb': round(virtual_mem/1024/1024/1024,4),
               'Memoria total_gb': round( (max(physical_mem) + virtual_mem)/1024/1024/1024,4),
               'input_size_gb': round(input_size/1024/1024/1024,4),
               'output_size_gb': round(output_size/1024/1024/1024,4),
               'throughput': round(throughput,4),
               'speedup': round(speedup,4),
               'eficiencia': round(eficiencia,4),
               'isoeficiencia': round(isoeficiencia,4),
               'dps_gb_seg': round(dps/1024/1024/1024,4)
               }

    """
    print "time total " + str((finish_time - start_time) / 1000)
    print "total_task " + str(total_task)
    print "phy_mem " + str(physical_mem)
    print "virtual_mem " + str(virtual_mem)
    print "total_mem " + str((physical_mem + virtual_mem))
    print "input_size " + str(input_size)
    print "output_size " + str(output_size)

    """
    return metrics


def get_metrics(log_file, secuencial_time):

    log_lines = read_log(log_file)
    metrics = calculate_metrics_spark(log_lines, secuencial_time)

    return metrics

"""
def write_file(metrics_spark, path_dir, delete):
    filename = path_dir + '../metricas/spark-' + metrics_spark['app_name'] + '-' + \
               time.strftime("%d%m%Y") + '.csv'

    if delete and os.path.isfile(filename):
        os.remove(filename)

    file = open(filename, 'a')
    file.write(metrics_spark['app_name'] + '\n')
    for key in metrics_spark.keys():

        if key in ['phy_mem', 'virtual_mem', 'Memoria total', 'input_size',
                   'output_size']:
            file.write(key + ',' + str(metrics_spark[key]) + ',' +
                       str(metrics_spark[key] / 1024) + 'Kb,' +
                       str(float(
                           metrics_spark[key] / 1024 / 1024)) + 'Mb,' + '\n')
        else:
            file.write(key + ',' + str(metrics_spark[key]) + '\n')
    file.write('\n')
    file.close()


def controller(path_dir):
    lst_dir = os.walk(path_dir)

    current_app = 'app'
    for path, dirs, files in lst_dir:
        print files
        files.sort()
        print files

        for file in files:

            logs_spark = read_log(path_dir + '/' + file)
            # show_list(logs_spark)
            metrics_spark = get_metrics_spark(logs_spark)

            if current_app != metrics_spark['app_name']:
                delete = True
                current_app = metrics_spark['app_name']
            else:
                delete = False

            write_file(metrics_spark, path_dir, delete)


if __name__ == '__main__':
    controller('/home/usuario/Documentos/BigData/Spark/logs/')
"""