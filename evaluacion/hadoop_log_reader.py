import time
import ast
import os


def read_log(file_name):
    lines = []

    log_file = open(file_name, 'r')

    for line in log_file.readlines():
        line = line.replace('false', 'False')
        line = line.replace('true', 'True')
        line = line.replace('null', '"null"')
        line = line.replace('""null""', '"null"')

        if line.startswith("{"):
            lines.append(ast.literal_eval(line))

        """
        elif line.startswith("#") or line.startswith("\n"):
            print ""
        else:
            print "Line " + line
        """

    log_file.close()

    return lines

def calculate_metrics_hadoop(hadoop_logs, secuencial_time):

    #Todas las unidades son en byes y segundos
    app_name = ''
    start_time = 0.0
    finish_time = 0.0
    total_task = 0.0
    virtual_mem = 0.0
    physical_mem = 0.0
    input_size = 0.0
    output_size = 0.0

    total_time = 0.0
    throughput = 0.0

    label = 'org.apache.hadoop.mapreduce.jobhistory'
    for log in hadoop_logs:
        if log['type'] == 'JOB_SUBMITTED':
            framework = label + '.JobSubmitted'

            # get start time
            start_time = log['event'][framework]['submitTime']
            app_name = log['event'][framework]['jobName']

        elif log['type'] == 'JOB_FINISHED':
            framework = label + '.JobFinished'

            # get finish time
            finish_time = log['event'][framework]['finishTime']

            # get total tasks
            total_task = log['event'][framework]['finishedMaps'] + \
                log['event'][framework]['finishedReduces']

            groups = log['event'][framework]['totalCounters']['groups']

            for group in groups:

                # get input size
                if group['displayName'] == 'File Input Format Counters ':
                    input_size = float(group['counts'][0]['value'])

                # get output size
                elif group['displayName'] == 'File Output Format Counters ':
                    output_size = float(group['counts'][0]['value'])

        elif log['type'] == 'TASK_FINISHED':
            framework = label + '.TaskFinished'

            groups = log['event'][framework]['counters']['groups']

            for group in groups:

                # get total memory
                if group['displayName'] == 'Map-Reduce Framework':
                    for count in group['counts']:
                        if count['name'] == 'PHYSICAL_MEMORY_BYTES':
                            physical_mem = float(max(physical_mem, count['value']))
                        elif count['name'] == 'VIRTUAL_MEMORY_BYTES':
                            virtual_mem = float(max(virtual_mem, count['value']))

    total_time = float((finish_time-start_time)/1000)  # segundos
    throughput = float(total_task/total_time)

    speedup = float(secuencial_time/total_time)

    processor_total = 8  # Numero de procesadores para ejecutar la aplicacion
    eficiencia = float(speedup/processor_total)

    t = processor_total*(total_time-secuencial_time)  # funcion overhead T0(W,p) del sistema
    k = eficiencia/(1-eficiencia)  # K
    isoeficiencia = k*t

    dps = input_size/total_time  # data gb per second

    metrics = {'app_name': app_name,
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
               'phy_mem_gb': round(physical_mem/1024/1024/1024,4),
               'virtual_mem_gb': round(virtual_mem/1024/1024/1024,4),
               'Memoria total_gb': round((physical_mem+virtual_mem)/1024/1024/1024,4),
               'input_size_gb': round(input_size/1024/1024/1024,4),
               'output_size_gb': round(output_size/1024/1024/1024,4),
               'throughput': round(throughput,4),
               'speedup': round(speedup,4),
               'eficiencia': round(eficiencia,4),
               'isoeficiencia': round(isoeficiencia,4),
               'dps_gb_seg': round(dps/1024/1024/1024,4)
               }
    """
    print "start time " + str(start_time) + " fecha " + time.strftime('%d %B %Y %H:%M:%S', time.gmtime(start_time/1000))
    print "finish_time " + str(finish_time) + " fecha " + time.strftime('%d %B %Y %H:%M:%S', time.gmtime(finish_time/1000))
    print "time total " + str(total_time)
    print "total_task " + str(total_task)
    print "phy_mem " + str(physical_mem)
    print "virtual_mem " + str(virtual_mem)
    print "total_mem " + str((physical_mem+virtual_mem))
    print "input_size " + str(input_size)
    print "output_size " + str(output_size)
    """
    return metrics

#log_file = log de la app hadoop
#secuencial_time = Tiempo de ejecucion de programa secuencial segundos
def get_metrics(log_file, secuencial_time):

    log_lines = read_log(log_file)
    metrics = calculate_metrics_hadoop(log_lines, secuencial_time)

    return metrics

"""
def write_file(metrics_hadoop, path_dir, delete):
    filename = path_dir + '../metricas/hadoop-' + metrics_hadoop['app_name'] + '-' +\
               time.strftime("%d%m%Y") + '.csv'

    if delete and os.path.isfile(filename):
        os.remove(filename)

    file = open(filename, "a")
    file.write(metrics_hadoop['app_name'] + "\n")
    for key in metrics_hadoop.keys():
        if key in ['phy_mem','virtual_mem','total_mem','input_size',
                   'output_size']:
            file.write(key + ',' + str(metrics_hadoop[key]) + ',' +
                       str(metrics_hadoop[key]/1024) + 'Kb,' +
                       str(float(metrics_hadoop[key]/1024/1024)) + 'Mb,' + "\n")
        else:
            file.write(key + ',' + str(metrics_hadoop[key]) + "\n")
    file.write('\n')
    file.close()
"""


"""
def controller(path_dir):
    lstDir = os.walk(path_dir)

    delete = False
    current_app = 'app'
    for path, dirs, files in lstDir:
        print files
        files.sort()
        print files

        for file in files:

            logs_hadoop = read_log(path_dir + '/' + file)
            metrics_hadoop = calculate_metrics_hadoop(logs_hadoop)

            if current_app != metrics_hadoop['app_name']:
                delete = True
                current_app = metrics_hadoop['app_name']
            else:
                delete = False

            write_file(metrics_hadoop, path_dir, delete)


if __name__ == '__main__':
    controller('/home/usuario/Documentos/BigData/leerlogs/logs')
"""
