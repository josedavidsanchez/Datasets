import os, sys
from time import time
import logging

logger = logging.getLogger('wordcount')
hdlr = logging.FileHandler('wordcount.trace')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.INFO)

def print_to_file(counts, folder_output):

	ouput = open(folder_output + '/wordcount.txt','w')

	for word in counts:
		ouput.write(word + ' ' + str(counts[word]) + '\n')

	logger.info('Ver resultado en ' + folder_output + '/wordcount.txt')


def get_listinput(conf_path):

	input_folders = []
	with open(conf_path) as conf_file:
		for line in conf_file:
			line = line [:-1]
			input_folders.append(line);
			
	return input_folders


def get_words(input_folders, output_folder):

	list_of_words = []
	words_counted = dict()
	
	for input in input_folders:
		logger.info('Contando ocurrencias de cada palabra en ' + input)
		
		for root, dirs, files in os.walk(input):
			
			for file in files:
				with open(os.path.join(root,file)) as f:
					
					for line in f:
						for word in line.split():
							word = word.upper()
							if word in words_counted:
								words_counted[word] += 1
							else:
								words_counted[word] = 1
		logger.info('Archivos leidos desde ' + input + ' palabras encontradas '+str(len(words_counted)))
	
	print_to_file(words_counted, output_folder)


def wordcount():
	try:
		input_folders = get_listinput(sys.argv[1])
		output_folder = sys.argv[2]

		if not os.path.exists(output_folder):
			os.makedirs(output_folder)

		logger.info('Iniciando aplicacion')
		logger.info('Lista de entrada ' + str(input_folders).strip('[]'))
		start_time = time()
		get_words(input_folders, output_folder)
		end_time = time()
		logger.info('Termino aplicacion')

		elapsed_time = end_time - start_time
		logger.info('Tiempo de ejecucion ' + str(elapsed_time))

		output = dict()
		output["app_name"] = "wordcount"
		output["start_app"] = start_time
		output["end_app"] = end_time
		output["input"] = input_folders
		output["elapsed_time_seg"] = elapsed_time

		logfile = open(output_folder + '/wordcount.log','w')
		logfile.write(str(output))
		logger.info('Encontrar logs en ' + output_folder + '/wordcount.log')

	except IndexError:
		logger.error('Por favor indique la ubicacion de los datos entrada y salida')
    
if __name__ == '__main__':
	wordcount()
	