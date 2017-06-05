#http://stackoverflow.com/questions/36683756/merge-sort-a-list-of-words

import os, sys, re
from time import time
import logging

logger = logging.getLogger('sort')
hdlr = logging.FileHandler('sort.trace')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.INFO)

def merge_sort(list, key=lambda x: x):
    if len(list) == 1:
        return list
    else:
        # recursive step. Break the list into chunks of 1
        mid_index = len(list) // 2
        left  = merge_sort( list[:mid_index], key )
        right = merge_sort( list[mid_index:], key )

    left_counter, right_counter, master_counter = 0, 0, 0

    while left_counter < len(left) and right_counter < len(right):
        if left[left_counter] < right[right_counter]:
            list[master_counter] = left[left_counter]
            left_counter += 1
        else:
            list[master_counter] = right[right_counter]
            right_counter += 1

        master_counter += 1

    # Handle the remaining items in the remaining_list
    # Either left or right is done already, so only one of these two
    #    loops will execute

    while left_counter < len(left):  # left list isn't done yet
        list[master_counter] = left[left_counter]
        left_counter   += 1
        master_counter += 1

    while right_counter < len(right):  # right list isn't done yet
        list[master_counter] = right[right_counter]
        right_counter   += 1
        master_counter  += 1

    return list


def get_listinput(conf_path):

	input_folders = []
	with open(conf_path) as conf_file:
		for line in conf_file:
			line = line [:-1]
			input_folders.append(line);
			
	return input_folders


def get_words(input_folders, output_folder):

	
	read_words = dict()
	
	for input in input_folders:
		logger.info('Leyendo entrada desde ' + input)
	
		for root, dirs, files in os.walk(input):
			for file in files:
				with open(os.path.join(root,file)) as f:
					
					for line in f:
						for word in line.split():
							#word = word.upper()
							read_words[word] = 1
		logger.info('Archivos leidos desde ' + input + ' palabras encontradas '+str(len(read_words)))
	
	list_of_words = read_words.keys()
	logger.info('Ordenando ' + str(len(list_of_words)) + ' palabras')

	words_sorted = merge_sort(list_of_words, key=len)
	logger.info('Palabras ordenadas')

	ouput = open(output_folder + '/sort.txt','wa+')
	ouput.write('\n'.join(words_sorted))
	logger.info('Ver resultado en ' + output_folder + '/sort.txt')
	

def sort():
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
		output["app_name"] = "sort"
		output["start_app"] = start_time
		output["end_app"] = end_time
		output["input"] = input_folders
		output["elapsed_time_seg"] = elapsed_time

		logfile = open(output_folder + '/sort.log','w')
		logfile.write(str(output))
		logger.info('Encontrar logs en ' + output_folder + '/sort.log')

	except IndexError:
		logger.error('Por favor indique la ubicacion de los datos entrada y salida')
    
if __name__ == '__main__':
	sort()