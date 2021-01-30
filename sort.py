import os
import sys
import math
from operator import itemgetter

metadata = {}   # {column name : column size,index}
isThreading = False  #check if the program has to run using threads 
total_columns = 0    #total no of columns in in input file
FACTOR = 1048576  #value of 2^20 - to convert MB to bytes 
#here record implies tuples



def buildMinHeap(array,n) :
	for i in range(n/2-1,0) :
		minHeapify(array,n,i)



def minHeapify(array,n,i) :
	largest = i
	left = 2*i 
	rigt = 2*i + 1

	if(array[left] > array[largest]) :
		largest = left
	if(array[right] > array[largest]) :
		largest = right
	if(largest != i) :
		array[largest],array[i] = array[i],array[largest]
		minHeapify(array,n,largest) 
	




#getting metadata 
def getMetaData() :
	try :
		with open('Metadata.txt','r') as meta_file :
			data = meta_file.readlines()
			record_size = 0
			index = 0
			for i in data :
				parts = i.strip().split(',')
				metadata[parts[0]] = [parts[1],index]
				record_size += int(parts[1])	
				index += 1
			total_columns = len(metadata)

	except (OSError,IOError) as e:
		print(p)
		sys.exit(0)
	return record_size



def phaseTwo(output_file,ram_size,num_of_subfiles,cols_to_sort_indices) :

	subfiles = []
	for i in range(0,num_of_subfiles) :
		file_name = "subfile" + i + ".txt"
		subfile = open(file_name,"r")
		subfiles.append(subfile)


	#create the outfile file
	with open(output_file,'w') as outfile :
		for i in range(0,num_of_subfiles) :
			file_name = "subfile" + i + ".txt"
			with open(file_name,'r') as subfile :
				data = subfile.readlines()










def phaseOne(ram_size,input_file,output_file,cols_to_sort,sorting_order):

	#get meta data 
	record_size = getMetaData()
	print("###running phase 1")
	total_records = 0
	cols_to_sort_indices = []
	#read input file to count no of records in the file
	try :
		with open(input_file,'r') as inp:
			data = inp.readlines()
			for i in data :
				total_records += 1
		print(total_records)
	except (OSError,IOError) as e:
		print(e)
		sys.exit(0)

	#get the column indices which need to be sorted :
	for i in cols_to_sort: 
		cols_to_sort_indices.append(metadata[i][1])
	
	#calculate number of records in each file 
	ram_size = ram_size * FACTOR 
	subfile_record = ram_size//(record_size)

	#calculate no of subfiles 
	num_of_subfiles = math.ceil(total_records/subfile_record)
	print("Number of subfiles :" , num_of_subfiles)


	###SPLITTING THE MAIN FILE AND SORTING EACH SUBFILE
	with open(input_file) as inp :
		data = inp.readlines()
		temp = 0	   
		subfile = 1    #stores the current subfile no. that is being sorted
		for i in range(0,num_of_subfiles) :
			sublist = []   #stores the records for each file before getting written into subfile
			#splitting the main file
			for j in range(temp, temp + subfile_record) :
				#print(j)
				if(j == len(data)) :
					break
				data_cols = data[j].strip().split('  ')
				sublist.append(data_cols)
			temp += subfile_record

			#sorting the data
			print(f"###sorting sublist{i+1}")
			if(sorting_order == 'asc') :
				sublist = sorted(sublist,key=itemgetter(*cols_to_sort_indices))
			else :
				sublist = sorted(sublist,key=itemgetter(*cols_to_sort_indices), reverse=True)
			file_name = 'subfile' + str(subfile) + '.txt'

			#writing the sublist to subfile
			with open(file_name,'w') as f:
				for i in sublist:
					for j in i :
						f.write(str(j))
						f.write('  ')
					f.write('\n')
			subfile += 1


	#MERGE THESE SUBFILES INTO ONE FILE :
	phaseTwo(output_file,ram_size,num_of_subfiles,cols_to_sort_indices)





#program starts here 
if __name__ == "__main__" :

	print("###start execution")

	#store the input - 
	input_file = sys.argv[1]
	output_file = sys.argv[2]
	ram_size = int(sys.argv[3])

	#WITHOUT THREADING
	if(sys.argv[4] == 'asc'  or sys.argv[3] == 'desc') :
		order = sys.argv[5]
		temp = 5

	#WITH THREADING
	else :
		isThreading = True
		no_of_threads = sys.argv[3]
		order = sys.argv[4]
		temp = 6
	cols_to_sort = []
	for i in range(temp,len(sys.argv)) :
		cols_to_sort.append(sys.argv[i])

	#Execute phase 1 : 
	phaseOne(ram_size,input_file,output_file,cols_to_sort,order)













