import os
import sys
import math
from operator import itemgetter
import heapq

metadata = {}   # {column name : column size,index}
isThreading = False  #check if the program has to run using threads 
total_columns = 0    #total no of columns in in input file
FACTOR = 1048576  #value of 2^20 - to convert MB to bytes 
GAP = 2 		  #a gap of 2 bytes(2 spaces) to read a columns from a row in input file
#here record implies tuples



class heapNode:

	def __init__(self,cols_to_sort_indices,sorting_order,row,file_name):
		self.cols_to_sort_indices = cols_to_sort_indices
		self.sorting_order = sorting_order
		self.row = row
		self.file_name = file_name


	def __lt__(self,other) :

		#for ascending order
		if(self.sorting_order == 'asc'):
			for i in self.cols_to_sort_indices :
				if(self.row[i] == other.row[i]) :
					continue
				elif(self.row[i] < other.row[i]) :
					return True
				else:
					return False

		#for descending order
		else :
			for i in self.cols_to_sort_indices :
				if(self.row[i] == other.row[i]) :
					continue
				elif(self.row[i] < other.row[i]) :
					return False
				else :
					return True
		return False


def processOneRow(file_data) :

	#emp_file_data = file_data[index]   #take a line/row from file
	count = 0             
	temp_count = 0
	row_data = []
	#for every col in the row, keep adding the content until the size of the 
	#column is reached
	for cols in metadata :
		st = ''
		count = temp_count + metadata[cols][0]
		while(temp_count < count) :           
			st += file_data[temp_count] 
			temp_count += 1
		row_data.append(st)
		temp_count += 2 #to skip the 2 spaces present between the two columns in the row
	return row_data



#getting metadata 
def getMetaData() :
	try :
		with open('Metadata.txt','r') as meta_file :
			data = meta_file.readlines()
			record_size = 0
			index = 0
			for i in data :
				parts = i.strip().split(',')
				metadata[parts[0]] = [int(parts[1]),index]
				record_size += int(parts[1])	
				index += 1
			total_columns = len(metadata)

	except (OSError,IOError) as e: 
		print(e) 
		sys.exit(0) 
	return record_size



def phaseTwo(output_file,ram_size,num_of_subfiles,cols_to_sort_indices,sorting_order) :

	print("##Phase 2 execution begins...")

	outfile = open(output_file,'w')


	subfiles = {}  #list of file pointers
	intial_data = []
	mergelist = []	
	file_end_flag = False	


	#build intial heap with first line of every file
	for i in range(0,num_of_subfiles) :
		file_name = "subfile" + str(i+1) + ".txt"
		subfile = open(file_name,"r")
		subfiles[file_name] = subfile
		subfile_data = subfile.readline()

		#PROCESS  A ROW OF INPUT FILE : 
		row_data = processOneRow(subfile_data)
		node = heapNode(cols_to_sort_indices,sorting_order,row_data,file_name)
		mergelist.append(node)


	#node = heapq.heappop(mergelist)
	if(sorting_order == 'asc') :
		heapq.heapify(mergelist)
	else :
		heapq._heapify_max(mergelist)


	'''while(len(mergelist) > 0 ) :
		node = heapq._heappop_max(mergelist)
		print("node.row is :" , node.row)
		print("node file is :", node.file_name)'''


	print("###merging subfiles")
	while(len(mergelist) > 0) :

		#get root data from heap
		if(sorting_order == 'asc') :
			node = heapq.heappop(mergelist)
		else :
			node = heapq._heappop_max(mergelist)

		#write data to output file
		for i in node.row :
			outfile.write(str(i) + "  ")
		outfile.write("\n")

		#get the next line 
		current_file = subfiles[node.file_name]
		temp_row_data = current_file.readline()

		#if the line is not empty 
		if(temp_row_data) :
			row_data = processOneRow(temp_row_data)
			temp_node = heapNode(cols_to_sort_indices, sorting_order, row_data, node.file_name)
			mergelist.append(temp_node)
			if(sorting_order == 'asc') :
				heapq.heapify(mergelist)
			else :
				heapq._heapify_max(mergelist)


		#file has reached its end
		else :
			temp_file_name = node.file_name
			#os.remove(temp_file_name)
			del subfiles[temp_file_name]
			print("Contents of " + temp_file_name.split(".")[0] + " merged to final output")


	outfile.close()
	print("Finished execution")



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
	except (OSError,IOError) as e:
		print(e)
		sys.exit(0)

	#get the column indices which need to be sorted :
	for i in cols_to_sort: 
		cols_to_sort_indices.append(metadata[i][1])
	
	#calculate number of records in each file 
	ram_size = ram_size * FACTOR 
	print("ram size",ram_size)
	subfile_record = ram_size//(record_size+4)
	print("subfile record",subfile_record)
	print("total_record", total_records)

	#calculate no of subfiles 
	num_of_subfiles = math.ceil(total_records/subfile_record)
	print("Number of subfiles :" , num_of_subfiles)

	print("cols to sort are : ", cols_to_sort_indices)


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
				
				#PROCESS  A ROW OF INPUT FILE : 
				row_data = processOneRow(data[j])
				sublist.append(row_data)
			temp += subfile_record


			#sorting the data
			print(f"###sorting sublist{i+1}")
			if(sorting_order == 'asc') :
				sublist = sorted(sublist,key=itemgetter(*cols_to_sort_indices))
				#sublist = sorted(sublist,key=itemgetter(0))
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
	phaseTwo(output_file,ram_size,num_of_subfiles,cols_to_sort_indices,sorting_order)





#program starts here 
if __name__ == "__main__" :

	print("###start execution")

	#store the input - 
	input_file = sys.argv[1]
	output_file = sys.argv[2]
	ram_size = int(sys.argv[3])


	#calculate file size :
	input_file_size = os.stat(input_file).st_size
	print("Input file size : ", input_file_size)


	#WITHOUT THREADING
	if(sys.argv[4].lower() == 'asc'  or sys.argv[4].lower() == 'desc') :
		order = sys.argv[4].lower()
		temp = 5


	#WITH THREADING
	else :
		isThreading = True
		no_of_threads = sys.argv[3]
		order = sys.argv[4].lower()
		temp = 6

	cols_to_sort = []  #stores the names of the cols to be sorted
	for i in range(temp,len(sys.argv)) :
		cols_to_sort.append(sys.argv[i])


	#Execute phase 1 : 
	phaseOne(ram_size,input_file,output_file,cols_to_sort,order)
	print(g)












