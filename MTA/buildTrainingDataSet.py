## This program is used to clean out the data from the csv that you collected.
## It aims at removing duplicate entries and extracting any further insights 
## that the author(s) of the code may see fit

## Usage (for file as is currently): python buildTrainingDataSet.py <filename of file from part 1>
  
import sys

# Pandas is a python library used for data analysis
import pandas
from pandas import read_csv
from pytz import timezone
import datetime, time

'''
today = time.strftime("%Y-%m-%d")
t_midnight = "%s 00:00:00" %(today)
#print t_midnight
t_midnight = datetime.datetime.strptime(t_midnight, "%Y-%m-%d %H:%M:%S").timetuple()
t_midnight = time.mktime(t_midnight)
'''
TIMEZONE = timezone('America/New_York')
last_pointer = 0#record the last accessed express row
def main(fileHandle):
	last_pointer = 0
	# This creates a dataframe
	rawData = read_csv(fileHandle)

	# Remove duplicate entries based on tripId, retain the one with maximum timestamp
	data  =rawData.groupby('tripId').apply(lambda x: x.ix[x.timestamp.idxmax()])

	#convert 1 -> L; 2,3 -> E
	data['route'] = data['route'].map({1: 'L', 2: 'E', 3:'E'})
	#convert timestamp to mins after midnight
	data['time_96'] = data['time_96'].apply(lambda x: toMins(x))
	data['time_42'] = data['time_42'].apply(lambda x: toMins(x))

	#sort dataframe by time_96 (train departure time)
	data = data.sort_values(by='time_96',kind = "heapsort")
	data['delta'] = data['time_42']-data['time_96']

	#delete rows with delta<=0
	data = data[data.delta>0]
	#print data
	# Seperate all the local trains and form a new data frame

	localTrains = data[data.route == 'L']
	#print localTrains
	# Express trains
	expressTrains = data[data.route == 'E']
	flags = []
	for _ in localTrains.index:
		dep_time = localTrains.ix[_].time_96
		arr_42_local = localTrains.ix[_].time_42
		#print last_pointer
		for idx,e in enumerate(expressTrains.index[last_pointer:]):
			flag = None
			if expressTrains.ix[e].time_96>=dep_time:
				last_pointer = idx
				if expressTrains.ix[e].time_42>=arr_42_local:
					#stay
					flag = 0
				else:
					#switch
					flag = 1
				flags.append(flag)
				break
	localTrains['output']=flags
	del localTrains['delta'], localTrains['origin_timestamp'],localTrains['timestamp']
	#print localTrains
		
					#localTrains['otuput'] = 0
	# 1. Find the time difference (to reach 96th) between all combinations of local trains and express
	# 2. Consider only positive delta
	# 3. Make the final table

	# Create a new data frame for final table
	finalData = pandas.DataFrame(localTrains)
	#print finalData
	finalData.to_csv(fileHandle[:-4]+"_final.csv",index=False)
	del localTrains['output']
	finalData_batch = pandas.DataFrame(localTrains)


	############## INSERT YOUR CODE HERE ###############
	
	

	finalData_batch.to_csv(fileHandle[0:-4]+"_batch.csv",index=False)
	
#reference is based on the day of timestamp
def toMins(timestamp):
	reference = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
	reference = "%s 00:00:00" %(reference)
	reference = datetime.datetime.strptime(reference, "%Y-%m-%d %H:%M:%S").timetuple()
	reference = time.mktime(reference)

	res = int(round((timestamp-reference)/60))
	return res

def merge(a,b,e,c,d,f):
	ad = read_csv(a)
	bd = read_csv(b)
	ed = read_csv(e)
	res = [ad,bd,ed]
	print pandas.concat(res)
	finalData = pandas.DataFrame(pandas.concat(res))
	finalData.to_csv("final.csv",index=False)
	cd = read_csv(c)
	dd = read_csv(d)
	fd = read_csv(f)
	res = [cd,dd,fd]
	batch = pandas.DataFrame(pandas.concat(res))
	batch.to_csv("final_batch.csv",index=False)


if __name__ == "__main__":	
	fileHandle1 = "data_new_3.19.csv"
	fileHandle2 = "data_new_3.20.csv"
	fileHandle3 = "data_new_3.21.csv"
	main(fileHandle1)
	main(fileHandle2)
	main(fileHandle3)
	merge("data_new_3.19_final.csv","data_new_3.20_final.csv","data_new_3.21_final.csv","data_new_3.19_batch.csv","data_new_3.20_batch.csv","data_new_3.21_batch.csv")
