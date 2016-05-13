import sys, json, time, csv, datetime
sys.path.append('../utils')
from pytz import timezone
import gtfs_realtime_pb2
import google.protobuf
import alert, tripupdate, vehicle, mtaUpdate

try:
	today = time.strftime("%Y-%m-%d")
	t_midnight = "%s 00:00:00" %(today)
	t_midnight = datetime.datetime.strptime(t_midnight, "%Y-%m-%d %H:%M:%S").timetuple()
	t_midnight = time.mktime(t_midnight)

	print t_midnight
	count = 1
	output = []
	while(count<1000):		
		mta = mtaUpdate.mtaUpdates('9677454425d764f551397579a52aa866')
		trips = mta.getTripUpdates() #get a list of tripUpdate objects
		print len(trips)
		for _ in trips:	
			if _.routeId == "1" or _.routeId == "2" or _.routeId == "3":
				try:
					tt = time.time()#?
					timestamp = int((tt-t_midnight)/60)
					tripId = _.tripId

					trip_start_time = _.tripId[0:6]
					ex_stopId = "120"+_.direction
					ts_stopId = "127"+_.direction
					route = _.routeId
					fs = json.loads(json.dumps(_.futureStops))
					#print fs.get(ex_stopId)[0]
					time_reaches_96 = fs.get(ex_stopId)[0]["arrivalTime"]
					time_reaches_ts = 0
					status = ""
				except:
					continue
				try:
					#print _.vehicleData.StopId
					if _.vehicleData.StopId == ts_stopId:
						time_reaches_ts = _vehicleData.timestamp
						status = _.vehicleData.currentStopStatus
				#	print "tripId: %s \n futureStops: %s \n vehicleData: \n currentStopNum: %s \n currentStopId: %s \n tiemstamp: %s \n currentStopStatus: %s \n\n" %(_.tripId, fs.get('127N'), _.vehicleData.currentStopNumber, _.vehicleData.currentStopId, _.vehicleData.timestamp, _.vehicleData.currentStopStatus) #_.vehicleData.currentStopNumber)
				except:
					try:
						time_reaches_ts = fs.get(ts_stopId)[0]["arrivalTime"]
						status = "from trip"
					except:
						continue
				#print timestamp, tripId, trip_start_time, status
				d = {"origin_timestamp":tt,"timestamp": timestamp, "tripId": tripId, "trip_start_time":trip_start_time, "route":route, "time_96": time_reaches_96, "time_42": time_reaches_ts}
				output.append(d)
				
		print "round %d: feed length: %d" %(count,len(output))
		count +=1
		time.sleep(30)
except:
	print "Writing into csv..."
	with open('data_new.csv', 'a') as fou:
		fieldnames = ['origin_timestamp','timestamp', 'tripId','trip_start_time', 'route', 'time_96', 'time_42']
		dw = csv.DictWriter(fou,fieldnames=fieldnames)
		dw.writeheader()
		#print output
		for _ in output:
			#print _
			dw.writerow(_)

	print "Done."
	
		#export to csv