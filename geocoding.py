from omgeo import Geocoder
import string
import math

def load_data(inPath, outPath):
	outFile = open(outPath, 'w')
	counter = 0
	with open(inPath,'Ur') as inFile:
		for line in inFile:
			GarageID,Zip,Street,City,State,cnt = line.split(",")
			address = Street+" "+City+" "+State+" "+"USA"
			g = Geocoder()
			result = g.geocode(address)

			outFile.write('%s\n' % (result))

def parse_data(inPath, outPath):
	outFile = open(outPath,'w')
	with open(inPath,'Ur') as inFile:
		for line in inFile:
			a,b, cand = line.split(":")
			print cand
			a = cand.replace("[<","")
			b = a.replace("EsriWGS>]}","")
			c = b.replace(" (",',')
			d = c.replace(')','')
			print d
			outFile.write('%s\n' % d)

def calc_dist(inPath1, inPath2, outPath):
	
	station = open(inPath2,"Ur")
	outFile = open(outPath,"w")
	for line_s in station:
		garage = open(inPath1,"Ur")
		#tuples = line_s.split(",")
		Zip1, Station, City1, Address, Lat1, Lon1,UniqueID,Regular, Midgrade,Premium,Diesel,RegPercentile,MidPercentile, PremPercentile, DiesPercentile = line_s.split(",")
		for line_g in garage:
			
			tuples = line_g.strip().split(",")
			#print len(tuples),tuples
			Street = tuples[0]
			City2  = tuples[1]
			State  = tuples[2] 
			Zip2 =  tuples[3]
			Lat2  = tuples[5]
			Lon2 =  tuples[4]
			print Lat1, Lon1,Lat2, Lon2
		
			lat1 = float(Lat1)
			lon1 = float(Lon1)
			lat2 = float(Lat2)
			lon2 = float(Lon2)

			lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
			dlon = lon2 - lon1
			dlat = lat2 - lat1
			#print dlon, dlat

			a = (math.sin(dlat/2))**2 + math.cos(lat1) * math.cos(lat2) * (math.sin(dlon/2))**2
			c = 2 * math.atan2( math.sqrt(a), math.sqrt(1-a) )
			r = 3963
			d = r * c 
			#print a,c,d
			outFile.write('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%f\n' 
				% (Zip1, Station, City1, Zip2,Address, Street, City2, State,Lat1,Lon1,Lat2,Lon2,d))
		garage.close()







def main():
	#load_data("garage_mostveh.csv","garage_mostveh_geo.csv")
	#parse_data("garage_mostveh_geo.csv", "garage_mostveh_parse.csv")
	calc_dist("garage_mostveh_parse.csv","gas_station_out.csv","garage_station.csv")

if __name__ == "__main__":
    main()
