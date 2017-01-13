from faker import Faker
from datetime import datetime,timedelta

gen = Faker('en_GB')

start = datetime(2016,1,1,12,0,0)
events = 100000
outputFileName = 'customers.psv'

outputFile = open(outputFileName, 'w')

watchlistFileName = 'watchlist.psv'

watchlistFile = open(watchlistFileName,'w')

watchlist_range=[]

for i in range(1,100):
  w = gen.random_int(min=1, max=events)
  watchlist_range.append(w)

for i in range(1, events):

  interval = gen.random_int(min=60*5,max=60*60)

  tstamp = start + timedelta(0,interval)

  fname = gen.first_name()

  sname = gen.last_name()

  dob = str(gen.date_time_between(datetime(1900,1,1),datetime(2002,12,31)).date())
 
  nino = gen.random_int(max=99999999)
  
  if i in watchlist_range:
    line = str(nino) + "\n" 
    watchlistFile.write(line)

  housingCosts = gen.random_int(min=250, max=1000)

  childCareCosts = gen.random_int(min=100, max=800)

  address = str(gen.street_address()).replace("\n", ' ')

  postcode = gen.postcode()

  line = tstamp.strftime('%Y-%m-%dT%H:%M:%S.001Z') + "|" + \
         fname + "|" + \
         sname + "|" + \
         dob + "|" + \
         str(nino) + "|" + \
         str(housingCosts) + "|" + \
         str(childCareCosts) + "|" + \
         str(address) + "|" + \
         postcode + "\n"

  start = tstamp

  outputFile.write(line)

outputFile.close
watchlistFile.close
print str(events) + " events generated and saved in file: " + outputFileName
