import sys, stomp, gzip, StringIO, xmltodict, json, time, requests

def timediff(first, second):
	firsthours = int(float(first[0:2]))
	firstminutes = int(float(first[4:5]))

	firsttotalminutes = (firsthours*60)+firstminutes

	secondhours = int(float(second[0:2]))
	secondminutes = int(float(second[4:5]))

	secondtotalminutes = (secondhours*60)+secondminutes

	difference = secondtotalminutes-firsttotalminutes;

	return str(difference)


class MyListener(object):

        def on_error(self, headers, message):
            	print('received an error %s' % message)

        def on_message(self, headers, message):
                fp = gzip.GzipFile(fileobj = StringIO.StringIO(message))
                text = fp.readlines()
                fp.close()
                traininfo = json.dumps(xmltodict.parse(text[0],process_namespaces=True),separators=(',',':'))
		cleaninfo = traininfo.replace("http://www.thalesgroup.com/rtti/PushPort/","").replace("@","").replace("#","plat")
		cleanjson = json.loads(cleaninfo)
		parsejson = cleanjson["v12:Pport"]["v12:uR"]
		ts = cleanjson["v12:Pport"]["ts"]
		requestsource = parsejson.get("requestSource")
		updateorigin = parsejson.get("updateOrigin")
#TS MESSAGES
		try:
			if "v12:TS" in parsejson:
				ssd = parsejson["v12:TS"]["ssd"]
				uid = parsejson["v12:TS"]["uid"]
				rid = parsejson["v12:TS"]["rid"]
				location = parsejson["v12:TS"]["Forecasts/v2:Location"]
				if type(location) is not list:
					location = [location]
				for key in location:
					ptd = key.get("ptd")
					pta = key.get("pta")
					wta = key.get("wta")
					wtd = key.get("wtd")
					wtp = key.get("wtp")
					for r in key:
						if "Forecasts/v2:arr" in r:
							aat = key["Forecasts/v2:arr"].get("at")
							aet = key["Forecasts/v2:arr"].get("et")
							adelayed = key["Forecasts/v2:arr"].get("delayed")
							awet = key["Forecasts/v2:arr"].get("wet")
							asrc = key["Forecasts/v2:arr"].get("src")
							asrcInst = key["Forecasts/v2:arr"].get("srcInst")
							break
						else:
							aat = 'null'
							aet = 'null'
							adelayed = 'null'
							awet = 'null'
							asrc = 'null'
							asrcInst = 'null'
					for s in key:
						if "Forecasts/v2:dep" in s:
							dat = key["Forecasts/v2:dep"].get("at")	
							det = key["Forecasts/v2:dep"].get("et")
							ddelayed = key["Forecasts/v2:dep"].get("delayed")
							dwet = key["Forecasts/v2:dep"].get("wet")
							dsrc = key["Forecasts/v2:dep"].get("src")
							dsrcInst = key["Forecasts/v2:dep"].get("srcInst")
							break
						else:
							dat = 'null'
							det = 'null'
							ddelayed = 'null'
							dwet = 'null'
							dsrc = 'null'
							dsrcInst = 'null'
					for t in key:
						if "Forecasts/v2:plat" in t:
							platform = key["Forecasts/v2:plat"]
							if type(platform) is dict:
								platsup = platform.get("platsup")
								plattext = platform.get("plattext")
								cisPlatsup = platform.get("cisPlatsup")
								platsrc = platform.get("platsrc")
								platconf = platform.get("platconf")
							if type(platform) is str:
								platsup = platform
						else:
							plattext = 'null'
							platsup = 'null'
							cisPlatsup = 'null'
							platsrc = 'null'
							platconf = 'null'
					for u in key:
						if "Forecasts/v2:pass" in u:
							pat = key["Forecasts/v2:pass"].get("at")
							pet = key["Forecasts/v2:pass"].get("et")
							pdelayed = key["Forecasts/v2:pass"].get("delayed")
							break
						else:
							pat= 'null'
							pet = 'null'
							pdelayed = 'null'


					aet = awet if awet is not None else (aet if aet is not None else 'null')
					det = dwet if dwet is not None else (det if det is not None else 'null')
					aat = aat if aat is not None else 'null'
					dat = dat if dat is not None else 'null'
					pet = pet if pet is not None else 'null'
					pat = pat if pat is not None else 'null'
					wta = wta if wta is not None else 'null'
					wtd = wtd if wtd is not None else 'null'
					wtp = wtp if wtp is not None else 'null'

					ad = timediff(wta,aat) if aat != 'null' else (timediff(wta,aet) if aet != 'null' else '0')
					dd = timediff(wtd,dat) if dat != 'null' else (timediff(wtd,det) if det != 'null' else '0')
					pd = timediff(wtp,pat) if pat != 'null' else (timediff(wtp,pet) if pet != 'null' else '0')

					delayed = 'true' if int(float(ad)) > 1 else ('true' if int(float(dd)) != 1 else ('true' if int(float(pd)) != 1 else 'false'))

					try:

						r = requests.post('http://localhost:8182/graphs/emptygraph/edges/'+rid+key["tpl"]+'?_inV='+rid+'&_label=update&_outV='+key["tpl"]+'&aet='+aet+'&det='+det+'&aat='+aat+'&dat='+dat+'&pat='+pat+'&pet='+pet+'&ad='+ad+'&dd='+dd+'&pd='+pd+'&delayed='+delayed)

					except IOError as e:
					    print "I/O error({0}): {1}".format(e.errno, e.strerror)
						
		except:
		    print('parserror'), sys.exc_info()

#SCHEDULE MESSAGES
		try:
			if "v12:schedule" in parsejson:
				trainId = parsejson["v12:schedule"].get("trainId")
				uid = parsejson["v12:schedule"].get("uid")
				toc = parsejson["v12:schedule"].get("toc")
				ssd = parsejson["v12:schedule"].get("ssd")
				rid = parsejson["v12:schedule"].get("rid")
				traincat = parsejson["v12:schedule"].get("trainCat")

				r = requests.post('http://localhost:8182/graphs/emptygraph/vertices/'+rid + '?headcode='+trainId+'&toc='+toc+'&ssd='+ssd+'&uid='+uid+'&type=schedule')
				
				types = ["OR","PP","IP","OPIP","DT"]

				for t in types:

					if parsejson["v12:schedule"].get("Schedules/v1:"+t) is not None:			
						sch = parsejson["v12:schedule"]["Schedules/v1:"+t]
						if type(sch) is not list:
							sch = [sch]			

						for s in sch:
							tiploc = s.get("tpl")
							pta = s.get("pta") if s.get("pta") is not None else 'null'
							ptd = s.get("ptd") if s.get("ptd") is not None else 'null'
							wta = s.get("wta") if s.get("wta") is not None else 'null'
							wtd = s.get("wtd") if s.get("wtd") is not None else 'null'
							wtp = s.get("wtp") if s.get("wtp") is not None else 'null'
							act = s.get("act") if s.get("act") is not None else 'null'

							r = requests.post('http://localhost:8182/graphs/emptygraph/edges/'+tiploc+rid+'?_outV='+rid+'&_label=schedule&_inV='+tiploc+'&type='+t+'&pta='+pta+'&ptd='+ptd+'&wtd='+wtd+'&wta='+wta+'&wtp='+wtp)
		except:
			print(parsejson)

#DEACTIVATION MESSAGES
		try:
			if "v12:deactivated" in parsejson:
				rid = parsejson["v12:deactivated"].get("rid")
				r = requests.delete('http://localhost:8182/graphs/emptygraph/vertices/'+rid)

		except:
			print(parsejson)

conn = stomp.Connection([('datafeeds.nationalrail.co.uk', 61613)])

conn.set_listener('', MyListener())
conn.start()
conn.connect(username = 'd3user', passcode = 'd3password', wait=False)

conn.subscribe(destination='/queue/D3df975335-cb93-4112-982e-31309622a945', id=1, ack='auto')

while True:

	time.sleep(5)

conn.disconnect()
