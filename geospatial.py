def haversine(row) :
    R = 6371000
    lat1 = row.Latitude
    lat2 = row.Latitude_lag
    lon1 = row.Longitude
    lon2 = row.Longitude_lag
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_lat = math.radians(math.fabs(lat2 - lat1))
    delta_lon = math.radians(math.fabs(lon2 - lon1))
    a = math.pow(math.sin(delta_lat/2),2) + math.cos(phi1) * math.cos(phi2) * math.pow(math.sin(delta_lon/2),2)
    try:
        a >= 0 and a <= 1
    except ValueError:
        print 'Floating Point Rounding Warning: hav(d/R) > 1 - large errors will be incurred'
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = R * c * 0.000621371
    return Row(row[7],d)

def vincenty(row) :
    R = 6371000
    lat1 = row.Latitude
    lat2 = row.Latitude_lag
    lon1 = row.Longitude
    lon2 = row.Longitude_lag
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_lon = math.radians(math.fabs(lon2 - lon1))
    a = math.pow(math.cos(phi2) * math.sin(delta_lon),2) + math.pow(math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(delta_lon),2)
    b = math.sin(lat1) * math.sin(lat2) + math.cos(lat1) * math.cos(lat2) * math.cos(delta_lon)
    c = math.atan2(math.sqrt(a),b)
    d = R * c * 0.000621371
    return Row(row[7],d)

def speedFunc(row) :
    d = row.Distance
    t = (row.UnixTS_lag - row.UnixTS)
    if t == 0 :
        s = 0
    else :
        s = d/float(t)*3600
    return Row(row[7],s)