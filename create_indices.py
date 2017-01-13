from elasticsearch import Elasticsearch

def create_index(i,m):
  if es.indices.exists(i):
    es.indices.delete(i)
  
  es.indices.create(index=i, ignore=400, body=m)


es = Elasticsearch()

metricsMapping = '''
{
"mappings":{
  "metric":{
    "properties":{
      "@timestamp":{
        "type":"date",
          "format":"strict_date_optional_time||epoch_millis"
        },
        "topCentileHousingCosts":{
          "type":"double"
        }
      }
    }
  }
}'''

customersMapping='''
{"mappings":{
  "customer":{
    "properties":{
      "address":{"type":"string"},
      "childCareCosts":{"type":"double"},
      "dob":{"type":"date","format":"strict_date_optional_time||epoch_millis"},
      "firstname":{"type":"string"},
      "housingCosts":{"type":"double"},
      "nino":{"type":"string"},
      "postcode":{"type":"string"},
      "surname":{"type":"string"},
      "tstamp":{"type":"date","format":"strict_date_optional_time||epoch_millis"}
    }
  }
}
}'''

risksMapping='''
{"mappings":{
  "topCentileHousingCosts":{
    "properties":{
      "housingCosts":{"type":"double"},
      "nino":{"type":"string"},
      "risk_category":{"type":"string"},
      "tstamp":{"type":"date","format":"strict_date_optional_time||epoch_millis"}
    }
  }
}
}'''

create_index('customers', customersMapping)
create_index('risks', risksMapping)
create_index('metrics', metricsMapping)
