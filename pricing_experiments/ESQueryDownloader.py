import csv
import zipfile
from zipfile import ZipFile, ZipInfo
from elasticsearch import Elasticsearch
from datetime import datetime
from dateutil import tz
import pandas as pd

# OUR TIMEZONE
DEFAULT_TIMEZONE = "Asia/Kolkata"

## ES CONFIG
ES_HOST_PROD_E = "54.255.139.39"
ES_HOST_PROD_I = "172.31.31.1"
ES_HOST_PREPROD_E = "54.169.42.232"
ES_HOST_PREPROD_I  = "10.0.1.186"
ES_HOST = ES_HOST_PREPROD_I
ES_INDEX = "orders_alias"
ES_DOC_TYPE = "master"


ES_QUERY = { 
    "query":{  
      "bool":{  
         "must":[{  
                "query_string":{  
                  "default_field":"from_city",
                  "query":"Anjar"
                }}],
         "must_not":[],
         "should":[]
      }},
   "from":0,
   "size":1000,
   "sort":[],
   "aggs":{}
}

DEFAULT_HEADER = ['Id', 'LR Number', 'Customer Name', 'Status', 'Shipment Date', 'Finalized Date', 'From City',
          'To City', 'Distance', 'From Sublocation', 'To Sublocation', 'Order Value', 'Order Truck Type',
          'Registration Number', 'Used truck Type', 'Driver Mobile Number', 'Created By', 'Creation Time',
          'Discount Round Down Order Value', 'Discount %', 'Warehouse Start Loc', 'Warehouse Start Code',
          'Warehouse End Loc', 'Warehouse End Code', 'supply partner name', 'supply partner number',
          'driver_number', 'driver_name', 'Owner Name', 'Owner Mobile Number', 'Helper Number', 'Broker Name',
          'Broker Number', 'Indent Comments', 'Accepted By', 'Blocked Truck Type', 'Blocked by', 'Sector Name',
          'Tonnage', 'Per Ton Rate', 'Finance Payment Type', 'Freight Amount', 'Loading Charges', 'Unloading Charges',
          'Included Loading Charges', 'Included Unloading Charges', 'Halt Duration', 'Halt Charges', 'Halt Reason', 
          'Bonus Amount','Advance Channel Txt', 'Advance Cash Paid Txt', 'Advance Cash Amount', 'Advance eTransfer paid txt',
          'Advance eTransfer Amount', 'Settlement Channel Txt', 'Settlement Cash Amount', 'Settlement eTransfer Amount',
          'Destination halt charges', 'Dest halt duration', 'Dest Halt Reason', 'Total Dest misc charges', 
          'Total Source misc charges', 'Damages', 'Damages recovered Amt', 'Totals', 'Order Source Email',
          'Order Source Email Date', 'Order Requested By', 'Requested Date', 'Order Cancelled By', 'Mg Contract Id',
          'Order Invoice Status', 'MG Payment', 'Manual Placement', 'Product Name', 'Fuel Card Number', 
          'Advance Fuel Card Amount','Cash Card Number', 'Cash Card Amount', 'Advance Cash Card Amount Request', 
          'Customer Master Rate', 'Customer Rate Type', 'Customer Rate Value Type', 'Receivables', 'Profitability Index',
          'Order Requested', 'Order Processing', 'Rate Pending', 'KAM Review', 'Ops Review', 'Order Paused',
          'Accept Requested', 'Order Accepted', 'Approval Pending', 'Order Blocked', 'MG Blocked', 'Accepted - Under Review',
          'Truck Delayed', 'Order Incomplete', 'Cancelled By Customer', 'Cancelled', 'Truck Arrival Source',
          'LR Generated', 'Advance DocVerification', 'Advance Docs Rejected', 'Advance Docs Approval Requested',
          'Payment Pending', 'Advance Payment Rejected', 'Payment Done', 'Order Finalized', 'Started Trip',      
          'Truck Checklist Verified', 'Truck Owner Verification', 'Truck Departure Source', 'Truck In-Transit',
          'Truck Arrival Destination', 'Transit Issue', 'Transit Exception', 'Truck Unloading',
          'Truck Departure Destination', 'To Be Settled', 'Settlement DocVerification', 'Settlement Docs Rejected',
          'Settlement Docs Approval Requested', 'Settlement Pending', 'Settlement Hold', 'Settlement Payment Rejected',
          'Settlement Done', 'Docs Pending', 'Docs Received', 'Settlement Images Uploaded', 'Truck Rejected',
          'Truck Owner Verification Rejected', 'Waiting For Loading', 'Waiting For Customer Invoice', 'Order Completed',
          'Settlement Initiated', 'Settlement Issue Raised']
    

def getDataFromES(query, header=DEFAULT_HEADER):
    es = Elasticsearch(hosts=[ES_HOST])
    output = es.search(index=ES_INDEX, doc_type=ES_DOC_TYPE, body=query)
    doc_size = output['hits']['total']
    currentTime = datetime.now(tz.gettz(DEFAULT_TIMEZONE))
    title = 'order-reports_%s.csv' % (currentTime.strftime('%d_%m_%Y_%H_%M'))
    out_file = title
    out_file_obj = open(out_file, 'wb')
    csv_w = csv.writer(out_file_obj, dialect='excel')
    from_size, fetch_size = 0, 1000
    query['size'] = fetch_size
    # write header to file
    csv_w.writerow(header)
    for_loop_start = datetime.now()
    while from_size < doc_size:
        orders_doc_list = []
        query['from'] = from_size
        #print query
        output = es.search(index=ES_INDEX, doc_type=ES_DOC_TYPE, body=query)
        orders_doc_list.extend(output['hits']['hits'])
        from_size += fetch_size
        for data in orders_doc_list:
            try:
                data = data["_source"]["internal_report"]
            except Exception as e:
                continue
            final_data = [unicode(s).encode('utf-8') for s in data]
            csv_w.writerow(final_data)
    loop_time = datetime.now()
    print 'Total records read %s and time taken %s' % (doc_size, (loop_time - for_loop_start))
    out_file_obj.close()
    zip_file_name = '%s.zip' % (out_file)
    z_file = ZipFile(zip_file_name, 'w', compression=zipfile.ZIP_DEFLATED)
    z_file.write(out_file, arcname=title)
    z_file.close()
    print 'Wrote data into file %s and %s' %(out_file, zip_file_name)
    return zip_file_name

if __name__ == "__main__":
    getDataFromES(ES_QUERY)
