import math
import random
import zipfile
import csv
from zipfile import ZipFile, ZipInfo
import pandas as pd
import requests
import numpy as np
import pandas as pd
from urlparse import urlparse
from os.path import splitext, basename
from pandas import Series, DataFrame, concat
from statsmodels.tsa.arima_model import ARIMA
from statsmodels.tsa.seasonal import seasonal_decompose
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.ensemble import RandomForestRegressor
from sklearn.feature_extraction import DictVectorizer
from math import sqrt
from sklearn.svm import SVR
from pylab import rcParams
import datetime as dt
from datetime import datetime, timedelta
from dateutil import parser, tz
from elasticsearch import Elasticsearch
import json

# OUR TIMEZONE
DEFAULT_TIMEZONE = "Asia/Kolkata"

## ES CONFIG
ES_HOST_PROD_E = "54.255.139.39"
ES_HOST_PROD_I = "172.31.31.1"
ES_HOST_PREPROD_E = "54.169.202.225"
ES_HOST_PREPROD_I  = "10.0.1.137"
#ES_HOST = ES_HOST_PREPROD_E
ES_HOST = ES_HOST_PREPROD_I
#ES_HOST = ES_HOST_PROD_I
ES_INDEX = "orders_alias"
ES_DOC_TYPE = "master"

DENT_RS = 25

## To get the order reports file 
ORDERS_DUMP_ZIP = "http://zinka-prod.s3.amazonaws.com/uploads/export_report/2017_09_12/order-reports_12_09_2017_22_49.csv.zip"

LOCAL_FILE="order-reports_11_10_2017_22_44.csv.zip"

PREPROD_ALB_URL = "http://pricing-exch.alb.prebb.net"
PREPROD_URL = "http://52.77.211.221:8080"
PROD_URL = "http://pricing-exch.alb.jinka.in"

SET_AUTOPRICE_API = "pricing-exch/v2/algoprices/current_price"
SET_CURRENTPRICE_API = "pricing-exch/v2/current_price"

#PUT_URL = PREPROD_URL
PUT_URL = PREPROD_ALB_URL
#PUT_URL = PROD_URL

## Make URL by appending prefix to API NAME
def makeURL(prefixURL, apiName):
    return "%s/%s" %(prefixURL, apiName)

SET_AUTOPRICE_API_URL = makeURL(PUT_URL, SET_AUTOPRICE_API)
SET_CURRENTPRICE_API_URL = makeURL(PUT_URL, SET_CURRENTPRICE_API)

## ES QUERY STRING
ES_QUERY = { 
    "query":{  
      "bool":{  
         "must":[],
         "must_not":[],
         "should":[
             {"match":{"from_city":"Anjar"}},
             {"match":{"from_city":"Mundra"}},
         ]
      }},
   "from":0,
   "size":1000,
   "sort":[],
   "aggs":{}
}

# DEFAULT HEADER FOR DATA DOWNLOADED FROM ES
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
    
PRODUCTS_MAP ={
 'Agriculture': {'ID': 10, 'TYPE': 'Agriculture'},
 'Alcoholic Beverages': {'ID': 100, 'TYPE': 'Alcoholic Beverages'},
 'Animal Feed': {'ID': 21, 'TYPE': 'Animal Feed'},
 'Apple': {'ID': 101, 'TYPE': 'Apple'},
 'Atta': {'ID': 102, 'TYPE': 'Atta'},
 'Auto parts': {'ID': 24, 'TYPE': 'Auto parts'},
 'BLOCKS': {'ID': 54, 'TYPE': 'BLOCKS'},
 'BV': {'ID': 45, 'TYPE': 'BV'},
 'Barley': {'ID': 103, 'TYPE': 'Barley'},
 'Batteries': {'ID': 3, 'TYPE': 'Batteries'},
 'Bentonite Powder': {'ID': 104, 'TYPE': 'Bentonite Powder'},
 'Beverages': {'ID': 4, 'TYPE': 'Beverages'},
 'Billet': {'ID': 40, 'TYPE': 'Billet'},
 'Biscuits': {'ID': 44, 'TYPE': 'Biscuits'},
 'Bitumen': {'ID': 105, 'TYPE': 'Bitumen'},
 'Cement': {'ID': 9, 'TYPE': 'Cement'},
 'Ceramic': {'ID': 106, 'TYPE': 'Ceramic'},
 'Chana': {'ID': 107, 'TYPE': 'Chana'},
 'Chemicals': {'ID': 15, 'TYPE': 'Chemicals'},
 'Chlor Alkali': {'ID': 36, 'TYPE': 'Chlor Alkali'},
 'Choclairs': {'ID': 47, 'TYPE': 'Choclairs'},
 'Clay': {'ID': 13, 'TYPE': 'Clay'},
 'Coal': {'ID': 8, 'TYPE': 'Coal'},
 'Cocoa Beans': {'ID': 50, 'TYPE': 'Cocoa Beans'},
 'Coconut': {'ID': 108, 'TYPE': 'Coconut'},
 'Coconut Oil': {'ID': 109, 'TYPE': 'Coconut Oil'},
 'Coil': {'ID': 173, 'TYPE': 'Coil'},
 'Construction': {'ID': 5, 'TYPE': 'Construction'},
 'Consumer Durables': {'ID': 17, 'TYPE': 'Consumer Durables'},
 'Container': {'ID': 110, 'TYPE': 'Container'},
 'Copra': {'ID': 111, 'TYPE': 'Copra'},
 'Coriander': {'ID': 112, 'TYPE': 'Coriander'},
 'Cotton Thread': {'ID': 113, 'TYPE': 'Cotton Thread'},
 'Cotton Yarn': {'ID': 114, 'TYPE': 'Cotton Yarn'},
 'Courier': {'ID': 2, 'TYPE': 'Courier'},
 'Crumb': {'ID': 49, 'TYPE': 'Crumb'},
 'DB': {'ID': 34, 'TYPE': 'DB'},
 'Dal': {'ID': 169, 'TYPE': 'DAL'},
 'Drum': {'ID': 115, 'TYPE': 'Drum'},
 'Extrusions Semi': {'ID': 42, 'TYPE': 'Extrusions Semi'},
 'FMC Durables': {'ID': 11, 'TYPE': 'FMC Durables'},
 'Farm Equipments': {'ID': 18, 'TYPE': 'Farm Equipments'},
 'Fertilizer': {'ID': 116, 'TYPE': 'Fertilizer'},
 'Flour': {'ID': 32, 'TYPE': 'Flour'},
 'Foam': {'ID': 117, 'TYPE': 'Foam'},
 'Fruit': {'ID': 118, 'TYPE': 'Fruit'},
 'Ghee': {'ID': 119, 'TYPE': 'Ghee'},
 'Granite': {'ID': 23, 'TYPE': 'Granite'},
 'Gypsum': {'ID': 120, 'TYPE': 'Gypsum'},
 'Halls': {'ID': 48, 'TYPE': 'Halls'},
 'IT and Electronic Goods': {'ID': 27, 'TYPE': 'IT and Electronic Goods'},
 'Ingot': {'ID': 37, 'TYPE': 'Ingot'},
 'Iron': {'ID': 121, 'TYPE': 'Iron'},
 'Lumber': {'ID': 122, 'TYPE': 'Lumber'},
 'Lumps': {'ID': 123, 'TYPE': 'Lumps'},
 'Machinery': {'ID': 124, 'TYPE': 'Machinery'},
 'Marble (Blocks)': {'ID': 125, 'TYPE': 'Marble (Blocks)'},
 'Marble (Slabs)': {'ID': 126, 'TYPE': 'Marble (Slabs)'},
 'Metal Scrap': {'ID': 127, 'TYPE': 'Metal Scrap'},
 'Metals': {'ID': 6, 'TYPE': 'Metals'},
 'Nitrogen Cylinders': {'ID': 128, 'TYPE': 'Nitrogen Cylinders'},
 'Non-metals': {'ID': 26, 'TYPE': 'Non-metals'},
 'Nutrition': {'ID': 19, 'TYPE': 'Nutrition'},
 'Oil': {'ID': 129, 'TYPE': 'Oil'},
 'Oil Drum': {'ID': 130, 'TYPE': 'Oil Drum'},
 'Onion': {'ID': 131, 'TYPE': 'Onion'},
 'Others': {'ID': 99, 'TYPE': 'Others'},
 'PANELS': {'ID': 55, 'TYPE': 'PANELS'},
 'PB': {'ID': 33, 'TYPE': 'PB'},
 'Packaged Foods': {'ID': 12, 'TYPE': 'Packaged Foods'},
 'Packages Foods': {'ID': 132, 'TYPE': 'Packages Foods'},
 'Packaging': {'ID': 133, 'TYPE': 'Packaging'},
 'Paint': {'ID': 16, 'TYPE': 'Paint'},
 'Paper': {'ID': 134, 'TYPE': 'Paper'},
 'Personal Care Products': {'ID': 29, 'TYPE': 'Personal Care Products'},
 'Petroleum Coke': {'ID': 135, 'TYPE': 'Petroleum Coke'},
 'Pharmaceuticals': {'ID': 7, 'TYPE': 'Pharmaceuticals'},
 'Plaster of Paris': {'ID': 136, 'TYPE': 'Plaster of Paris'},
 'Plastic Agglo LD': {'ID': 137, 'TYPE': 'Plastic Agglo LD'},
 'Plastic Agglo LL': {'ID': 138, 'TYPE': 'Plastic Agglo LL'},
 'Plastics': {'ID': 139, 'TYPE': 'Plastics'},
 'Plywood': {'ID': 140, 'TYPE': 'Plywood'},
 'Polyester': {'ID': 166, 'TYPE': 'Polyester'},
 'Powder': {'ID': 43, 'TYPE': 'Powder'},
 'Powder (Bags)': {'ID': 141, 'TYPE': 'Powder (Bags)'},
 'Powder (Bags) - Big Factory': {'ID': 142, 'TYPE': 'Powder (Bags) - Big Factory'},
 'Powder (Bags) - Medium Factory': {'ID': 143, 'TYPE': 'Powder (Bags) - Medium Factory'},
 'Powder (Bags) - Small Factory': {'ID': 144, 'TYPE': 'Powder (Bags) - Small Factory'},
 'Powder (Loose)': {'ID': 145, 'TYPE': 'Powder (Loose)'},
 'Powder Loose (Non - Sample)': {'ID': 168, 'TYPE': 'Powder Loose (Non - Sample)'},
 'Powder Loose (Sample)': {'ID': 167, 'TYPE': 'Powder Loose (Sample)'},
 'Processed Food': {'ID': 146, 'TYPE': 'Processed Food'},
 'Pulses': {'ID': 147, 'TYPE': 'Pulses'},
 'Rajma': {'ID': 170, 'TYPE': 'Rajma'},
 'Raw Tea': {'ID': 25, 'TYPE': 'Raw Tea'},
 'Resin': {'ID': 52, 'TYPE': 'Resin'},
 'Rice': {'ID': 148, 'TYPE': 'Rice'},
 'Rolled Semi': {'ID': 41, 'TYPE': 'Rolled Semi'},
 'Salt': {'ID': 149, 'TYPE': 'Salt'},
 'Sand': {'ID': 150, 'TYPE': 'Sand'},
 'Scrap Material': {'ID': 53, 'TYPE': 'Scrap Material'},
 'Scrap Waste': {'ID': 20, 'TYPE': 'Scrap Waste'},
 'Seed': {'ID': 151, 'TYPE': 'Seed'},
 'Slab': {'ID': 38, 'TYPE': 'Slab'},
 'Slurry': {'ID': 152, 'TYPE': 'Slurry'},
 'Snacks': {'ID': 1, 'TYPE': 'Snacks'},
 'Solar Panels': {'ID': 22, 'TYPE': 'Solar Panels'},
 'Soya': {'ID': 31, 'TYPE': 'Soya'},
 'Spices': {'ID': 153, 'TYPE': 'Spices'},
 'Steel': {'ID': 154, 'TYPE': 'Steel'},
 'Steel Coil': {'ID': 171, 'TYPE': 'Steel Coil'},
 'Steel Fabrication': {'ID': 155, 'TYPE': 'Steel Fabrication'},
 'Steel Flat': {'ID': 172, 'TYPE': 'Steel Flat'},
 'Sugar': {'ID': 14, 'TYPE': 'Sugar'},
 'Sugar.': {'ID': 51, 'TYPE': 'Sugar.'},
 'Synthetics': {'ID': 28, 'TYPE': 'Synthetics'},
 'Tang': {'ID': 46, 'TYPE': 'Tang'},
 'Tarcoal Drum': {'ID': 156, 'TYPE': 'Tarcoal Drum'},
 'Teak Wood': {'ID': 157, 'TYPE': 'Teak Wood'},
 'Textiles': {'ID': 158, 'TYPE': 'Textiles'},
 'Tiles': {'ID': 159, 'TYPE': 'Tiles'},
 'Timber': {'ID': 160, 'TYPE': 'Timber'},
 'VAS': {'ID': 35, 'TYPE': 'VAS'},
 'Vegetable': {'ID': 161, 'TYPE': 'Vegetable'},
 'Veneer': {'ID': 162, 'TYPE': 'Veneer'},
 'Wheat': {'ID': 163, 'TYPE': 'Wheat'},
 'Wire Rod': {'ID': 39, 'TYPE': 'Wire Rod'},
 'Wood': {'ID': 164, 'TYPE': 'Wood'},
 'Yellow Peas': {'ID': 165, 'TYPE': 'Yellow Peas'}
}

BASE_SUBLOCATIONS_MAP={
 ## FROM SUBLOTCIONS at KANDLA CLUSTER
 'Anjar': {'ID': 3133, 'LOCATION_ID': 557},
 'Apnanagar': {'ID': 711, 'LOCATION_ID': 557},
 'Bharapar': {'ID': 3237, 'LOCATION_ID': 557},
 'Bhimasar': {'ID': 3344, 'LOCATION_ID': 557},
 'Dhaneti': {'ID': 2740, 'LOCATION_ID': 557},
 'Gandhidham': {'ID': 2403, 'LOCATION_ID': 557},
 'Kandla': {'ID': 1111, 'LOCATION_ID': 557},
 'Lilashah Nagar': {'ID': 852, 'LOCATION_ID': 557},
 'Padana': {'ID': 3513, 'LOCATION_ID': 148},
 'Tuna': {'ID': 2732, 'LOCATION_ID': 557},

 ## To SUBLOCATIONS originated from KANDLA cluster
 'Agra': {'ID': 78, 'LOCATION_ID': 46},
 'Ambala': {'ID': 183, 'LOCATION_ID': 100},
 'Dabok': {'ID': 2070, 'LOCATION_ID': 841},
 'Dasna': {'ID': 3354, 'LOCATION_ID': 128},
 'Dholka': {'ID': 3449, 'LOCATION_ID': 498},
 'Dholpur House': {'ID': 1092, 'LOCATION_ID': 46},
 'Ghaziabad': {'ID': 3010, 'LOCATION_ID': 128},
 'Karnal': {'ID': 2422, 'LOCATION_ID': 673},
 'Kashmere Gate': {'ID': 2186, 'LOCATION_ID': 41},
 'Ludhiana': {'ID': 1458, 'LOCATION_ID': 153},
 'Mundra': {'ID': 1813, 'LOCATION_ID': 562},
 'Madhopur': {'ID': 3317, 'LOCATION_ID': 156},
 'Modinagar': {'ID': 1240, 'LOCATION_ID': 1942},
 'Muzaffarnagar': {'ID': 3347, 'LOCATION_ID': 1952},
 'Narela': {'ID': 235, 'LOCATION_ID': 41},
 'Panipat': {'ID': 1576, 'LOCATION_ID': 678},
 'Pathankot': {'ID': 980, 'LOCATION_ID': 156},
 'Pinglaj': {'ID': 1144, 'LOCATION_ID': 567},
 'Pune': {'ID': 204, 'LOCATION_ID': 35},
 'Rabariyawas': {'ID': 3379, 'LOCATION_ID': 1665},
 'Varanasi': {'ID': 91, 'LOCATION_ID': 54},
 'Narolgam': {'ID': 3412, 'LOCATION_ID': 101},  
 'Ahmedabad': {'ID': 181, 'LOCATION_ID': 101}, 
 'Bhagwanpur': {'ID': 2951, 'LOCATION_ID': 106},
 'Dera Bassi': {'ID': 2624, 'LOCATION_ID': 1515},
 'Himmatnagar': {'ID': 2930, 'LOCATION_ID': 612},
 'Kheda': {'ID': 1200, 'LOCATION_ID': 567},
 'Meerut': {'ID': 3159, 'LOCATION_ID': 1942},
 'Roorkee': {'ID': 1490, 'LOCATION_ID': 106},
 'Shahjahanpur': {'ID': 2962, 'LOCATION_ID': 1972},    
 'Palanpur': {'ID': 2734, 'LOCATION_ID': 182}    
}

BASE_LOCATIONS_MAP={
 'Anjar': {'LOCATION_ID': 557},
 'Agra': {'LOCATION_ID': 46},
 'Mundra': {'LOCATION_ID': 562},
 'Pathankot': {'LOCATION_ID': 156},
 'Ahmedabad': {'LOCATION_ID': 101},
 'Ambala': {'LOCATION_ID': 100},
 'Himmatnagar': {'LOCATION_ID': 612},
 'Kharar': {'LOCATION_ID': 1515},    
 'Matar': {'LOCATION_ID': 567},    
 'Meerut': {'LOCATION_ID': 1942},
 'Muzaffarnagar': {'LOCATION_ID': 1952},    
 'Roorkee': {'LOCATION_ID': 106},
 'Shahjahanpur': {'LOCATION_ID': 1972},  
 'Vadgam': {'LOCATION_ID': 182},     
}


## Info about a truck type by it's name
TT_INFO_MAP={
    "19ft/7.5T"         :{"id":1,  "body_type":"FULL_BODY",  "capacity":7.5,   "length":19.00 },
    "32ft SAC"          :{"id":2,  "body_type":"CONTAINER",  "capacity":7.5,   "length":32.00 },
    "32ft MAC"          :{"id":3,  "body_type":"CONTAINER",  "capacity":14.5,  "length":32.00 },
    "16T/15T"           :{"id":4,  "body_type":"FULL_BODY",  "capacity":16,    "length":-1 },
    "9T"                :{"id":5,  "body_type":"HALF_BODY",  "capacity":9,     "length":-1 },
    "21T/20T"           :{"id":6,  "body_type":"HALF_BODY",  "capacity":21,    "length":-1 },
    "20ft SAC/24ft SAC" :{"id":7,  "body_type":"CONTAINER",  "capacity":7.5,   "length":20.00 },
    "25T/24T"           :{"id":8,  "body_type":"HALF_BODY",  "capacity":25,    "length":-1 },
    "20 Ft Container"   :{"id":9,  "body_type":"CONTAINER",  "capacity":7.5,   "length":20.00 },
    "7.5 MT"            :{"id":10, "body_type":"FULL_BODY",  "capacity":7.5,   "length":19.00 },
    "40ft MAC"          :{"id":14, "body_type":"CONTAINER",  "capacity":-1,    "length":40.00 },
    "24T"               :{"id":19, "body_type":"HALF_BODY",  "capacity":25,    "length":-1 },
    "24ft MAC"          :{"id":21, "body_type":"CONTAINER",  "capacity":14,    "length":24.00},
    "27T/28T"           :{"id":22, "body_type":"HALF_BODY",  "capacity":28,    "length":-1},
    "34T"               :{"id":23, "body_type":"HALF_BODY",  "capacity":34,    "length":-1},
    "32ft HQ"           :{"id":100,"body_type":"HQ",         "capacity":-1,    "length":32.00},
    "Flat Bed 20ft"     :{"id":101,"body_type":"FLAT_BED",   "capacity":-1,    "length":20.00},
    "Flat Bed 40ft"     :{"id":102,"body_type":"FLAT_BED",   "capacity":-1,    "length":40.00},
    "Tanker"            :{"id":103,"body_type":"TANKER",     "capacity":-1,    "length":-1},
    "Warm Tanker"       :{"id":104,"body_type":"WARM_TANKER","capacity":-1,    "length":-1},
    "Bulker"            :{"id":105,"body_type":"BULKER",     "capacity":-1,    "length":-1},
    "Dumper"            :{"id":106,"body_type":"DUMPER",     "capacity":-1,    "length":-1},
    "30 MT"             :{"id":108,"body_type":"HALF_BODY",  "capacity":30,    "length":-1},
}

## Info about a truck type by it's name
TT_INFO_MAP_SETPRICE={
    "21T/20T"           :{"body_type":"HALF_BODY",  "capacity":21},
    "27T/28T"           :{"body_type":"HALF_BODY",  "capacity":27},
    "30 MT"             :{"body_type":"HALF_BODY",  "capacity":30},
    "34T"               :{"body_type":"HALF_BODY",  "capacity":35},
}



""" 
### GB TRUCK TYPE MAPPING
+-----+-------------------+-------------+--------------+--------+
| id  | name              | body_type   | capacity_new | length |
+-----+-------------------+-------------+--------------+--------+
|   1 | 19ft/7.5T         | full_body   |         7.50 |  19.00 |
|   2 | 32ft SAC          | container   |         7.50 |  32.00 |
|   3 | 32ft MAC          | container   |        14.50 |  32.00 |
|   4 | 16T/15T           | full_body   |        16.00 |  -1.00 |
|   5 | 9T                | half_body   |         9.00 |  -1.00 |
|   6 | 21T/20T           | half_body   |        21.00 |  -1.00 |
|   7 | 20ft SAC/24ft SAC | container   |         7.50 |  20.00 |
|   8 | 25T/24T           | half_body   |        25.00 |  -1.00 |
|   9 | 20 Ft Container   | container   |         7.50 |  20.00 |
|  10 | 7.5 MT            | full_body   |         7.50 |  19.00 |
|  14 | 40ft MAC          | container   |        -1.00 |  40.00 |
|  19 | 24T               | half_body   |        25.00 |  -1.00 |
|  21 | 24ft MAC          | container   |        14.00 |  24.00 |
|  22 | 27T/28T           | half_body   |        28.00 |  -1.00 |
|  23 | 34T               | half_body   |        34.00 |  -1.00 |
| 100 | 32ft HQ           | hq          |        -1.00 |  32.00 |
| 101 | Flat Bed 20ft     | flat_bed    |        -1.00 |  20.00 |
| 102 | Flat Bed 40ft     | flat_bed    |        -1.00 |  40.00 |
| 103 | Tanker            | tanker      |        -1.00 |  -1.00 |
| 104 | Warm Tanker       | warm_tanker |        -1.00 |  -1.00 |
| 105 | Bulker            | bulker      |        -1.00 |  -1.00 |
| 106 | Dumper            | dumper      |        -1.00 |  -1.00 |
| 108 | 30 MT             | half_body   |        30.00 |  -1.00 |
+-----+-------------------+-------------+--------------+--------+
"""

## Parse a date
def parseDate(d):
    try:
        return parser.parse(d)        
    except:
        return None

## Percentage Error    
def pererr(err, avg):
    return (err/avg)*100

## Return Mean
def mean(l):
    return reduce(lambda x, y: x + y, l) / len(l)

    
# create a differenced series
def difference(dataset, interval=1):
    diff = list()
    for i in range(interval, len(dataset)):
        value = dataset[i] - dataset[i - interval]
        diff.append(value)
    return np.array(diff)


# invert differenced value
def inverse_difference(history, pred, interval=1):
    return pred + history[-interval]

    
## Add some delta to a time stamp 
def addDelta(v):
    delta = timedelta(0,random.randint(1, 15), random.randint(1, 1001))
    dt = parseDate(str(v[0]))
    if dt is not None:
        return dt+delta
    else:
        return dt
    
## get earliest out of two dates
def maxD(v):
    dt1 = parseDate(v[0])
    dt2 = parseDate(v[1])
    if dt1 is not None and dt2 is not None:
        return max(dt1, dt2)
    elif dt1 is not None:
        return dt1
    else:
        return dt2

## Download a order reports from given URL
def getOrderReports(url):
    disassembled = urlparse(url)
    filename = basename(disassembled.path)
    headers = {'cache-control': "no-cache"}
    r = requests.request("GET", url, headers=headers, stream=True)    
    assert r.status_code == 200, "Failed to downlad orders data "
    if r.status_code == 200:
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(1024):
                f.write(chunk)
    # write the filename
    return filename


## Put predicted value to pricing server
def putPredictValue(url, payload):
    headers = { 'content-type': "application/json", 'cache-control': "no-cache" }
    r = requests.request("PUT", url, data=payload, headers=headers)
    assert r.status_code == 200, "Failed uploading data to pricing server %r" %r.json() 

## Set Current price on pricing server
def setCurrentPrice(url, payload):
    headers = { 'content-type': "application/json", 'cache-control': "no-cache", 'x-consumer-id': "3430" }
    r = requests.request("PUT", url, data=payload, headers=headers)
    assert r.status_code == 200, "Failed setting current price on pricing server"    
    
### read a dataframe from a zip file
def readData(zipf):
    zf = zipfile.ZipFile(zipf) 
    ## Read the first file from zip
    assert len(zf.namelist())>0, "Expecting atleast 1 csv file in zip"
    kp = pd.read_csv(zf.open(zf.namelist()[0]), header=0)
    return kp

## get DataFrame in required format
def getDataFrame(datafile):

    # get data frame from the zipped csv file
    kp = readData(datafile)
    
    # lets do some data cleansing, mungin
    kpi=kp.iloc[:, [0, 2, 6, 7, 8, 9, 10, 11, 14, 20, 22, 37, 38, 39, 75, 41, 93, 95]]

    # Picking earliest of Order Accepted or Order Blocked date as Order Date
    kpi['date']=kpi[['Order Accepted', 'Order Blocked']].apply(maxD, axis=1)
    #kpi.drop(['Order Accepted', 'Order Blocked'], axis=1, inplace=True)
    
    # droping all rows where we don't know about date (i.e date is null) as it won't make any sense in TS
    kpi = kpi[pd.isnull(kpi['date'])==False]

    # Lets try to make d as unique as possible by adding some secs, ms
    kpi['d'] = kpi[['date']].apply(addDelta, axis=1)
    
    # calculating per ton price for order
    kpi['ptr'] = (kpi['Freight Amount']/kpi['Tonnage'])

    return kpi

## CLUB DataFrame Based on TruckTypes
def clubTruckTypes(truck_types_to_club, kpi_groupBy):
    all_groups=[]
    tobe_clubbed={}
    ## Group certain DF based on TruckTypes
    for (x, y) in kpi_groupBy:
        if x[3] in truck_types_to_club:
            key = (x[0], x[1], x[2])
            if tobe_clubbed.get(key):
                tobe_clubbed[key].append(y)
            else:
                tobe_clubbed[key] = [y]
        else:
            all_groups.append((x, y))            
    for (k,v) in tobe_clubbed.iteritems():
        key = k+(truck_types_to_club[0],)
        all_groups.append((key, pd.concat(v)))        
    return all_groups

## Convert DATAFrame to Series with Resampling & Forward/Backword Filling NANs
def getSeriesWithResampleAndFNA(kpi, groupBY, truck_types_to_club, ptr="ptr", date="d", W=8):
    kpi_ = kpi[(pd.isnull(kpi[ptr])==False)]
    kpi_groupBy = kpi_.groupby(groupBY)
    kpi_groupBy = [x for x in list(kpi_groupBy) if len(x[1]) >= W]
    if truck_types_to_club:
        kpi_groupBy =  clubTruckTypes(truck_types_to_club, kpi_groupBy)   
    all_series = [(x[0], Series(x[1][ptr].values, index=x[1][date].values)) for x in kpi_groupBy]
    all_series = [(x, s.sort_index()) for (x, s) in all_series]
    all_series = [(x, s.resample("1d").fillna(method="ffill").fillna(method="bfill")) for (x, s) in all_series]
    return [(x, s) for (x, s) in all_series if len(s) > W]


## Convert DATAFrame to Series without Any Resampling
def getSeriesWithoutResample(kpi, groupBY, truck_types_to_club, ptr="ptr", date="d", W=8):
    kpi_ = kpi[(pd.isnull(kpi[ptr])==False)]
    kpi_groupBy = kpi_.groupby(groupBY)
    if truck_types_to_club:
        kpi_groupBy =  clubTruckTypes(truck_types_to_club, kpi_groupBy)   
    kpi_groupBy = [x for x in list(kpi_groupBy) if len(x[1]) >= W]
    all_series = [(x[0], Series(x[1][ptr].values, index=x[1][date].values)) for x in kpi_groupBy]
    all_series = [(x, s.sort_index()) for (x, s) in all_series]
    return [(x, s) for (x, s) in all_series if len(s) >= W]

## get moving window features from the series for W size window with differencing
def getMovingWindowFeatures(series, W, last=False):
    X = series.values
    # converting time to epoch
    time = [t.to_datetime64().astype(np.int64)//10 ** 9 for t in series.index]
    # move each window(W+2) by 1  data point
    # keep last chunk for prediction
    NWs = len(X)-W-1 

    # Features should have dimensions (NWs x WSize)
    # Labels should have dimensions (NWs)
    feat = np.zeros( [NWs, 2*W] ) - 9999999.
    labl = np.zeros( [NWs] ) - 99999
    delta = [0]*NWs
    
    # Now fill the train feature and label arrays with differencing
    for i in range(NWs):
        feat[i,:] = np.concatenate((time[i:i+W] - time[i+W], X[i:i+W] - X[i+W]))        
        assert np.isfinite(feat[i, :]).all(), (W, len(X), NWs, i, series)
        labl[i]   = X[i+W+1] - X[i+W]    
        delta[i] = X[i+W]

    if last:
        n = len(X)-1
        return (feat, labl, delta, np.concatenate((time[n-W:n] - time[n], X[n-W:n] - X[n])), X[n])
    return (feat, labl, delta)

# get features for all the series with given window size
def getFeaturesForAllSeries(all_series, keys, W=7):
    all_series_features = [(x[0], getMovingWindowFeatures(x[1], W, True)) for x in all_series]
    train_feat, train_labl = [], []
    test_feat, test_delta = [], []
    for (x,(feat,lbl,_,pf,delta)) in all_series_features:
        for (f, l) in zip(feat, lbl):
            train_feat.append(dict(zip (keys, list(x)+f.tolist())))
            train_labl.append(l)
        test_feat.append(dict(zip (keys, list(x)+pf.tolist())))
        test_delta.append(delta)        
    return ((train_feat, train_labl), (test_feat, test_delta))

# get features for all the series with given window size and split into training and testing datasets
def getFeaturesForAllSeriesWithSplit(all_series, keys, W=7, train_frac = 0.5):
    all_series_features = [(x[0], getMovingWindowFeatures(x[1], W)) for x in all_series]    
    train_feat, train_labl = [], []
    test_feat, test_labl = [], []
    test_delta = []    
    for (x,(feat,lbl, delta)) in all_series_features:
        t_size = int(len(feat)*train_frac)    
        for (f, l) in zip(feat[:t_size], lbl[:t_size]):
            train_feat.append(dict(zip (keys, list(x)+f.tolist())))
            train_labl.append(l)
        for (f, l) in zip(feat[t_size:], lbl[t_size:]):
            test_feat.append(dict(zip (keys, list(x)+f.tolist())))
            test_labl.append(l)
        test_delta = test_delta + delta[t_size:]    
    return ((train_feat, train_labl), (test_feat, test_labl, test_delta))

# Transform Features into Vectorize form
def transformFeatures(train_feat, test_feat, vec = DictVectorizer()):
    train_feat_t = vec.fit_transform(train_feat).toarray()
    test_feat_t = vec.transform(test_feat).toarray()
    return (train_feat_t, test_feat_t)


## Download data from Elastic Search
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


## get payload as json string
def makeJsonPayload(price, truck_names, product_type, from_loc_id, to_loc_id, from_sub_id, to_sub_id,  
                    price_unit='TON', price_type='SP', comments="", algo =""):
    truck_types = []
    for truck_name in truck_names:        
        if algo:
            # get truck type object from INFO map
            tt = TT_INFO_MAP[truck_name].copy()
            # get rid of id
            del tt["id"]
        else:
            tt = TT_INFO_MAP_SETPRICE[truck_name].copy()

        # delete capacity if it's -1
        if tt.get('capacity') == -1:
            del tt['capacity']

        # delete Truck length if it's -1 
        if tt.get('length') == -1:
            del tt['length']     
        truck_types.append(tt)

        d = {
        'from_location_id': from_loc_id,
        'to_location_id': to_loc_id,
        'price': price,
        'price_type': price_type,
        'price_unit': price_unit,
        'product_id': PRODUCTS_MAP[product_type]["ID"],
        'truck_type': truck_types,
        'comments':comments
        }
    
    if from_sub_id and to_sub_id:
        d.update({"from_sublocation_id":from_sub_id, "to_sublocation_id":to_sub_id})

    if algo:
        d.update({"algo":algo})
    
    ## dump as json string
    return json.dumps(d)


# runRFR takes a pandas.Series with dateIndex and T as number of Test Points
def runRFR(series, T, W):
    X = series.values
    size = len(X) - T
    time = [t.to_datetime64().astype(np.int64)//10 ** 9 for t in series.index]

    # move each window by 1 data point
    # keep last chunk for prediction
    NWs = len(X)-W-T-1 

    # Features should have dimensions (NWs x WSize)
    # Labels should have dimensions (NWs)
    feat = np.zeros( [NWs+T, 2*W] ) - 9999999.
    labl = np.zeros( [NWs+T] ) - 99999
    
    # Now fill the train feature and label arrays with differencing
    for i in range(int(NWs+T)):
        feat[i,:] = np.concatenate((time[i:i+W] - time[i+W], X[i:i+W] - X[i+W]))        
        #feat[i,:] = X[i:i+W] - X[i+W]
        labl[i]   = X[i+W+1] - X[i+W]
    
    # divide data into train and test
    train_feat, train_labl = feat[:NWs], labl[:NWs]
    test_feat, test_labl = feat[NWs:], labl[NWs:]

    # Let's learn a Random Forrest Regressor
    rfRegress = RandomForestRegressor(n_estimators=500, 
                                criterion='mae', max_depth=None, 
                                min_samples_split=2, min_samples_leaf=1, 
                                max_features='auto', max_leaf_nodes=None, 
                                bootstrap=True, oob_score=False, n_jobs=-1, 
                                random_state=None, verbose=0)
    # Fit Model
    rfRegress.fit(train_feat, train_labl)
    pred = rfRegress.predict(test_feat)

    ### Lets get some numbers
    test_labl_act = np.zeros( [T] )
    pred_act = np.zeros( [T] )
    errs = []
    for i in range(T):
        test_labl_act[i] = test_labl[i] + X[i+NWs+W]
        pred_act[i] = pred[i] + X[i+NWs+W]
        err=pererr(abs(test_labl_act[i]-pred_act[i]),test_labl_act[i])
        errs.append(err)
    rmse = sqrt(mean_squared_error(pred_act, test_labl_act))
    mae = mean_absolute_error(pred_act, test_labl_act)

    # divide data into train and test for last day
    train_feat, train_labl = feat[:-1], labl[:-1]
    test_feat, test_labl = feat[-1:], labl[-1:]

    # Fit Model
    rfRegress.fit(train_feat, train_labl)
    pred_ = rfRegress.predict(test_feat)
    pred_act_ = pred_[0] + X[NWs+W+T-1]
    test_labl_act_ = test_labl[0] + X[NWs+W+T-1]
    errs.append(pererr(abs(test_labl_act_-pred_act_), test_labl_act_))
    today = str(dt.date.today())
    print('%s, RFR, RMSE: %d, MAE:%d, MEAN Err:%.2f pc, LAST Err:%.2f pc'% (today, rmse, mae, mean(errs), errs[-1]))

# run RF takes a pandas.Series with dateIndex and get next T prediction
def runRFRForTS(series, W, T):
    X = series.values
    time = [t.to_datetime64().astype(np.int64)//10 ** 9 for t in series.index]
    # move each window by 1 data point
    # keep last chunk for prediction
    # It's basically W+2 size window for one feature first W as feature W+1 for diff & W+2 is label
    NWs = len(X)-W-1 #(X-(W+2)+1) 
    
    # Features should have dimensions (NWs x WSize)
    # Labels should have dimensions (NWs)
    train_feat = np.zeros( [NWs, 2*W] ) - 9999999. 
    train_labl = np.zeros( [NWs] ) - 99999.

    # Now fill the train feature and label arrays with differencing
    for i in range(int(NWs)):
        train_feat[i,:] = np.concatenate((time[i:i+W] - time[i+W], X[i:i+W] - X[i+W]))        
        #train_feat[i,:] = X[i:i+W] - X[i+W]
        train_labl[i]   = X[i+W+1] - X[i+W]
        
    # Let's learn a Random Forrest Regressor
    rfRegress = RandomForestRegressor(n_estimators=500, 
                                criterion='mae', max_depth=None, 
                                min_samples_split=2, min_samples_leaf=1, 
                                max_features='auto', max_leaf_nodes=None, 
                                bootstrap=True, oob_score=False, n_jobs=-1, 
                                random_state=None, verbose=0)
    #Learn The model
    rfRegress.fit(train_feat, train_labl)
    predictions=[]    
    
    # Let's predict some values
    for i in range(T):
        # Now make Feature for Prediction
        pred_feat = np.zeros([1, 2*W])
        n = len(X)-1
        pred_feat[0,:] = np.concatenate((time[n-W:n] - time[n], X[n-W:n] - X[n]))
        #pred_feat[0,:] = X[n-W:n] - X[n]
        pred = rfRegress.predict(pred_feat)
        # From prediction we got differencd Value
        # Have to convert it to usable predicton
        pred_act = pred[0] + X[n-1]
        predictions.append(pred_act)
        X = np.append(X, pred_act)
    return predictions

#get next day prediction with ARIMA 
def nextDayPredictionWithARIMA(X, I, ar, i, ma):
    # get differenced SERIES
    differenced = difference(X, I)
    # Make an ARIMA model
    model = ARIMA(differenced, order=(ar,i,ma))
    # FIT the model with data
    model_fit = model.fit(disp=False)
    # one-step out-of sample forecast
    pred = (model_fit.forecast()[0][0])        
    # From prediction we got differencd Value
    # Have to convert it to usable predicton
    return inverse_difference(X, pred, I)


# runArimaForTs takes a pandas.Series with dateIndex and T as number of Test Points, (ar, i, ma) as arima params 
def runArimaForTs(series, T, I, ar, i, ma):
    X = series.values
    size = len(X) - T
    train, test = X[0:size], X[size:]
    predictions = list()
    errs = []
    today = str(dt.date.today())
    for t in range(len(test)):
        pred_act = nextDayPredictionWithARIMA(train, I, ar, i, ma)
        predictions.append(pred_act)
        # Get the actual observation
        obs = test[t]
        # Append OBS to training set
        train = np.append(train, obs)
        # Get the percentage error
        err=pererr(abs(obs-pred_act),obs)
        # append to errs
        errs.append(err)
    rmse = sqrt(mean_squared_error(test, predictions))
    mae = mean_absolute_error(test, predictions)
    print('%s, ARIMA, RMSE: %d, MAE:%d, MEAN Err:%.2f pc, LAST Err:%.2f pc'% (today, rmse, mae, mean(errs), errs[-1]))
    # Now get the next day prediction
    pred_act = nextDayPredictionWithARIMA(train, I, ar, i, ma)
    predictions.append(pred_act)
    return predictions

# Run Improved RFR with given dataframe df, Window Size W and T as in # of predictions to do
def runIRFRForDF(df, groupBY, keys, truck_types_to_club,  W=7, T=1):
    rfr = RandomForestRegressor(n_estimators=100, 
                            criterion='mae', max_depth=None, 
                            min_samples_split=2, min_samples_leaf=1, 
                            max_features='auto', max_leaf_nodes=None, 
                            bootstrap=True, oob_score=False, n_jobs=-1, 
                            random_state=None, verbose=0)
    all_series = getSeriesWithoutResample(df, groupBY, truck_types_to_club)
    ((train_feat, train_labl), (test_feat, delta)) = getFeaturesForAllSeries(all_series, keys)
    # Transform the features
    (train_feat_t, test_feat_t) = transformFeatures(train_feat, test_feat)
    # Learn IRFR
    rfr.fit(train_feat_t, train_labl)
    pred = rfr.predict(test_feat_t)    
    pred_act = []
    for i in range(len(pred)):
        pred_act.append(pred[i]+delta[i])
    return {(":".join([t[k] for k in groupBY])):(p, t) for t,p in zip(test_feat, pred_act)}        

## get Last Day price 
def getLastDayPrice(kpi, from_sub, to_sub, truck_name, product_type):
    # Let's filter orders based on our choices
    df = kpi[(kpi['From Sublocation'] == from_sub) & 
                 (kpi['To Sublocation'] == to_sub) &
                 (kpi['Used truck Type'] == truck_name) & 
                 (kpi['Product Name'] == product_type)]
    
    # Lets get the series
    series = Series(df['Per Ton Rate'].values, index=df['d']).sort_index()  
    return series.sort_index()[-1]


def getNextDayPrices(df, key, irfrVals, ptr="Per Ton Rate", date="d"):
    # Lets get the series
    series = Series(df[ptr].values, index=df[date]).sort_index()  
    
    # Let's resample series into 1day bucket and filling missing values by mean
    series_fna = series.resample("1d").mean().fillna(method="ffill")

    # Window size
    W = 7
    # How many days to predict
    T = 7
    
    # ARIMA params
    I = 1
    p = 3
    q = 1
    r = 1
    
    # get IRFR values for irfr           
    irfr = [series_fna[-1]]
    try:
        irfr = [irfrVals[key][0]]
    except Exception as e:
        pass
    
    # Get ERRORs for RFR
    try:
       rfr = runRFR(series, W, T)
    except Exception as e:
       pass

    # get me next day prediction for this series
    rfr = [series_fna[-1]]
    try:
        rfr = runRFRForTS(series, W, 1)
    except Exception as e:
        pass

    arima = [series_fna[-1]]
    try:
        arima = runArimaForTs(series_fna, T, I, p, q, r)
    except Exception as e:
        pass
    
    rfr_r = round(rfr[-1], -1)
    arima_r = round(arima[-1], -1)
    irfr_r = round(irfr[-1], -1)
    return (arima_r, rfr_r, irfr_r)

## Main function to call for prediction
def makePredictionAndSave(kpi, from_sub, to_sub, truck_name, product_type, irfrVals):    
    # Unit of Price
    price_unit='TON'
    # Price TYPE ? .. have to ask Hrishi/Shashank :X
    price_type='SP'

    # Let's filter orders based on our choices
    df = kpi[(kpi['From Sublocation'] == from_sub) & 
                 (kpi['To Sublocation'] == to_sub) &
                 (kpi['Used truck Type'] == truck_name) & 
                 (kpi['Product Name'] == product_type)]
   
    key = "%s:%s:%s:%s" %(from_sub, to_sub, truck_name, product_type)
    tommorrow = str((dt.date.today() + dt.timedelta(days=1)))

    (arima_r, rfr_r, irfr_r) = getNextDayPrices(df, key, irfrVals)    

    print "FROM_SUBLOCATION, TO_SUBLOCATION, TRUCK_NAME, PRODUCT_TYPE, ARIMA_PRICE, RFR_PRICE, IRFR_PRICE"
    print "%s, %s, %s, %s, %s, %s, %s, %s" %(tommorrow, from_sub, to_sub, truck_name, product_type, arima_r, rfr_r, irfr_r)
    
    
    # make the json payload
    payload1 = makeJsonPayload(rfr_r, [truck_name], product_type, 
                               BASE_SUBLOCATIONS_MAP[from_sub]["LOCATION_ID"],
                               BASE_SUBLOCATIONS_MAP[to_sub]["LOCATION_ID"],
                               BASE_SUBLOCATIONS_MAP[from_sub]["ID"],
                               BASE_SUBLOCATIONS_MAP[to_sub]["ID"],
                               price_unit, price_type,
                              'Setting Price using RFR', 'RFR')
    payload2 = makeJsonPayload(arima_r, [truck_name], product_type, 
                               BASE_SUBLOCATIONS_MAP[from_sub]["LOCATION_ID"],
                               BASE_SUBLOCATIONS_MAP[to_sub]["LOCATION_ID"],
                               BASE_SUBLOCATIONS_MAP[from_sub]["ID"],
                               BASE_SUBLOCATIONS_MAP[to_sub]["ID"],
                               price_unit, price_type,
                              'Setting Price using ARIMA', 'ARIMA')
    payload3 = makeJsonPayload(irfr_r, [truck_name], product_type, 
                               BASE_SUBLOCATIONS_MAP[from_sub]["LOCATION_ID"],
                               BASE_SUBLOCATIONS_MAP[to_sub]["LOCATION_ID"],
                               BASE_SUBLOCATIONS_MAP[from_sub]["ID"],
                               BASE_SUBLOCATIONS_MAP[to_sub]["ID"],
                               price_unit, price_type,
                              'Setting Price using IRFR', 'IRFR')
    ### Now save this prediction into Server by HTTP Put
    putPredictValue(SET_AUTOPRICE_API_URL, payload1)
    putPredictValue(SET_AUTOPRICE_API_URL, payload2)
    putPredictValue(SET_AUTOPRICE_API_URL, payload3)
    return (arima_r, rfr_r, irfr_r)

## Main function to call for prediction
def makePredictionAndSave_City(kpi, from_city, to_city, truck_names, product_type, irfrVals):    
    # Unit of Price
    price_unit='TON'
    # Price TYPE ? .. have to ask Hrishi/Shashank :X
    price_type='SP'

    # Let's filter orders based on our choices
    df = kpi
    # FilterBy From City, To City, Product    
    df = df[(df['From City'] == from_city)&(df['To City'] == to_city)&(df['Product Name'] == product_type)]
    # Filter By Trucks
    trucks = False
    for truck_name in truck_names:
        trucks = trucks|(df["Used truck Type"] == truck_name) 
    df = df[trucks]

    key = "%s:%s:%s:%s" %(from_city, to_city, truck_names[0], product_type)
    tommorrow = str((dt.date.today() + dt.timedelta(days=1)))
    (arima_r, rfr_r, irfr_r) = getNextDayPrices(df, key, irfrVals)    

    print "FROM_CITY, TO_CITY, TRUCK_NAME, PRODUCT_TYPE, ARIMA_PRICE, RFR_PRICE, IRFR_PRICE"
    print "%s, %s, %s, %s, %s, %s, %s, %s" %(tommorrow, from_city, to_city, truck_name, product_type, arima_r, rfr_r, irfr_r)
    
    # make the json payload
    payload1 = makeJsonPayload(rfr_r, [truck_name], product_type, 
                               BASE_LOCATIONS_MAP[from_city]["LOCATION_ID"],
                               BASE_LOCATIONS_MAP[to_city]["LOCATION_ID"],
                               None, None, price_unit, price_type,
                              'Setting Price using RFR', 'RFR')
    payload2 = makeJsonPayload(arima_r, [truck_name], product_type, 
                               BASE_LOCATIONS_MAP[from_city]["LOCATION_ID"],
                               BASE_LOCATIONS_MAP[to_city]["LOCATION_ID"],
                               None, None, price_unit, price_type,
                              'Setting Price using ARIMA', 'ARIMA')
    payload3 = makeJsonPayload(irfr_r, [truck_name], product_type, 
                               BASE_LOCATIONS_MAP[from_city]["LOCATION_ID"],
                               BASE_LOCATIONS_MAP[to_city]["LOCATION_ID"],
                               None, None, price_unit, price_type,
                              'Setting Price using IRFR', 'IRFR')
    ### Now save this prediction into Server by HTTP Put
    putPredictValue(SET_AUTOPRICE_API_URL, payload1)
    putPredictValue(SET_AUTOPRICE_API_URL, payload2)
    putPredictValue(SET_AUTOPRICE_API_URL, payload3)
    return (arima_r, rfr_r, irfr_r)


    
def letsDoIt(date="09/28/2017"):
    ## PARAMS
    PARAM_TUPS = [
        { "from_sub" : 'Kandla', "to_sub" : 'Pathankot', "truck_name" : '27T/28T', "product_type" : 'Coal' },
        { "from_sub" : 'Kandla', "to_sub" : 'Pathankot', "truck_name" : '30 MT', "product_type" : 'Coal' },
        { "from_sub" : 'Kandla', "to_sub" : 'Dabok', "truck_name" : '30 MT', "product_type" : 'Coal' },
        { "from_sub" : 'Kandla', "to_sub" : 'Dholka', "truck_name" : '30 MT', "product_type" : 'Coal' },
        { "from_sub" : 'Kandla', "to_sub" : 'Modinagar', "truck_name" : '27T/28T', "product_type" : 'Oil' },        
        { "from_sub" : 'Kandla', "to_sub" : 'Ghaziabad', "truck_name" : '27T/28T', "product_type" : 'Oil' },        
        { "from_sub" : 'Kandla', "to_sub" : 'Ludhiana', "truck_name" : '27T/28T', "product_type" : 'Oil' },        
        { "from_sub" : 'Kandla', "to_sub" : 'Muzaffarnagar', "truck_name" : '34T', "product_type" : 'Coal' },        
        { "from_sub" : 'Kandla', "to_sub" : 'Muzaffarnagar', "truck_name" : '34T', "product_type" : 'Petroleum Coke' },
        { "from_sub" : 'Kandla', "to_sub" : 'Karnal', "truck_name" : '27T/28T', "product_type" : 'Oil' },        
        { "from_sub" : 'Kandla', "to_sub" : 'Agra', "truck_name" : '27T/28T', "product_type" : 'Oil' },        
        { "from_sub" : 'Kandla', "to_sub" : 'Pinglaj', "truck_name" : '27T/28T', "product_type" : 'Sugar' },        
        { "from_sub" : 'Kandla', "to_sub" : 'Rabariyawas', "truck_name" : '30 MT', "product_type" : 'Coal' },        
        { "from_sub" : 'Kandla', "to_sub" : 'Rabariyawas', "truck_name" : '34T', "product_type" : 'Coal' },            
        { "from_sub" : 'Kandla', "to_sub" : 'Madhopur', "truck_name" : '30 MT', "product_type" : 'Coal' },        
        { "from_sub" : 'Kandla', "to_sub" : 'Madhopur', "truck_name" : '27T/28T', "product_type" : 'Coal' },        
        { "from_sub" : 'Kandla', "to_sub" : 'Narolgam', "truck_name" : '30 MT', "product_type" : 'Coal' },        
        { "from_sub" : 'Kandla', "to_sub" : 'Ambala', "truck_name" : '27T/28T', "product_type" : 'Coal' },        
        { "from_sub" : 'Kandla', "to_sub" : 'Ambala', "truck_name" : '30 MT', "product_type" : 'Coal' },        
        { "from_sub" : 'Kandla', "to_sub" : 'Ambala', "truck_name" : '34T', "product_type" : 'Coal' },          
        { "from_sub" : 'Kandla', "to_sub" : 'Himmatnagar', "truck_name" : '30 MT', "product_type" : 'Coal' },    
        { "from_sub" : 'Kandla', "to_sub" : 'Meerut', "truck_name" : '34T', "product_type" : 'Coal' },    
        { "from_sub" : 'Kandla', "to_sub" : 'Palanpur', "truck_name" : '30 MT', "product_type" : 'Coal' },    
        { "from_sub" : 'Kandla', "to_sub" : 'Shahjahanpur', "truck_name" : '30 MT', "product_type" : 'Coal' },    
        { "from_sub" : 'Kandla', "to_sub" : 'Shahjahanpur', "truck_name" : '27T/28T', "product_type" : 'Coal' },    
        { "from_sub" : 'Kandla', "to_sub" : 'Modinagar', "truck_name" : '27T/28T', "product_type" : 'Oil' },    
        { "from_sub" : 'Kandla', "to_sub" : 'Roorkee', "truck_name" : '34T', "product_type" : 'Coal' },    

        { "from_sub" : 'Tuna', "to_sub" : 'Pathankot', "truck_name" : '30 MT', "product_type" : 'Coal' },    

        { "from_sub" : 'Gandhidham', "to_sub" : 'Panipat', "truck_name" : '27T/28T', "product_type" : 'Textiles' },        

        { "from_sub" : 'Lilashah Nagar', "to_sub" : 'Dholpur House', "truck_name" : '34T', "product_type" : 'Oil' },
        { "from_sub" : 'Lilashah Nagar', "to_sub" : 'Dholpur House', "truck_name" : '27T/28T', "product_type" : 'Oil' },
        { "from_sub" : 'Lilashah Nagar', "to_sub" : 'Ambala', "truck_name" : '27T/28T', "product_type" : 'Oil' },        
        { "from_sub" : 'Lilashah Nagar', "to_sub" : 'Ambala', "truck_name" : '30 MT', "product_type" : 'Oil' },        
        { "from_sub" : 'Lilashah Nagar', "to_sub" : 'Ghaziabad', "truck_name" : '27T/28T', "product_type" : 'Oil' },        
        { "from_sub" : 'Lilashah Nagar', "to_sub" : 'Kashmere Gate', "truck_name" : '27T/28T', "product_type" : 'Oil' },
        { "from_sub" : 'Lilashah Nagar', "to_sub" : 'Agra', "truck_name" : '27T/28T', "product_type" : 'Oil' },

        { "from_sub" : 'Anjar', "to_sub" : 'Agra', "truck_name" : '21T/20T', "product_type" : 'Oil' },
        { "from_sub" : 'Anjar', "to_sub" : 'Muzaffarnagar', "truck_name" : '34T', "product_type" : 'Coal' },

        { "from_sub" : 'Mundra', "to_sub" : 'Pathankot', "truck_name" : '30 MT', "product_type" : 'Coal' },
        { "from_sub" : 'Mundra', "to_sub" : 'Muzaffarnagar', "truck_name" : '34T', "product_type" : 'Coal' },
    ]

    PARAM_TUPS3 = [
        { "from_city" : 'Anjar', "to_city" : 'Pathankot', "truck_name" : ['27T/28T', '30 MT','34T'], 
          "product_type" : 'Coal' },
        { "from_city" : 'Anjar', "to_city" : 'Agra', "truck_name" : ['27T/28T', '30 MT','34T'], 
          "product_type" : 'Oil' },
        { "from_city" : 'Anjar', "to_city" : 'Agra', "truck_name" : ['21T/20T'], 
          "product_type" : 'Oil' },
        { "from_city" : 'Anjar', "to_city" : 'Muzaffarnagar', "truck_name" : ['27T/28T', '30 MT','34T'], 
          "product_type" : 'Coal' },
        { "from_city" : 'Anjar', "to_city" : 'Muzaffarnagar', "truck_name" : ['27T/28T', '30 MT','34T'], 
          "product_type" : 'Petroleum Coke' },
        { "from_city" : 'Anjar', "to_city" : 'Ahmedabad', "truck_name" : ['27T/28T', '30 MT','34T'], 
          "product_type" : 'Coal' },
        { "from_city" : 'Anjar', "to_city" : 'Matar', "truck_name" : ['27T/28T', '30 MT','34T'], 
          "product_type" : 'Sugar' },
        { "from_city" : 'Anjar', "to_city" : 'Ambala', "truck_name" : ['27T/28T', '30 MT','34T'], 
          "product_type" : 'Coal' },
        { "from_city" : 'Anjar', "to_city" : 'Ambala', "truck_name" : ['27T/28T', '30 MT','34T'], 
          "product_type" : 'Oil' },
        { "from_city" : 'Anjar', "to_city" : 'Himmatnagar', "truck_name" : ['27T/28T', '30 MT','34T'], 
          "product_type" : 'Coal' },
        { "from_city" : 'Anjar', "to_city" : 'Meerut', "truck_name" : ['27T/28T', '30 MT','34T'], 
          "product_type" : 'Coal' },
        { "from_city" : 'Anjar', "to_city" : 'Meerut', "truck_name" : ['27T/28T', '30 MT','34T'], 
          "product_type" : 'Oil' },
        { "from_city" : 'Anjar', "to_city" : 'Roorkee', "truck_name" : ['27T/28T', '30 MT','34T'], 
          "product_type" : 'Coal' },
        { "from_city" : 'Anjar', "to_city" : 'Vadgam', "truck_name" : ['27T/28T', '30 MT','34T'], 
          "product_type" : 'Coal' },
        { "from_city" : 'Anjar', "to_city" : 'Shahjahanpur', "truck_name" : ['27T/28T', '30 MT','34T'], 
          "product_type" : 'Coal' },
        { "from_city" : 'Mundra', "to_city" : 'Pathankot', "truck_name" : ['27T/28T', '30 MT','34T'],
          "product_type" : 'Coal' },
        { "from_city" : 'Mundra', "to_city" : 'Roorkee', "truck_name" : ['27T/28T', '30 MT','34T'],
          "product_type" : 'Coal' },
        { "from_city" : 'Mundra', "to_city" : 'Muzaffarnagar', "truck_name" : ['27T/28T', '30 MT','34T'],
          "product_type" : 'Coal' },
    ]
    
    
    groupBY1 = ['From City', 'To City', 'Product Name', 'Used truck Type']
    keys1 = ['From City', 'To City', 'Product Name', 'Used truck Type', 
            'd1', 'd2', 'd3', 'd4', 'd5', 'd6', 'd7',
            'pd1', 'pd2', 'pd3', 'pd4', 'pd5', 'pd6', 'pd7']
    truck_types_to_club = ['27T/28T', '30 MT','34T']

    groupBY2 = ['From Sublocation', 'To Sublocation', 'Product Name', 'Used truck Type']
    keys2 = ['From Sublocation', 'To Sublocation', 'Product Name', 'Used truck Type', 
            'd1', 'd2', 'd3', 'd4', 'd5', 'd6', 'd7',
            'pd1', 'pd2', 'pd3', 'pd4', 'pd5', 'pd6', 'pd7']

    # first get the orders reports
    getfile = getDataFromES(ES_QUERY)
    kpi = getDataFrame(getfile)
    #kpi = getDataFrame(LOCAL_FILE)
    kpi = kpi[(kpi['date'] >= parseDate("06/01/2017"))]
    
    ## RUN IRFR
    irfrVals = runIRFRForDF(kpi, groupBY2, keys2, [],  W=7, T=1)
    irfrVals_City = runIRFRForDF(kpi, groupBY1, keys1, truck_types_to_club,  W=7, T=1)

    for p in PARAM_TUPS:
        (arima, rfr, irfr) = makePredictionAndSave(kpi, p["from_sub"], p["to_sub"], p["truck_name"], p["product_type"], irfrVals)
        p["arima"] = arima
        p["rfr"] = rfr
        p["irfr"] = irfr
        p["avg"] = (rfr + arima + irfr)/3.0
        print "SUBLOC@=> %s, %s, %s, %s, %s, %s, %s" %(p["from_sub"], p["to_sub"], p["truck_name"], p["product_type"], arima, rfr, irfr)
    

    for p in PARAM_TUPS3:
        (arima, rfr, irfr) = makePredictionAndSave_City(kpi, p["from_city"], p["to_city"], p["truck_name"], p["product_type"], irfrVals_City)
        p["arima"] = arima
        p["rfr"] = rfr
        p["irfr"] = irfr
        p["avg"] = (rfr + arima + irfr)/3.0
        print "CITY@=> %s, %s, %s, %s, %s, %s, %s" %(p["from_city"], p["to_city"], p["truck_name"], p["product_type"], arima, rfr, irfr) 
    
    print "======================  Setting UP LANE Price CITY-2-CITY ================================="
    tt_to_set = ["27T/28T", "30 MT", "34T"] 
    
    for i in [0, 1, 2, -3]:
    #for p in PARAM_TUPS3:
        p = PARAM_TUPS3[i]
        ## SET SP PRICE
        payload = makeJsonPayload(round(p["avg"], -1), p["truck_name"], p["product_type"], 
                          BASE_LOCATIONS_MAP[p["from_city"]]["LOCATION_ID"],
                          BASE_LOCATIONS_MAP[p["to_city"]]["LOCATION_ID"],
                          None, None, 'TON', 'SP', 
                          'Setting Price using AVG(ARIMA, RFR, IRFR) %s' %p["truck_name"])        
        print 'SP=>%s, %s, %s, %s, %s, %s, %s, "%s"' %(p["from_city"], p["to_city"], p["truck_name"], p["product_type"], round(p["avg"], -1), 
                                     'TON', 'SP', 'Setting Price using AVG(ARIMA, RFR, IRFR) %s' % p["truck_name"])
        ### Now set current price    
        setCurrentPrice(SET_CURRENTPRICE_API_URL, payload)
    
        ## SET CUSTOMER PRICE
        customer_price = round(p["avg"], -1)-DENT_RS
        payload = makeJsonPayload(customer_price, p["truck_name"], p["product_type"], 
                          BASE_LOCATIONS_MAP[p["from_city"]]["LOCATION_ID"],
                          BASE_LOCATIONS_MAP[p["to_city"]]["LOCATION_ID"],
                          None, None, 'TON', 'CUSTOMER', 
                          'Setting Price using AVG(ARIMA, RFR, IRFR) %s' %p["truck_name"])        
        print 'CS=>%s, %s, %s, %s, %s, %s, %s, "%s"' %(p["from_city"], p["to_city"], p["truck_name"], p["product_type"], customer_price, 
                                     'TON', 'CUSTOMER', 'Setting Price using AVG(ARIMA, RFR, IRFR) %s' % p["truck_name"])
        setCurrentPrice(SET_CURRENTPRICE_API_URL, payload)
    
    
    with_new_lanes = [([0, 1], 0), ([10], 1), ([29, 30], 1), ([27], 0), ([14, 15], 0), ([35], 1),([38], 15), ([7], 3), 
              ([8], 4), ([37], 3), ([39], 17), ([16], 5), ([11], 6), ([22], 13), ([26], 12), ([23, 24], 14),
             ([21], 10), ([25], 11), ([20], 9), ([17, 18, 19], 7), ([31, 32], 8)]
    

    without_new_lanes = [([0, 1], 0), ([10], 1), ([29, 30], 1), ([27], 0), ([14, 15], 0), ([35], 1),([38], 15)]
    
    print "======================  Setting UP LANE Price SUB-2-SUB ================================="
    for x,loc in without_new_lanes:
        avg_price = 0.0
        tt_types = []
        for i in x:
            p = PARAM_TUPS[i]
            tt_types.append(p["truck_name"])
            avg_price = avg_price+p["avg"]
        avg_price = (avg_price*1.0)/len(x)
        loc_avg_prce = PARAM_TUPS3[loc]["avg"]
        perr = pererr(abs(avg_price-loc_avg_prce), loc_avg_prce) 
        avg_price = round(avg_price, -1)
        loc_avg_prce = round(loc_avg_prce, -1)
        if( perr >= 3.5):
            print "Error (%.2f) is greater than 3.5pc !! SETTING LOCATION price for SUBLOCATION PRICE" %perr
            print "%s, %s, %s, %s, %s, %s" %(p["from_sub"], p["to_sub"], p["truck_name"], p["product_type"], avg_price, loc_avg_prce)
            avg_price = loc_avg_prce
        p = PARAM_TUPS[x[0]]        

        ## SET SP PRICE
        payload = makeJsonPayload(avg_price, tt_to_set, p["product_type"], 
                           BASE_SUBLOCATIONS_MAP[p["from_sub"]]["LOCATION_ID"],
                           BASE_SUBLOCATIONS_MAP[p["to_sub"]]["LOCATION_ID"],
                           BASE_SUBLOCATIONS_MAP[p["from_sub"]]["ID"],
                           BASE_SUBLOCATIONS_MAP[p["to_sub"]]["ID"],
                           'TON', 'SP', 
                          'Setting Price using AVG(ARIMA, RFR, IRFR) %s' %tt_types)        
        print 'SP=>%s, %s, %s, %s, %s, %s, %s, (PERR=%.2f, LP=%s), "%s"' %(p["from_sub"], p["to_sub"], tt_to_set, p["product_type"], avg_price, 
                                     'TON', 'SP', perr, loc_avg_prce, 'Setting Price using AVG(ARIMA, RFR, IRFR) %s' %tt_types)
        ### Now set current price    
        setCurrentPrice(SET_CURRENTPRICE_API_URL, payload)

        ## SET CUSTOMER PRICE
        customer_price = avg_price-DENT_RS
        payload = makeJsonPayload(customer_price, tt_to_set, p["product_type"], 
                           BASE_SUBLOCATIONS_MAP[p["from_sub"]]["LOCATION_ID"],
                           BASE_SUBLOCATIONS_MAP[p["to_sub"]]["LOCATION_ID"],
                           BASE_SUBLOCATIONS_MAP[p["from_sub"]]["ID"],
                           BASE_SUBLOCATIONS_MAP[p["to_sub"]]["ID"],
                           'TON', 'CUSTOMER', 
                          'Setting Price using AVG(ARIMA, RFR, IRFR) %s' %tt_types)        
        print 'CS=>%s, %s, %s, %s, %s, %s, %s, (PERR=%.2f, LP=%s), "%s"' %(p["from_sub"], p["to_sub"], tt_to_set, p["product_type"], customer_price, 
                                     'TON', 'CUSTOMER', perr, loc_avg_prce, 'Setting Price using AVG(ARIMA, RFR, IRFR) %s' %tt_types)
        ### Now set current price    
        setCurrentPrice(SET_CURRENTPRICE_API_URL, payload)
        
    for x,loc in [(36, 2)]:
        p = PARAM_TUPS[x]        
        avg_price = p["avg"]
        loc_avg_prce = PARAM_TUPS3[loc]["avg"]
        perr = pererr(abs(avg_price-loc_avg_prce), loc_avg_prce) 
        avg_price = round(avg_price, -1)
        loc_avg_prce = round(loc_avg_prce, -1)
        if( perr >= 3.5):
            print "Error (%.2f) is greater than 3.5pc !! SETTING LOCATION price for SUBLOCATION PRICE" %perr
            print "%s, %s, %s, %s, %s, %s" %(p["from_sub"], p["to_sub"], p["truck_name"], p["product_type"], avg_price, loc_avg_prce)
            avg_price = loc_avg_prce
            
        ## SET SP PRICE
        payload = makeJsonPayload(avg_price, [p["truck_name"]], p["product_type"], 
                           BASE_SUBLOCATIONS_MAP[p["from_sub"]]["LOCATION_ID"],
                           BASE_SUBLOCATIONS_MAP[p["to_sub"]]["LOCATION_ID"],
                           BASE_SUBLOCATIONS_MAP[p["from_sub"]]["ID"],
                           BASE_SUBLOCATIONS_MAP[p["to_sub"]]["ID"],
                           'TON', 'SP', 
                          'Setting Price using AVG(ARIMA, RFR, IRFR) %s' %[p["truck_name"]])        
        print 'SP=>%s, %s, %s, %s, %s, %s, %s, (PERR=%.2f, LP=%s),"%s"' %(p["from_sub"], p["to_sub"], [p["truck_name"]], p["product_type"], avg_price, 
                                     'TON', 'SP', perr, loc_avg_prce, 'Setting Price using AVG(ARIMA, RFR, IRFR) %s' %p["truck_name"])
        ### Now set current price    
        setCurrentPrice(SET_CURRENTPRICE_API_URL, payload)
        
        ## SET CUSTOMER PRICE
        customer_price = avg_price-DENT_RS
        payload = makeJsonPayload(customer_price, [p["truck_name"]], p["product_type"], 
                           BASE_SUBLOCATIONS_MAP[p["from_sub"]]["LOCATION_ID"],
                           BASE_SUBLOCATIONS_MAP[p["to_sub"]]["LOCATION_ID"],
                           BASE_SUBLOCATIONS_MAP[p["from_sub"]]["ID"],
                           BASE_SUBLOCATIONS_MAP[p["to_sub"]]["ID"],
                           'TON', 'CUSTOMER', 
                          'Setting Price using AVG(ARIMA, RFR, IRFR) %s' %[p["truck_name"]])        
        print 'CS=>%s, %s, %s, %s, %s, %s, %s, (PERR=%.2f, LP=%s), "%s"' %(p["from_sub"], p["to_sub"], [p["truck_name"]], p["product_type"], customer_price, 
                                     'TON', 'CUSTOMER', perr, loc_avg_prce, 'Setting Price using AVG(ARIMA, RFR, IRFR) %s' %p["truck_name"])
        ### Now set current price    
        setCurrentPrice(SET_CURRENTPRICE_API_URL, payload)

    return PARAM_TUPS

if __name__ == "__main__":
    letsDoIt()