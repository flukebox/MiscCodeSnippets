# Some code is shamelessly copied from Learning Blogs
%matplotlib inline
import numpy as np
import pandas as pd
from pandas import Series
from pandas import DataFrame
from pandas import concat
from statsmodels.tsa.arima_model import ARIMA
from statsmodels.tsa.seasonal import seasonal_decompose
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.ensemble import RandomForestRegressor
from sklearn.feature_extraction import DictVectorizer
from math import sqrt
import matplotlib.pyplot as plt
from sklearn.svm import SVR
from pylab import rcParams
import math
import zipfile
from datetime import datetime
from dateutil import parser
import datetime
import random
from functools import reduce
from sklearn.feature_extraction import DictVectorizer
from sklearn.ensemble import RandomForestRegressor


pd.set_option('display.max_columns', None)
rcParams['figure.figsize'] = 15, 5

def gt(x,y): 
    if x > y:
        return 1
    else:
        return 0
    
def le(x,y): 
    if x <= y:
        return 1
    else:
        return 0


def parseDate(d):
    try:
        return parser.parse(d)        
    except:
        return None

def maxD(v):
    dt1 = parseDate(v[0])
    dt2 = parseDate(v[1])
    if dt1 is not None and dt2 is not None:
        return max(dt1, dt2)
    elif dt1 is not None:
        return dt1
    else:
        return dt2
    
def addDelta(v):
    delta = datetime.timedelta(0,random.randint(1, 15), random.randint(1, 1001))
    dt = parseDate(str(v[0]))
    if dt is not None:
        return dt+delta
    else:
        return dt
    

    
def num(s):
    try:
        return int(s[0])
    except ValueError:
        return float(s[0])
    except ValueError:
        return None

def differ(v):
    d1 = num(v[0])
    d2 = num(v[1])
    if d1 is not None and d2 is not None and abs(d1-d2)<=10:
        return False
    else:
        return True
    
def pererr(err, avg):
    return (err/avg)*100

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

    
def predict(coef, history):
    pred = 0.0
    for i in range(1, len(coef)+1):
        pred += coef[i-1] * history[-i]
    return pred    

def calculateErrs(test, pred):
    errs = []
    for i in range(len(test)):
        err=pererr(abs(test[i]-pred[i]), test[i])
        errs.append(err)
    rmse = sqrt(mean_squared_error(test, pred))
    mae = mean_absolute_error(test, pred)
    print('Test RMSE: %.3f, MAE:%.3f, Err:%.3f'% (rmse, mae, mean(errs)))
    return errs
    
    
def errorRolls(errs):    
    counters = [np.array([le(x,2), le(x,5), le(x,7), le(x,10), le(x,15), gt(x,15)]) for x in errs]
    total = len(counters)*1.0
    errorRollUp = zip([2, 3, 5, 7, 10, 15, 15], reduce((lambda x,y : x+y), counters))
    return [(x[0], ((x[1]/total)*100)) for x in errorRollUp] 

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

def splitData(all_series_fna, keys, W=7, train_frac = 0.5):
    all_series_fna_featured = [(x[0], toWindowFeatures(x[1], 5)) for x in all_series_fna]    
    train_feat, train_labl = [], []
    test_feat, test_labl = [], []
    test_delta = []    
    for (x,(feat,lbl, delta)) in all_series_fna_featured:
        t_size = int(len(feat)*train_frac)    
        for (f, l) in zip(feat[:t_size], lbl[:t_size]):
            train_feat.append(dict(zip (keys, list(x)+f.tolist())))
            train_labl.append(l)
        for (f, l) in zip(feat[t_size:], lbl[t_size:]):
            test_feat.append(dict(zip (keys, list(x)+f.tolist())))
            test_labl.append(l)
        test_delta = test_delta + delta[t_size:]    
    return ((train_feat, train_labl), (test_feat, test_labl, test_delta))

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
