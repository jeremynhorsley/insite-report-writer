import json
import requests
import io
import pandas as pd
import numpy as np
import scipy as sp
import time
import csv
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta, MO
from calendar import monthrange
import matplotlib.pyplot as plt
from matplotlib import dates as mpl_dates
import seaborn as sns
#import plotly.graph_objects as go
#from plotly.subplots import make_subplots
#import plotly.express as px
#import chart_studio.tools as tls
import os

# DESCRIPTION
# Script interacts with Swagger api using the user's api token based on admin crendentials.
# Monthly data is downloaded into Pandas dataframes.
# Data is analyzed based on the pre-defined insite metrics.
# Text files are generated for each measurement point and exported into the current working directory.
 
# INSTRUCTIONS 
# 1. Copy and paste your key from the Swagger api in place of the encrypted section "zmY.....mjM" example below.
#        example api_token = 'Basic zmVyZo15bi5ob3JzbGV5QHBvd2Vyc2lkZS5jb206a2l0dHlMaXR0ZXIxMjM='
# 2. Enter the dates for report. Currently this script looks at current month and previous month to compare some trends.
#        prev_start_time (previous month)
#        start_time (current month of desired report)
#        end_time (first day of the next month)
#        report_month_yr (used for document titles)
# 3. Verify the desired measurement points in the mps list. 
# 4. Run this script from Anaconda prompt for best results.  Most libraries are built into Anaconda.


# 1. API login credentials and base url
api_token = ''
api_url_base = 'https://www.admin.cloud.powerside.com/v1/'

# 2. User entered dates for analysis
#    **Be aware when crossing over a daylight savings time month that the time zone noted in the masurement points list (mps) may not be correct. 
prev_start_time = "2021-04-01"
start_time = "2021-05-01"
end_time = "2021-06-01"
report_month_yr = "May 2021"


# Variables
newline = '\n' # used in text file output for new lines

# All trend variables below assign readable names to the firmware names based on the "20200525 Channels Definition.xlsx" file
L1_curr_avg = "c_16_avg_a"
L2_curr_avg = "c_17_avg_a"
L3_curr_avg = "c_18_avg_a"
N_curr_avg = "c_19_avg_a"
gnd_curr_avg = "c_20_avg_a"
tot_app_pwr_avg = "c_106_avg_va"
tot_activ_pwr_avg = "c_70_avg_w"
tot_react_pwr_avg = "c_88_avg_var"
tot_pf_avg = "c_124_avg_none"
L1_v_avg = "c_4_avg_v"
L2_v_avg = "c_5_avg_v"
L3_v_avg = "c_6_avg_v"
tot_Pst_avg = "c_1612_avg_none"
thd_avg = "c_1609_avg_%"
tdd_avg = "c_1610_avg_%"
neg_v_unbal = "c_287_avg_%"
neg_i_unbal = "c_288_avg_%"

# Some sites had issues with too many columns in the dataframe and returned a 504 code. 
# For this reason, two of the least useful metrics were commented out for now.
# When changing the trend_list, it is imporant to also change the correlating trend_names.
#  - _list is used for accessing data, and _names is used for column headers.    
trend_list = [ 
    L1_curr_avg,
    L2_curr_avg,
    L3_curr_avg,
    gnd_curr_avg, 
    tot_activ_pwr_avg, 
    #tot_app_pwr_avg, 
    #tot_react_pwr_avg, 
    tot_pf_avg, 
    tot_Pst_avg, 
    thd_avg, 
    tdd_avg, 
    neg_i_unbal, 
    neg_v_unbal
    ]

trend_names = [
            "L1_curr_avg",
            "L2_curr_avg",
            "L3_curr_avg",
            "gnd_curr_avg", 
            "tot_activ_pwr_avg", 
            #"tot_app_pwr_avg", 
            #"tot_react_pwr_avg", 
            "tot_pf_avg", 
            "tot_Pst_avg", 
            "thd_avg", 
            "tdd_avg", 
            "neg_i_unbal", 
            "neg_v_unbal"
            ]
        


# 3. User entered measurement points from InSite
#       Format is "measurement point id": "UTC time zone offset" 
#          Example: Pinnacle is PDT time zone (+7 hours).
#       ** Time zone offsets will need manually adjusted depending on daylight savings time.
#       TODO: Detect daylight savings time automatically. 
mps = {
    # "15": "7",  #Pinnacle - PDT
    # "21": "6",  #Energy Txfr Lea
    # "766": "5", #Energy Txfr Perry
    # "13": "4",  #UE Enclosure
    # "17": "5",  #Myers CDT
    # "18": "5",  #Southwire CDT
    # "20": "4",  #PS Montreal 
    # "16": "6",  #Canfor
    # "826": "5", #Showplace Cab
    # "7": "4",   #Rada
    # "2167": "7",#Ready Roast North
    "2168": "7" #Ready Roast South
    }
 
# API HEADERS 
get_headers = {
    'accept': 'application/json',
    'authorization': api_token,
    }

post_headers = {
            'accept': 'text/csv',
            'authorization': api_token,
            'Content-Type': 'application/json',
            }

# FUNCTION DEFINITIONS            
def get_mp(p):
    '''
    parameter p: 
    get the measurement point information
    {
        "mpId": "Rada Entrance - 01",
        "roomId": 7,
        "measurementPointTypeId": 2,
        "measurementPointStatusId": 8,
        "commissionedWhen": "2019-11-22T22:46:52.000Z",
        "crmCode": null,
        "notes": "",
        "accountId": 5,
        "city": "Delson",
        "country": "Canada",
        "timezone": "America/Toronto",
        "accountName": "Rada Industries",
        "measurementPointTypeName": "In-Site",
        "measurementPointStatusName": "commissioned"
    }
    '''
    api_url = '{0}measurementPoint/{1}'.format(api_url_base, p)

    response = requests.get(api_url, headers=get_headers)

    if response.status_code == 200:
        return json.loads(response.content.decode('utf-8'))
    else:
        return None

def get_month_days(t):
            mo = datetime.fromisoformat(t[:-1]).month
            yr = datetime.fromisoformat(t[:-1]).month
            x, y = monthrange(yr, mo)
            return y

def post_trend_data(j, t, c):

    api_url = '{0}trends/measurementPoint/{1}'.format(api_url_base, measurementPointId)

    response = requests.post(api_url, headers=post_headers, json=j)

    if response.status_code == 200:
        r_text = response.text
        df = pd.read_csv(io.StringIO(r_text))
        col = ["date_time"] + c
        df.columns = col
        df.date_time = pd.to_datetime(df.date_time)
        df = df.set_index("date_time")
        df = df.tz_convert(tz = t) #acct_tz
        df = df.reset_index()
        return df
    else:
        print("post_trend_data API had no response - ", response.status_code)
        return None

def get_energy_data(p):
    '''
    {
      "status": 2,
      "totalActiveEnergyConsumed": 93728.8125,
      "totalApparentEnergyConsumed": 96521.25,
      "totalReactiveEnergyConsumed": 2259.7339999999967,
      "maxActivePowerDemand": 291832.8933333333,
      "minActivePowerDemand": 34082.69666666666,
      "avgActivePowerDemand": 125.97958669354838,
      "maxApparentPowerDemand": 129.7328629032258,
      "powerFactorAtMaxDemand": 0.972,
      "avgPowerFactor": 0.9710691946074052,
      "avgLoadFactor": 43.16839861833318,
      "samples": 2976,
      "dateTimeOfMaxActivePowerDemand": "2020-08-07T17:34:00.000Z"
    }
    '''

    api_url = '{0}energy/measurementPoint/{1}'.format(api_url_base, measurementPointId)

    response = requests.get(api_url, headers=get_headers, params=p)

    if response.status_code == 200:
        return json.loads(response.content.decode('utf-8'))
    else:
        return None

def get_pq_meausres(p):
    '''
        {
          "sagsAndSwellsPrior30Days": {
            "powerQualityStatusType": 2,
            "value": {
              "total": 3,
              "severe": 0
            }
          },
          "highfrequencyImpulsesPrior30Days": {
            "powerQualityStatusType": 2,
            "value": {
              "total": 0,
              "severe": 0
            }
          },
          "outagesPrior30Days": {
            "powerQualityStatusType": 2,
            "value": {
              "total": 0,
              "severe": 0
            }
          },
          "voltageFluctuationsPrior30Days": {
            "powerQualityStatusType": 2,
            "value": {
              "reportedConfiguration": "AUTO",
              "wiringConfiguration": "Wye",
              "nominalPhaseToPhaseVoltage": "600",
              "nominalPhaseToNeutralVoltage": "347",
              "Un": "347",
              "P95_RMS": 356.1,
              "P05_RMS": 345.6,
              "P95_PST": 0.36,
              "Num_SevereRVC_events": 0,
              "samplesThisPeriod": 43178
            }
          },
          "harmonicsPrior30Days": {
            "powerQualityStatusType": 2,
            "value": {
              "P95_THD": 1.69,
              "P95_TDD": 4.34,
              "samplesThisPeriod": 43178
            }
          },
          "imbalancePrior30Days": {
            "powerQualityStatusType": 2,
            "value": {
              "P95_V_UNB": 0.75,
              "P95_I_UNB": 28.31,
              "samplesThisPeriod": 43178
            }
          },
          "powerFactorPrior30Days": {
            "powerQualityStatusType": 1,
            "value": {
              "coverage": 0.9994907407407407,
              "count": 1141,
              "samplesThisPeriod": 43178
            }
          },
          "consumptionPrior30Days": {
            "powerQualityStatusType": 2,
            "value": {
              "startDate": "2020-09-08T04:00:00.000Z",
              "endDate": "2020-10-08T03:59:00.000Z",
              "seconds": 2591940,
              "consumption": 117433.8125,
              "samplesThisPeriod": 43178
            }
          },
          "groundCurrentPrior30Days": {
            "powerQualityStatusType": 1,
            "value": 0.2726
          },
          "wastePrior30Days": {
            "powerQualityStatusType": 2,
            "value": 5169.663427168505
          },
          "lastPowerConsumption": {
            "powerQualityStatusType": 2,
            "value": 267013
          },
          "power": {
            "powerQualityStatusType": 1,
            "value": {
              "samplesThisPeriod": 43178,
              "samplesPreviousPeriod": 43200,
              "samplesLastYearPeriod": 0,
              "energyThisPeriod": 117433.8125,
              "energyPreviousPeriod": 91081.125,
              "energyLastYear": 0
            }
          },
          "lastTemperature": {
            "powerQualityStatusType": 2,
            "value": {
              "lastTemperatureSampleTime": "2020-10-08T13:10:00.203Z",
              "sampleCountOverThreshold": 0,
              "lastTemperature": "58.56"
            }
          },
          "lastCommunication": {
            "powerQualityStatusType": 2,
            "value": "2020-10-08T13:10:00.000Z"
          },
          "sagsPrior30Days": {
            "powerQualityStatusType": 2,
            "value": {
              "total": 2,
              "severe": 0
            }
          },
          "swellsPrior30Days": {
            "powerQualityStatusType": 2,
            "value": {
              "total": 1,
              "severe": 0
            }
          },
          "sagsMonthToDate": {
            "powerQualityStatusType": 2,
            "value": {
              "total": 1,
              "severe": 0
            }
          },
          "swellsMonthToDate": {
            "powerQualityStatusType": 2,
            "value": {
              "total": 1,
              "severe": 0
            }
          },
          "highfrequencyImpulsesMonthToDate": {
            "powerQualityStatusType": 2,
            "value": {
              "total": 0,
              "severe": 0
            }
          },
          "outagesMonthToDate": {
            "powerQualityStatusType": 2,
            "value": {
              "total": 0,
              "severe": 0
            }
          },
          "voltageFluctuationsMonthToDate": {
            "powerQualityStatusType": 3,
            "value": {
              "samplesThisPeriod": 10080
            }
          },
          "harmonicsMonthToDate": {
            "powerQualityStatusType": 3,
            "value": {
              "samplesThisPeriod": 10080
            }
          },
          "imbalanceMonthToDate": {
            "powerQualityStatusType": 3,
            "value": {
              "samplesThisPeriod": 10080
            }
          },
          "powerFactorMonthToDate": {
            "powerQualityStatusType": 3,
            "value": {
              "samplesThisPeriod": 10080
            }
          },
          "consumptionMonthToDate": {
            "powerQualityStatusType": 2,
            "value": {
              "startDate": "2020-10-01T04:00:00.000Z",
              "endDate": "2020-10-08T03:59:00.000Z",
              "seconds": 604740,
              "consumption": 30929.375,
              "samplesThisPeriod": 10080
            }
          },
          "groundCurrentMonthToDate": {
            "powerQualityStatusType": 2,
            "value": 0.0939
          },
          "wasteMonthToDate": {
            "powerQualityStatusType": 2,
            "value": 1661.2472771113498
          },
          "powerMonthToDate": {
            "powerQualityStatusType": 3,
            "value": {
              "samplesThisPeriod": 10080,
              "samplesPreviousPeriod": 10080,
              "samplesLastYearPeriod": 0,
              "energyThisPeriod": 30929.375,
              "energyPreviousPeriod": 20520.875,
              "energyLastYear": 0,
              "samplesThisWeek": 10080,
              "samplesPreviousWeek": 0
            }
          }
        }
    '''
    api_url = '{0}powerQualityMeasures/measurementPoint/{1}'.format(api_url_base, measurementPointId)

    response = requests.get(api_url, headers=get_headers, params=p)

    if response.status_code == 200:
        return json.loads(response.content.decode('utf-8'))
    else:
        return None

def get_params(p):
    '''
        {
          "measurementPointId": "2167",
          "content": {
            "ratedCurrent": {
              "defaultValue": 3000
            },
            "contractedPower": {
              "defaultValue": 1000
            },
            "maxPowerDemandThreshold": {
              "defaultValue": 1000
            },
            "nominalPhaseToNeutralVoltage": {
              "defaultValue": 2400,
              "value": "277"
            },
            "nominalFrequency": {
              "defaultValue": 60
            },
            "powerFactorThreshold": {
              "defaultValue": 0.9
            },
            "powerConfiguration": {
              "defaultValue": "DELTA",
              "value": "Delta"
            },
            "groundCurrentThreshold": {
              "defaultValue": 1
            },
            "nominalPhaseToPhaseVoltage": {
              "defaultValue": 4160,
              "value": "480"
            }
          }
        }
    '''
    api_url = '{0}parameters/{1}'.format(api_url_base, p)

    response = requests.get(api_url, headers=get_headers)

    if response.status_code == 200:
        return json.loads(response.content.decode('utf-8'))
    else:
        return None              

if __name__ == '__main__':
    
    for num, tz in mps.items():
        mp_info = get_mp(num)
        acct_tz = mp_info['timezone']
        pr_s_t = prev_start_time + "T0" + tz + ":00:00.000Z"
        s_t = start_time + "T0" + tz + ":00:00.000Z"
        e_t = end_time + "T0" + tz + ":00:00.000Z"
        measurementPointId = num
        acct_a_name = mp_info['accountName']
        acct_b_name = mp_info['mpId']
        acct_name = acct_a_name+acct_b_name
        
# All of the below datetime manipulation is for formatting time,days,months,timespan for various parts of script. 
        previous_month_days = get_month_days(pr_s_t)    
        p_time = datetime.fromisoformat(pr_s_t[:-1])
        e_time = datetime.fromisoformat(e_t[:-1])
        pe_time = datetime.fromisoformat(s_t[:-1])
        pe_str = pe_time.strftime('%Y-%m-%dT%H:%M:%S')
        s_time = datetime.fromisoformat(s_t[:-1])
        ps_time = s_time - timedelta(days=previous_month_days)
        ps_str = ps_time.strftime('%Y-%m-%dT%H:%M:%S')
        report_timespan = e_time - s_time
        prev_report_timespan = s_time - p_time

        trend_json = {
            "startTime": s_t, 
            "endTime": e_t, 
            "table": "oneminute", 
            "interval": 1, 
            "period": "minute", 
            "output": "csv", 
            "writeToFile": False, 
            "columns": trend_list
            }

        prev_trend_json = {
            "startTime": pr_s_t, 
            "endTime": s_t, 
            "table": "oneminute", 
            "interval": 1, 
            "period": "minute", 
            "output": "csv", 
            "writeToFile": False, 
            "columns": trend_list
            }

        volt_fluct_names = [
            "tot_Pst_avg",
            "L1_v_avg", 
            "L2_v_avg", 
            "L3_v_avg"
            ]

        volt_fluct_list = [
            tot_Pst_avg,
            L1_v_avg, 
            L2_v_avg, 
            L3_v_avg
            ]

        volt_fluct_json = {
            "startTime": s_t, 
            "endTime": e_t, 
            "table": "oneminute", 
            "interval": 1, 
            "period": "minute", 
            "output": "csv", 
            "writeToFile": False, 
            "columns": volt_fluct_list
            }

        prev_volt_fluct_json = {
            "startTime": pr_s_t, 
            "endTime": s_t, 
            "table": "oneminute", 
            "interval": 1, 
            "period": "minute", 
            "output": "csv", 
            "writeToFile": False, 
            "columns": volt_fluct_list
            }

        period_params = (
            ('dateRangeStart', s_t),
            ('dateRangeEnd', e_t),
            )
            
        prev_period_params = (
            ('dateRangeStart', ps_str),
            ('dateRangeEnd', pe_str),
            )
        
        pq_measures = get_pq_meausres(period_params)
        pq_params = get_params(measurementPointId)
        
        #power_config = pq_measures['voltageFluctuationsPrior30Days']['value']['wiringConfiguration']
        power_config_1 = pq_params['content']['powerConfiguration'].get('value')
        power_config_2 = pq_params['content']['powerConfiguration'].get('defaultValue')
        #nom_pp_voltage = float(pq_measures['voltageFluctuationsPrior30Days']['value']['nominalPhaseToPhaseVoltage'])
        nom_pp_voltage_1 = pq_params['content']['nominalPhaseToPhaseVoltage'].get('value')
        nom_pp_voltage_2 = pq_params['content']['nominalPhaseToPhaseVoltage'].get('defaultValue')
        nom_pn_voltage_1 = pq_params['content']['nominalPhaseToNeutralVoltage'].get('value')
        nom_pn_voltage_2 = pq_params['content']['nominalPhaseToNeutralVoltage'].get('defaultValue')
        
        if power_config_1:
            power_config = power_config_1
        else:
            power_config = power_config_2
            
        if nom_pn_voltage_1:
            nom_pn_voltage = float(nom_pn_voltage_1)
        else:
            nom_pn_voltage = float(nom_pn_voltage_2)
        
        if nom_pp_voltage_2:
            nom_pp_voltage = float(nom_pp_voltage_1)
        else:
            nom_pp_voltage = float(nom_pp_voltage_2)
            
        filename = f"{acct_name} - {report_month_yr}.txt"
        file = open(filename, "w")

        #print(json.dumps(pq_measures, indent=1))

        #######################################################################      
        # TODO: Look into why I needed to set, reset index to date_time in order for conversion to work
        trend_df = post_trend_data(trend_json, acct_tz, trend_names)
        prev_trend_df = post_trend_data(prev_trend_json, acct_tz, trend_names)
        #trend_df['date_time'] = pd.to_datetime(trend_df['date_time']).dt.strftime('%H:%M:%S')

        volt_fluct_df = post_trend_data(volt_fluct_json, acct_tz, volt_fluct_names)
        prev_volt_fluct_df = post_trend_data(prev_volt_fluct_json, acct_tz, volt_fluct_names)


        # Build differential dataframe columns to find increases in rates
        #print(trend_df)
        trend_df['pwr_diff'] = trend_df['tot_activ_pwr_avg'].diff()
        trend_df['pf_diff'] = trend_df['tot_pf_avg'].diff()
        trend_df['pst_diff'] = trend_df['tot_Pst_avg'].diff()
        trend_df['thd_diff'] = trend_df['thd_avg'].diff()
        trend_df['tdd_diff'] = trend_df['tdd_avg'].diff()
        trend_df['nv_diff'] = trend_df['neg_v_unbal'].diff()
        trend_df['ni_diff'] = trend_df['neg_i_unbal'].diff()
        trend_df['gnd_diff'] = trend_df['gnd_curr_avg'].diff()
        trend_df['weekday'] = trend_df['date_time'].dt.day_name()
        sun_mask = trend_df['weekday'] == 'Sunday'
        mon_mask = trend_df['weekday'] == 'Monday'
        tue_mask = trend_df['weekday'] == 'Tuesday'
        wed_mask = trend_df['weekday'] == 'Wednesday'
        thu_mask = trend_df['weekday'] == 'Thursday'
        fri_mask = trend_df['weekday'] == 'Friday'
        sat_mask = trend_df['weekday'] == 'Saturday'
        sun_df = trend_df[sun_mask]
        mon_df = trend_df[mon_mask]
        tue_df = trend_df[tue_mask]
        wed_df = trend_df[wed_mask]
        thu_df = trend_df[thu_mask]
        fri_df = trend_df[fri_mask]
        sat_df = trend_df[sat_mask]
        
        

        prev_trend_df['pwr_diff'] = prev_trend_df['tot_activ_pwr_avg'].diff()
        prev_trend_df['pf_diff'] = prev_trend_df['tot_pf_avg'].diff()
        prev_trend_df['pst_diff'] = prev_trend_df['tot_Pst_avg'].diff()
        prev_trend_df['thd_diff'] = prev_trend_df['thd_avg'].diff()
        prev_trend_df['tdd_diff'] = prev_trend_df['tdd_avg'].diff()
        prev_trend_df['nv_diff'] = prev_trend_df['neg_v_unbal'].diff()
        prev_trend_df['ni_diff'] = prev_trend_df['neg_i_unbal'].diff()
        prev_trend_df['gnd_diff'] = prev_trend_df['gnd_curr_avg'].diff()
        
        

        #######################################################################
        #  BUILD METRICS
        #  
        #######################################################################   

        # Power ###############################################################
        # This 30 day period energy use is > 15% compared to prev month
        # OR this 30 day period > 30% of prev year 30 day period
        energy_dict = get_energy_data(period_params)
        this_month_active_energy = energy_dict['totalActiveEnergyConsumed']
        last_month_active_energy = get_energy_data(prev_period_params)['totalActiveEnergyConsumed']
        #print(last_month_active_energy)
        chg = this_month_active_energy - last_month_active_energy
        perc_chg = abs(round(100 * chg / last_month_active_energy, 2))
        #print("percent change")
        #print(perc_chg, "percent change")
        if perc_chg >= 0 and perc_chg <= 15:
            pwr_recommend = "No action."
            pwr_state = "a minor increase "
        elif perc_chg < 0:
            pwr_state = "a reduction "
        else:
            pwr_recommend = "Investigate increase in energy consumption."
            pwr_state = "an excessive increase "



        # Power Factor ########################################################
        # TODO - Evaluate maximum kw  - PF values below 40% of maximum will not be counted
        # Below 0.9 more than 5 cumulated hrs over 30 days.
        pf_mask = (trend_df["tot_pf_avg"] < 0.9) #& (trend_df["L1_curr_avg"] > 40) & (trend_df["L2_curr_avg"] > 40) & (trend_df["L3_curr_avg"] > 40)
        pf_diff_result_df = trend_df[pf_mask]
        #print(pf_diff_result_df)
        this_month_pf_result_time = pd.to_timedelta(pf_diff_result_df["tot_pf_avg"].count(), unit='minutes')
        pf_time_percent = round(100*this_month_pf_result_time/report_timespan, 2)
        this_month_pf_result_avg = round(pf_diff_result_df["tot_pf_avg"].mean(), 2)
        this_month_pf_result_min = round(pf_diff_result_df["tot_pf_avg"].min(), 2)
        #this_month_var_result_avg = round(pf_diff_result_df["tot_react_pwr_avg"].mean(), 2)

        prev_pf_mask = (prev_trend_df["tot_pf_avg"] < 0.9) & (prev_trend_df["L1_curr_avg"] > 40) & (prev_trend_df["L2_curr_avg"] > 40) & (prev_trend_df["L3_curr_avg"] > 40)
        prev_pf_diff_result_df = prev_trend_df[prev_pf_mask]
        prev_month_pf_result_time = pd.to_timedelta(prev_pf_diff_result_df["tot_pf_avg"].count(), unit='minutes')
        prev_pf_time_percent = round(100*prev_month_pf_result_time/prev_report_timespan, 2)
        prev_month_pf_result_avg = round(prev_pf_diff_result_df["tot_pf_avg"].mean(), 2)
        prev_month_pf_result_min = round(prev_pf_diff_result_df["tot_pf_avg"].min(), 2)
        #prev_month_var_result_avg = round(prev_pf_diff_result_df["tot_react_pwr_avg"].mean(), 2)
        pf_change = round(pf_time_percent - prev_pf_time_percent, 2)
        if this_month_pf_result_time > pd.Timedelta(5,'h'):
            pf_state = "exceeds"
        else:
            pf_state = "is within tolerance of"

        if pf_change > 0:
            pf_recommend = "Investigate why power factor has degraded since previous month"
        else:
            pf_recommend = "No action."

        # Volt fluctuation ####################################################
        # 10min Pst > 1 for 95% of 30 day period.
        # OR 1min volt outside +/- 7% nom_pn_voltage more than 5% of 30 day period.
        # TODO look into taknig average variance of nom_pn_voltage as a metric to display.  
        #  Report would show Voltage fluctuation percentage to 347 L-N: max, min, avg 

        # Absolute value of voltage fluctuation (Percent of nominal phase to neutral Voltage).
        volt_fluct_df["L1_%_fluct"] = abs((1 - nom_pn_voltage / volt_fluct_df["L1_v_avg"])) * 100
        volt_fluct_df["L2_%_fluct"] = abs((1 - nom_pn_voltage / volt_fluct_df["L2_v_avg"])) * 100
        volt_fluct_df["L3_%_fluct"] = abs((1 - nom_pn_voltage / volt_fluct_df["L3_v_avg"])) * 100

        L1_fluct_avg = round(volt_fluct_df["L1_%_fluct"].mean(), 2)
        L2_fluct_avg = round(volt_fluct_df["L2_%_fluct"].mean(), 2)
        L3_fluct_avg = round(volt_fluct_df["L3_%_fluct"].mean(), 2)


        lower_fluct_thresh = nom_pn_voltage - nom_pn_voltage*0.07
        upper_fluct_thresh = nom_pn_voltage + nom_pn_voltage*0.07

        a_mask = (volt_fluct_df["L1_v_avg"] >  upper_fluct_thresh) | (volt_fluct_df["L1_v_avg"] <  lower_fluct_thresh)
        b_mask = (volt_fluct_df["L2_v_avg"] >  upper_fluct_thresh) | (volt_fluct_df["L2_v_avg"] <  lower_fluct_thresh)
        c_mask = (volt_fluct_df["L3_v_avg"] >  upper_fluct_thresh) | (volt_fluct_df["L3_v_avg"] <  lower_fluct_thresh)

        a_result_df = volt_fluct_df[a_mask]
        b_result_df = volt_fluct_df[b_mask]
        c_result_df = volt_fluct_df[c_mask]

        L1_fluct_time = pd.to_timedelta(a_result_df["L1_%_fluct"].count(), unit='minutes')
        L2_fluct_time = pd.to_timedelta(a_result_df["L2_%_fluct"].count(), unit='minutes')
        L3_fluct_time = pd.to_timedelta(a_result_df["L3_%_fluct"].count(), unit='minutes')

        L1_fluct_time_perc = round(100*L1_fluct_time/report_timespan, 2)
        L2_fluct_time_perc = round(100*L2_fluct_time/report_timespan, 2)
        L3_fluct_time_perc = round(100*L3_fluct_time/report_timespan, 2)

        pst_threshold = 1
        pst_mask = (volt_fluct_df["tot_Pst_avg"] >= pst_threshold)
        pst_result_df = volt_fluct_df[pst_mask]

        pst_time = pd.to_timedelta(pst_result_df["tot_Pst_avg"].count(), unit='minutes')
        pst_mask_perc = round(100* pst_time / report_timespan, 2)


        # Absolute value of voltage fluctuation (Percent of nominal phase to neutral Voltage).
        prev_volt_fluct_df["L1_%_fluct"] = abs((1 - nom_pn_voltage / prev_volt_fluct_df["L1_v_avg"])) * 100
        prev_volt_fluct_df["L2_%_fluct"] = abs((1 - nom_pn_voltage / prev_volt_fluct_df["L2_v_avg"])) * 100
        prev_volt_fluct_df["L3_%_fluct"] = abs((1 - nom_pn_voltage / prev_volt_fluct_df["L3_v_avg"])) * 100

        prev_L1_fluct_avg = round(prev_volt_fluct_df["L1_%_fluct"].mean(), 2)
        prev_L2_fluct_avg = round(prev_volt_fluct_df["L2_%_fluct"].mean(), 2)
        prev_L3_fluct_avg = round(prev_volt_fluct_df["L3_%_fluct"].mean(), 2)

        prev_a_mask = (prev_volt_fluct_df["L1_v_avg"] >  upper_fluct_thresh) | (prev_volt_fluct_df["L1_v_avg"] <  lower_fluct_thresh)
        prev_b_mask = (prev_volt_fluct_df["L2_v_avg"] >  upper_fluct_thresh) | (prev_volt_fluct_df["L2_v_avg"] <  lower_fluct_thresh)
        prev_c_mask = (prev_volt_fluct_df["L3_v_avg"] >  upper_fluct_thresh) | (prev_volt_fluct_df["L3_v_avg"] <  lower_fluct_thresh)

        prev_a_result_df = prev_volt_fluct_df[prev_a_mask]
        prev_b_result_df = prev_volt_fluct_df[prev_b_mask]
        prev_c_result_df = prev_volt_fluct_df[prev_c_mask]

        prev_L1_fluct_time = pd.to_timedelta(prev_a_result_df["L1_%_fluct"].count(), unit='minutes')
        prev_L2_fluct_time = pd.to_timedelta(prev_a_result_df["L2_%_fluct"].count(), unit='minutes')
        prev_L3_fluct_time = pd.to_timedelta(prev_a_result_df["L3_%_fluct"].count(), unit='minutes')

        prev_L1_fluct_time_perc = round(100*prev_L1_fluct_time/prev_report_timespan, 2)
        prev_L2_fluct_time_perc = round(100*prev_L2_fluct_time/prev_report_timespan, 2)
        prev_L3_fluct_time_perc = round(100*prev_L3_fluct_time/prev_report_timespan, 2)

        prev_pst_mask = (prev_volt_fluct_df["tot_Pst_avg"] >= pst_threshold)
        prev_pst_result_df = prev_volt_fluct_df[prev_pst_mask]

        prev_pst_time = pd.to_timedelta(prev_pst_result_df["tot_Pst_avg"].count(), unit='minutes')
        prev_pst_mask_perc = round(100* prev_pst_time / prev_report_timespan, 2)

        if (L1_fluct_time_perc < 5 or L2_fluct_time_perc < 5 or L3_fluct_time_perc < 5):
            vf_conclusion_string = (
                f"Voltage fluctuation remained within 7% of nominal voltage for more than 95% of the month."
                )
        else:
            vf_conclusion_string = (
                f"Voltage fluctuation exceeded 7% of nominal voltage for more than 5% of the month"
                )
            
        if pst_mask_perc > 95:
            pst_conclusion_string = (
                f"Short Term Flicker Perceptibility (Pst) values exceeded 1 for at least 95% of the month."
                )
        else:
            pst_conclusion_string = (
                f"Short Term Flicker Perceptibility (Pst) values remained under 1 for 95% of the month."
                )
        #print(pst_result_df)
        #print(a_result_df)
        #print(b_result_df)

        # Harmonics ###########################################################
        # 1min THD-v >5% for more than 5% of 30 day period
        # OR 1 min  current TDD >25% for more than 25% of the 30 day period 
        tdd_trend_max = trend_df["tdd_avg"].max()
        tdd_trend_avg = round(trend_df["tdd_avg"].mean(), 2)
        thd_trend_avg = round(trend_df["thd_avg"].mean(), 2)
        tdd_thresh = round(tdd_trend_max - tdd_trend_avg, 2)
        tdd_mask = (trend_df["tdd_avg"] >= 25)
        tdd_mask_df = trend_df[tdd_mask]
        tdd_mask_time = pd.to_timedelta(tdd_mask_df["tdd_avg"].count(), unit='minutes')
        tdd_mask_perc = round(100* tdd_mask_time / report_timespan, 2)

        thd_mask = (trend_df["thd_avg"] >= 5)
        thd_mask_df = trend_df[thd_mask]
        thd_mask_time = pd.to_timedelta(thd_mask_df["thd_avg"].count(), unit='minutes')
        thd_mask_perc = round(100* thd_mask_time / report_timespan, 2)
        #print("tdd thresh    ", tdd_thresh)

        prev_tdd_trend_max = prev_trend_df["tdd_avg"].max()
        prev_tdd_trend_avg = round(prev_trend_df["tdd_avg"].mean(), 2)
        prev_thd_trend_avg = round(prev_trend_df["thd_avg"].mean(), 2)
        prev_tdd_thresh = round(prev_tdd_trend_max - tdd_trend_avg, 2)
        prev_tdd_mask = (prev_trend_df["tdd_avg"] >= 25)
        prev_tdd_mask_df = prev_trend_df[prev_tdd_mask]
        prev_tdd_mask_time = pd.to_timedelta(prev_tdd_mask_df["tdd_avg"].count(), unit='minutes')
        prev_tdd_mask_perc = round(100* prev_tdd_mask_time / prev_report_timespan, 2)

        prev_thd_mask = (prev_trend_df["thd_avg"] >= 5)
        prev_thd_mask_df = prev_trend_df[prev_thd_mask]
        prev_thd_mask_time = pd.to_timedelta(prev_thd_mask_df["thd_avg"].count(), unit='minutes')
        prev_thd_mask_perc = round(100* prev_thd_mask_time / prev_report_timespan, 2)

        if thd_mask_perc > 5:
            thd_conclusion_string = (
                f"Total Harmonic Distortion (THD-V) values exceeded 5% for more than 5% of the month."
                
                )
        else:
            thd_conclusion_string = (
                f"Total Harmonic Distortion (THD-V) values remained under 5% for at least 95% of the month."
                
                )

        if tdd_mask_perc > 25:
            tdd_conclusion_string = (
                f"Total Demand Distortion (TDD) values exceeded 25% for more than 25% of the month."
                
                )
        else:
            tdd_conclusion_string = (
                f"TDD values remained under the defined tolerance of 25% for at least 75% of the month."
                )
                
                
        # Unbalance ###########################################################
        #  TODO:  Add a mask for currents less than 30 amps
        # Negative voltage unbalance is > 2% for more than 5% of the 30 day period 
        # OR Negative current unbalance is > 50% for more than 5% of the 30 day period 
        nvu_trend_avg = round(trend_df["neg_v_unbal"].mean(), 2)
        nvu_mask = (trend_df["neg_v_unbal"] >= 2)
        nvu_mask_df = trend_df[nvu_mask]
        nvu_mask_time = pd.to_timedelta(nvu_mask_df["neg_v_unbal"].count(), unit='minutes')
        nvu_mask_perc = round(100 * nvu_mask_time / report_timespan, 2)

        niu_trend_avg = round(trend_df["neg_i_unbal"].mean(), 2)
        niu_mask = (trend_df["neg_i_unbal"] >= 50)
        niu_mask_df = trend_df[niu_mask]
        niu_mask_time = pd.to_timedelta(niu_mask_df["neg_v_unbal"].count(), unit='minutes')
        niu_mask_perc = round(100 * niu_mask_time / report_timespan, 2)

        prev_nvu_trend_avg = round(prev_trend_df["neg_v_unbal"].mean(), 2)
        prev_nvu_mask = (prev_trend_df["neg_v_unbal"] >= 2)
        prev_nvu_mask_df = prev_trend_df[prev_nvu_mask]
        prev_nvu_mask_time = pd.to_timedelta(prev_nvu_mask_df["neg_v_unbal"].count(), unit='minutes')
        prev_nvu_mask_perc = round(100 * nvu_mask_time / prev_report_timespan, 2)

        prev_niu_trend_avg = round(prev_trend_df["neg_i_unbal"].mean(), 2)
        prev_niu_mask = (prev_trend_df["neg_i_unbal"] >= 50)
        prev_niu_mask_df = prev_trend_df[prev_niu_mask]
        prev_niu_mask_time = pd.to_timedelta(prev_niu_mask_df["neg_v_unbal"].count(), unit='minutes')
        prev_niu_mask_perc = round(100 * prev_niu_mask_time / prev_report_timespan, 2)

        if nvu_mask_perc > 5:
            nvu_conclusion_string = (
                f"Negative voltage unbalance exceeded 2% for more than 5% of the month."
                )
        else:
            nvu_conclusion_string = (
                f"Negative voltage unbalance remained within the defined tolerance of 2% for at least 95% of the month."
                )

        if niu_mask_perc > 5:
            niu_conclusion_string = (
                f"Negative current unbalance exceeded 50% for more than 5% of the month."
                )
        else:
            niu_conclusion_string = (
                f"Negative current unbalance remained within the defined tolerance of 50% for at least 95% of the month."
                )
                
        # Ground Current ######################################################
        # 1 min avg > 0.1 amps for 30 day period
        gnd_trend_avg = round(trend_df["gnd_curr_avg"].mean(), 2)
        gnd_trend_max = round(trend_df["gnd_curr_avg"].max(), 2)
        gnd_mask1 = (trend_df["gnd_curr_avg"] >= 0.1)
        gnd_mask1_df = trend_df[gnd_mask1]
        gnd_mask1_time = pd.to_timedelta(gnd_mask1_df["gnd_curr_avg"].count(), unit='minutes')
        gnd_mask1_perc = round(100* gnd_mask1_time / report_timespan, 2)

        gnd_mask = (trend_df["gnd_diff"] > 0.2) & (trend_df["gnd_curr_avg"] >= .1)
        gnd_diff_result_df = trend_df[gnd_mask]
        
        
        
        
        

        prev_gnd_trend_avg = round(prev_trend_df["gnd_curr_avg"].mean(), 2)
        prev_gnd_trend_max = round(prev_trend_df["gnd_curr_avg"].max(), 2)
        prev_gnd_mask1 = (prev_trend_df["gnd_curr_avg"] >= 0.1)
        prev_gnd_mask1_df = prev_trend_df[prev_gnd_mask1]
        prev_gnd_mask1_time = pd.to_timedelta(prev_gnd_mask1_df["gnd_curr_avg"].count(), unit='minutes')
        prev_gnd_mask1_perc = round(100* prev_gnd_mask1_time / prev_report_timespan, 2)

        prev_gnd_mask = (prev_trend_df["gnd_diff"] > 0.2) & (prev_trend_df["gnd_curr_avg"] >= .1)
        prev_gnd_diff_result_df = prev_trend_df[prev_gnd_mask]

        if gnd_mask1_perc > 0:
            gnd_conclusion_string = (
                f"Ground current exceeded 0.1 A during this 30-day period for an accumulated time of {gnd_mask1_time} with an average ground current reading of {gnd_trend_avg} A and the maximum reading of {gnd_trend_max} A."
                )
        else: 
            gnd_conclusion_string = (
                f"Ground current remained within the defined tolerance of 0.1 A during this 30-day period."
                )
                
        
        
        # print("Ground Current Events: \n", gnd_diff_result_df, "\n")

        #######################################################################
        #  BUILD REPORT STRINGS
        #  
        #######################################################################
        
        ##Your In-Site gateway provides six alarm indicators to help analyze and trend the quality of your facility’s power. It compares data for the current period to the data #from the previous period based on a rolling 30-day window. Below are findings for the month of ###MONTH VARIABLE. 
        
        report_header_string = (
            f"###################    Monthly report for {acct_name}  ##################"
            f"{newline}Nominal Phase to Neutral Voltage: {nom_pn_voltage} Volts"
            f"{newline}Nominal Phase to Phase Voltage: {nom_pp_voltage} Volts"
            f"{newline}Wiring Configuratoin: {power_config}"
            f"{newline}"
            f"{newline}+++ This Period +++"
            f"{newline}Start time: {s_t}"
            f"{newline}End time: {e_t}"
            f"{newline}Duration: {report_timespan}"
            f"{newline}"
            f"{newline}+++ Prev Period +++"
            f"{newline}Start time: {ps_str}Z"
            f"{newline}End time: {pe_str}Z"
            f"{newline}Duration: {pe_time - ps_time}"
            f"{newline}################################################################################ " 
            f"{newline}"
            f"{newline}"
            )


            
        pwr_report_string = (
            f"{newline}"
            f"{newline}"
            f"POWER"
            
            #f"{newline}Recommendation: {pwr_recommend}"
            f"{newline}This measurement point had {pwr_state}in power consumption of {perc_chg}% from the previous month."
            #f"{newline}This period energy consumption: {this_month_active_energy} kWh"
            #f"{newline}Prev period energy consumption: {last_month_active_energy} kWh"
            #f"{newline}Energy consumption change from prev period: {perc_chg} %"
            #TODO A reduction in power usage of {perc_chg} % compared to the previous month.  
            #TODO Add the reduction or increase in Max Power Demand
            f"{newline}"
            f"{newline}"
            )
            
        pf_report_string = (
            f"{newline}"
            f"{newline}"
            f"POWER FACTOR"
            f"{newline}For this period, Power Factor (PF) degraded below 0.9 for a total of {this_month_pf_result_time} which {pf_state} the 5-hour threshold for a 30-day period.  ."
            f"{newline}"
            f"{newline}* Power Factor Correction may be required if your power factor slips below 0.9 for more than 5 hours in a 30-day period. Failing to correct a poor PF not only leads to much higher power bills, it may significantly damage sensitive electrical components in equipment and machinery."
            # f"{newline}The percentage of time while PF was less than 0.9 changed by {pf_change} % from the previous month."
            # f"{newline}Recommendation: {pf_recommend}"
            # f"{newline}This Month"
            # f"{newline}Total time while PF < 0.9: {this_month_pf_result_time}"
            # f"{newline}Percentage of time in low PF: {pf_time_percent} %"
            # f"{newline}Avg low PF: {this_month_pf_result_avg}"
            # f"{newline}Min low PF: {this_month_pf_result_min}"
            # f"{newline}Avg positive reactive power during low PF: {this_month_var_result_avg} VAR"
            # f"{newline}"
            # f"{newline}Previous Month"
            # f"{newline}Total time while PF < 0.9: {prev_month_pf_result_time}"
            # f"{newline}Percentage of time in low PF: {prev_pf_time_percent} %"
            # f"{newline}Avg low PF: {prev_month_pf_result_avg}"
            # f"{newline}Min low PF: {prev_month_pf_result_min}"
            # #f"{newline}Avg positive reactive power during low PF: {prev_month_var_result_avg} VAR"
            f"{newline}"
            f"{newline}"

            )
            
        vf_report_string = (
            f"{newline}"
            f"{newline}"
            f"VOLTAGE FLUCTUATION"
            f"{newline}Short term Flicker (Pst) values exceeded 1 for {pst_mask_perc}% of the 30-day period."
            f"{newline}{pst_conclusion_string}"
            f"{newline}{vf_conclusion_string}"
            # f"{newline}"
            # f"{newline}This month's Voltage fluctuation percentages and durations by phase:"
            # f"{newline}L1 avg fluctuation: {L1_fluct_avg} %"
            # f"{newline}L2 avg fluctuation: {L2_fluct_avg} %"
            # f"{newline}L3 avg fluctuation: {L3_fluct_avg} %"
            # f"{newline}L1 fluctuation > 7% duration: {L1_fluct_time}"
            # f"{newline}L2 fluctuation > 7% duration: {L2_fluct_time}"
            # f"{newline}L3 fluctuation > 7% duration: {L3_fluct_time}"
            # f"{newline}"
            # f"{newline}* Voltage fluctuations are defined as repetitive or random variations in the magnitude of the supply voltage which may cause spurious tripping of relays, interference with communication equipment, or even severe fluctuations may not allow other loads to be started due to the reduction in supply voltage. Additionally, induction motors that operate at maximum torque may stall if voltage fluctuations are of significant magnitude."
            # f"{newline}* The foremost effect of voltage fluctuations is lamp flicker. Lamp flicker is quantified using a measure called the short-term flicker index (Pst), which is normalized to 1.0 to represent the conventional threshold of irritability to the human eye."
            # f"{newline}* In general, the magnitudes of these variations should not exceed 7% of the nominal supply voltage for more than 5% of the 30-day period, and Flicker Pst values should not exceed 1 for 95% of the 30-day period."    
            # f"{newline}"
            # f"{newline}Previous Month"
            # f"{newline}L1 avg fluctuation: {prev_L1_fluct_avg} %"
            # f"{newline}L2 avg fluctuation: {prev_L2_fluct_avg} %"
            # f"{newline}L3 avg fluctuation: {prev_L3_fluct_avg} %"
            # f"{newline}L1 fluctuation > +/- 7% time: {prev_L1_fluct_time}"
            # f"{newline}L2 fluctuation > +/- 7% time: {prev_L2_fluct_time}"
            # f"{newline}L3 fluctuation > +/- 7% time: {prev_L3_fluct_time}"
            # f"{newline}"
            # f"{newline}Total time while Flicker Pst >= {pst_threshold} : {prev_pst_time}"
            # f"{newline}Percentage of time while flicker Pst >= {pst_threshold}: {prev_pst_mask_perc} %"
            f"{newline}"
            f"{newline}"
            
            )

        unb_report_string = (
            f"{newline}"
            f"{newline}"
            f"{newline}UNBALANCE"
            f"{newline}{nvu_conclusion_string}"
            f"{newline}{niu_conclusion_string}"
            f"{newline}"
            f"{newline}* The greatest effect of voltage unbalance is on three-phase induction motors. This will lead to a reduction in motor efficiency while reducing the insulation life caused by overheating."
            f"{newline}* Powerside recommends that the negative sequence voltage unbalance remain under 2%, and the current unbalance to remain under 50%, both of which should remain below the thresholds for at least 95% of the 30-day period."
            # f"{newline}This Month"
            # f"{newline}Negative voltage unbalance average: {nvu_trend_avg} %"
            #f"{newline}Negative current unbalance average: {niu_trend_avg} %"
            # f"{newline}"
            # f"{newline}Previous Month"
            # f"{newline}Negative voltage unbalance average: {prev_nvu_trend_avg} %"
            # f"{newline}Negative current unbalance average: {prev_niu_trend_avg} %"
            f"{newline}"
            f"{newline}"
            )
               
        harmonic_report_string = (
            f"{newline}"
            f"{newline}"
            f"{newline}HARMONICS"
            f"{newline}{tdd_conclusion_string}"
            f"{newline}{thd_conclusion_string}"
            f"{newline}"
            f"{newline}* Excessive harmonics are a concern as they may cause heating in synchronous/induction machines, interference in communication systems, or damage to capacitors and computers."
            f"{newline}* Powerside recommends that Total Harmonic Distortion should not exceed 5% for more than 5% of a 30-day period, and the Total Demand Distortion not to exceed 25% for more than 25% of a 30-day period."
            # f"{newline}This Month"
            # f"{newline}Average TDD: {tdd_trend_avg} %"
            # f"{newline}Average THD-V: {thd_trend_avg} %"
            # f"{newline}Total time while THD-v > 5%: {thd_mask_time}"
            # f"{newline}Percentage of time while THD-v > 5%: {thd_mask_perc} %"
            # f"{newline}"
            # f"{newline}Previous Month"
            # f"{newline}Average TDD: {prev_tdd_trend_avg} %"
            # f"{newline}Average THD-V: {prev_thd_trend_avg} %"
            # f"{newline}Total time while THD-v > 5%: {prev_thd_mask_time}"
            # f"{newline}Percentage of time while THD-v > 5%: {prev_thd_mask_perc} %"
            f"{newline}"
            f"{newline}"
            )
            
         
        gnd_report_string = (
            f"{newline}"
            f"{newline}"
            f"{newline}GROUND CURRENT"
            f"{newline}{gnd_conclusion_string}"
            f"{newline}"
            f"{newline}* The National Electrical Code (NEC) mandates that a ground cannot serve as a current-carrying conductor. While any amount of current over 10 milliamps (0.01 A) can produce painful to severe shock, currents between 100 and 200 mA (0.1 to 0.2 A) are lethal. Currents above 200 milliamps (0.2 A), while producing severe burns and unconsciousness, do not usually cause death if the victim is given immediate attention. Resuscitation, consisting of artificial respiration, will usually revive the victim."
            f"{newline}* Powerside's Insite monitors and alerts when ground current exceeds a threshold of 100 milliamps (0.1 A)."
            # f"{newline}Previous Month"
            # f"{newline}Total time while ground current > 0.1: {prev_gnd_mask1_time}"
            # f"{newline}Percentage of time in high gnd curr: {prev_gnd_mask1_perc} %"
            # f"{newline}Average ground current: {prev_gnd_trend_avg} amps"
            # f"{newline}Max of 1-min avg ground current readings: {prev_gnd_trend_max} amps"
            f"{newline}"
            f"{newline}"
            )

        #######################################################################
        #  PRINT REPORT STRINGS
        # 
        #######################################################################
        print(report_header_string)
        print(pwr_report_string)
        print(pf_report_string)
        print(vf_report_string)
        print(unb_report_string)
        print(harmonic_report_string)
        print(gnd_report_string)
        file.write(report_header_string + pwr_report_string + pf_report_string + vf_report_string + unb_report_string + harmonic_report_string +gnd_report_string)
        
        file.close()

## Add export to tables, gifs, and to a document

        #######################################################################
        #  PLOT
        # 
        #######################################################################

        # date_format = mpl_dates.DateFormatter('%d-%m-%Y T %H:%M:%S')

        # nvu_trend_hist = px.histogram(trend_df, x="neg_v_unbal",
            # title=f"{acct_name} Histogram of Voltage Unbalance (Negative Sequence)",
            # histnorm='percent',
            
            # labels={
                # "neg_v_unbal": "Voltage Unbalance Percentage",
                # }
        # )
        # nvu_trend_hist.layout.yaxis.title.text = 'Percent of Month'
        # nvu_trend_hist.show()

        # niu_trend_hist = px.histogram(trend_df, x="neg_i_unbal",
            # title=f"{acct_name} Histogram of Current Unbalance (Negative Sequence)",
            # histnorm='percent',
            
            # labels={
                # "neg_i_unbal": "Current Unbalance Percentage",
                # }
        # )
        # niu_trend_hist.layout.yaxis.title.text = 'Percent of Month'
        # niu_trend_hist.show()

        # tdd_hist = px.histogram(trend_df, x="tdd_avg",
                                # title=f"{acct_name} Histogram of TDD %",
                                # histnorm='percent',
                                
                                # labels={
                                        # "tdd_avg": "Total Demand Distortion TDD %",
                                        # }
                                # )
        # tdd_hist.layout.yaxis.title.text = 'Percent of Month'
        # tdd_hist.show()

        # thd_hist = px.histogram(trend_df, x="thd_avg",
                                # title=f"{acct_name} Histogram of THD-v%",
                                # histnorm='percent',
                                
                                # labels={
                                        # "thd_avg": "Total Harmonic Distortion TDD-v %",
                                        # }
                                # )
        # thd_hist.layout.yaxis.title.text = 'Percent of Month'
        # thd_hist.show()

        # pf_hist = px.histogram(trend_df, x="tot_pf_avg",
                                # title=f"{acct_name} Histogram of Power Factor%",
                                # histnorm='percent',
                                
                                # labels={
                                        # "tot_pf_avg": "Total Power Factor",
                                        # }
                                # )
        # pf_hist.layout.yaxis.title.text = 'Percent of Month'
        # pf_hist.show()
        
        # trend_df["date_time"] = trend_df["date_time"].astype(str).str[:-6]
        # trend_df["date_time"] = pd.to_datetime(trend_df["date_time"], format='%Y%m%d %H:%M:%S')
        
        # sun_gnd_plt = px.scatter(sun_df, x="date_time", y="gnd_curr_avg",
                                    # title=f"{acct_name} Sunday Gnd Current (Amps)",
                                    # )
        # sun_gnd_plt.show()
        # gnd_plots = go.Figure()
        # gnd_plots.add_trace(go.Scatter(
            # x=sun_df["date_time"],
            # y=sun_df["gnd_curr_avg"],
            # name="Sunday",
            # mode="markers"
            # ))  
        # gnd_plots.add_trace(go.Scatter(
            # x=mon_df["date_time"],
            # y=mon_df["gnd_curr_avg"],
            # name="Monday",
            # mode="markers"
            # ))
        # gnd_plots.add_trace(go.Scatter(
            # x=tue_df["date_time"],
            # y=tue_df["gnd_curr_avg"],
            # name="Tuesday",
            # mode="markers"
            # ))
        # gnd_plots.add_trace(go.Scatter(
            # x=wed_df["date_time"],
            # y=wed_df["gnd_curr_avg"],
            # name="Wednesday",
            # mode="markers"
            # ))
        # gnd_plots.add_trace(go.Scatter(
            # x=thu_df["date_time"],
            # y=thu_df["gnd_curr_avg"],
            # name="Thursday",
            # mode="markers"
            # ))
        # gnd_plots.add_trace(go.Scatter(
            # x=fri_df["date_time"],
            # y=fri_df["gnd_curr_avg"],
            # name="Friday",
            # mode="markers"
            # ))
        # gnd_plots.add_trace(go.Scatter(
            # x=sat_df["date_time"],
            # y=sat_df["gnd_curr_avg"],
            # name="Saturday",
            # mode="markers"
            # ))
            
        
        # fig.update_layout(
            # xaxis=dict(
                # domain=[0.3, 0.7]
            # ),
            # yaxis=dict(
                # title="yaxis title",
                # titlefont=dict(
                    # color="#1f77b4"
                # ),
                # tickfont=dict(
                    # color="#1f77b4"
                # )
            # ),
            # yaxis2=dict(
                # title="yaxis2 title",
                # titlefont=dict(
                    # color="#ff7f0e"
                # ),
                # tickfont=dict(
                    # color="#ff7f0e"
                # ),
                # anchor="free",
                # overlaying="y",
                # side="left",
                # position=0.15
            # ),
            # yaxis3=dict(
                # title="yaxis3 title",
                # titlefont=dict(
                    # color="#d62728"
                # ),
                # tickfont=dict(
                    # color="#d62728"
                # ),
                # anchor="x",
                # overlaying="y",
                # side="right"
            # ),
            # yaxis4=dict(
                # title="yaxis4 title",
                # titlefont=dict(
                    # color="#9467bd"
                # ),
                # tickfont=dict(
                    # color="#9467bd"
                # ),
                # anchor="free",
                # overlaying="y",
                # side="right",
                # position=0.85
            # )
        # )

        # # Update layout properties
        # fig.update_layout(
            # title_text="multiple y-axes example",
            # width=800,
        # )

        #gnd_plots.show()
        
        # trend_csv = trend_df.to_csv(f"{acct_name}output.csv", index = True)
        # sun_csv = sun_df.to_csv(f"{acct_name}_sun.csv", index = True)
        # mon_csv = mon_df.to_csv(f"{acct_name}_mon.csv", index = True)
        # tue_csv = tue_df.to_csv(f"{acct_name}_tue.csv", index = True)
        # wed_csv = wed_df.to_csv(f"{acct_name}_wed.csv", index = True)
        # thu_csv = thu_df.to_csv(f"{acct_name}_thu.csv", index = True)
        # fri_csv = fri_df.to_csv(f"{acct_name}_fri.csv", index = True)
        # sat_csv = sat_df.to_csv(f"{acct_name}_sat.csv", index = True)

        #print(trend_df)
        # plt.gca().xaxis.set_major_formatter(date_format)

        # sns.scatterplot(x="date_time", y="gnd_curr_avg", data=gnd_diff_result_df)
        # plt.xticks(rotation=90)
        # plt.title("Ground Current")
        # plt.show()


        # fig = go.Figure(data=go.Scatter(x=trend_df['date_time'], y=trend_df['gnd_curr_avg']))
        # fig.update_layout(title = 'Ground Current Events')
        # fig.show()

        # fig1 = px.scatter(x=gnd_diff_result_df['date_time'], y=gnd_diff_result_df['gnd_curr_avg'])
        # fig1.show()

        # fig2 = px.scatter(x=pf_diff_result_df['date_time'], y=pf_diff_result_df['tot_pf_avg'])
        # fig2.show()


        # print("Description of trends dataframe: \n", trend_df.describe(), "\n")

        # corrrelation = trend_df.corr(method="pearson");
        # print("Pearson correlation coefficient:");
        # print(corrrelation);

        # corrrelation    = trend_df.corr(method="kendall");
        # print("Kendall Tau correlation coefficient:");
        # print(corrrelation);

        # corrrelation    = trend_df.corr(method="spearman");
        # print("Spearman rank correlation:");
        # print(corrrelation);


