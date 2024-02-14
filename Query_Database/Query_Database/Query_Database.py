import pyodbc
import pandas as pd
import os
import re
import time 
from datetime import datetime
import numpy as np
import xlsxwriter
from faker import Faker



def valid_awd_input(awd_number):
    
    AWD_number = re.compile(r'^[A-Za-z](\d{7})')
    
    if(awd_number !='' and  AWD_number.search(awd_number) and ',' not in awd_number):
        return True
    elif(awd_number !='' and  ',' in awd_number):
        awds= awd_number.split(',')
        for i in awds:
            print(i)
            if(AWD_number.search(i)==False):
                return False
            else:
                return True 
            
        
    elif(awd_number ==''):
        return True
    else:
        return False




def validate_dates(start, end):
    if(start =='' and end == ''):
        return True
    start_date =pd.to_datetime(start)
    
    end_date = pd.to_datetime(end)

    delta = (end_date - start_date)
    
    return delta.days > 0

def valid_rate_code_input(rate_code):
    
    if(rate_code !='' and len(rate_code)==2 or ',' in rate_code):
        return True
    elif(rate_code ==''):
        return True
    else:
        return False  

#SQL STATEMENT TO BE EXECUTED 

def query_statement(default_segment_description, default_charge_descriptions, default_region, default_start_date, default_end_date, awds, rate_codes):
    
    
    default_segment_description = default_segment_description
    default_charge_descriptions = default_charge_descriptions
    default_region = default_region
    default_start_date = default_start_date
    default_end_date = default_end_date
    default_awd_num = awds
    default_rate_codes = rate_codes
    
    
    default_query = f'''SELECT
	    [Car Group Driven]
	  ,sum([Rentals]) as Rentals
      ,sum([Billed Days]) as Billed_Days
      ,sum([Damage Amount]) as Damages
	  , sum([Damage Amount])/sum([Rentals]) as Damages_PRental
      FROM [DAMAGES].[dbo].[DamagesNoDups]
      where [Damage Amount] >0 and 
      [AWD Number]  in ({default_awd_num}) and [Rate Code] in ({default_rate_codes}) and
      [Segment Description] in ({default_segment_description}) and [Charge Description] in ({default_charge_descriptions}) and
      [Rental Check Out Station Region Name] in ({default_region}) and 
      [Rental Check In Date & Time] between {default_start_date} and {default_end_date}
      group by [Car Group Driven] 
      order by  [Car Group Driven];  '''  
    
    
    
    return default_query



def get_parameters(par):
    #check if the the parameter is passed in
    if(par != ''):
        #get all the parameters separated by commas
        if(',' in par):
            parameters = str(par).split(',')
            return parameters
        elif(',' not in par):
            return par
    else: 
        return par
    
    

def download_path(path):
    return path+'/'+'Data.xlsx'


def add_quotes(word):
    return f"\'{word}\'"


def toUpper(string):
    if(string ==''):
        return ''
    return str(string).capitalize()
'''
function to query database based on gui selection 
'''
def connect_to_database():
    #THIS IS TO CONNECT TO THE ACTUAL DATABASE
    #------------------------------------------------------------------------
    #conn = pyodbc.connect('DRIVER={SQL Server};SERVER=VMISFCOGAFSSQLQ;DATABASE=RENT_EDW;Trusted_Connection=Yes')
    #cursor = conn.cursor()
    #------------------------------------------------------
    #Trusted connection is for teling sql to use microsoft authentification 
    #creating a connection 

    #Below is for testing the connection on my local host 
    conn = pyodbc.connect('DRIVER={SQL Server};SERVER=NBZEDHQ312;DATABASE=DATES;Trusted_Connection=Yes')
    #getting cursor to database.
    cursor = conn.cursor()
    return conn


def queryDB(values):
        
    
    conn = connect_to_database()
    #getting cursor to database.
    
    #Trusted connection is for teling sql to use microsoft authentification 
    #SERVER = 'NBZEDHQ312'
    #DATABASE = '<database-name>'
    #USERNAME = '<username>'
    #PASSWORD = '<password>'
    #A = '[Rental Check Out Station Region Name]'
    # Executing SQL queries
    #cursor.execute('SELECT TOP (1000) [date],[sales] FROM [DATES].[dbo].[Annual_data]')
        #boolean to check if ticked
    TMW = None#values[0]
    #boolean to check if ticked
    Damage_amounts =None# values[1]
    #boolean to check if ticked
    billed_days = None#values[2]
    
    AWD_NUMBER = toUpper(values['-AWDINPUT-'])
    Rate_code = toUpper(values['-RCINPUT-'])
    segment = values[2]
    charge_description = values[3]
    region = values[4]
    start_date = values['-STARTDATE'][0:10]
    end_date = values['-ENDDATE'][0:10]

    #if statement to check if either of these things have been selected 
    default_awd_num = '[AWD Number]'
    default_rate_codes = '[Rate Code]'
    default_segment_description = '[Segment Description]'
    default_charge_descriptions = '[Charge Description]'
    default_region = '[Rental Check Out Station Region Name]'
    default_start_date = '[Rental Check In Date & Time]'
    default_end_date = '[Rental Check In Date & Time]'
    rate_codes = []
    awds = []

    
    if(AWD_NUMBER !=''):
        if(',' not in AWD_NUMBER):
            default_awd_num = add_quotes(AWD_NUMBER)
        else:
            awds = AWD_NUMBER.split(',')
        
    if(Rate_code != ''):
        if(',' not in Rate_code):
            default_rate_codes = add_quotes(Rate_code)
        else:
            rate_codes  = Rate_code.split(',')
            

    if(segment != 'ALL'):
        default_segment_description =add_quotes(segment)
        
    if(region != 'ALL'):
        default_region = add_quotes(region)
    
    if(start_date and end_date):
        default_start_date = add_quotes(start_date)
        default_end_date = add_quotes(end_date)
           
        
    dataframes ={}    
    if(len(rate_codes) > 1 and len(awds) > 1):
        for i in rate_codes:
            for j in awds:
                default_awd_num = add_quotes(j)
                default_rate_codes= add_quotes(i)
                default_query= query_statement(default_segment_description,default_charge_descriptions, default_region, default_start_date, default_end_date, default_awd_num, default_rate_codes )
            dataframes[default_rate_codes] = pd.read_sql_query(default_query, conn)
            
        return dataframes
            
    elif(len(rate_codes) > 1):
        for i in rate_codes:
            default_rate_codes = add_quotes(i)
            default_query =query_statement(default_segment_description,default_charge_descriptions, default_region, default_start_date, default_end_date, default_awd_num, default_rate_codes )
            dataframes[default_rate_codes] = pd.read_sql_query(default_query, conn)
            
        return dataframes
    
    elif(len(awds) > 1):
        for i in default_awd_num:
            default_awd_num = add_quotes(j)
            default_query= query_statement(default_segment_description,default_charge_descriptions, default_region, default_start_date, default_end_date, default_awd_num, default_rate_codes )
            dataframes[default_awd_num] = pd.read_sql_query(default_query, conn)
            
        
        
        return dataframes
        

            
            
     

    else:
            default_query= query_statement(default_segment_description,default_charge_descriptions, default_region, default_start_date, default_end_date, default_awd_num, default_rate_codes )
            df = pd.read_sql_query(default_query, conn)
            # Print first few rows of DataFrame
            return df




 #CREATING THE GRAPHICAL USER INTERFACE
import PySimpleGUI as sg

Segments = ['ALL','Local','Weekend','Monthly',"VIPCO",'Government','Standard','Replacement','Other'
,'Long-haul Leisure','Commercial']

Car_group = ['ALL','A','B','C','D','E','F','G','H','I','J','K','L','M','N','P']

Charge_description = ['ALL','Lost / Stolen / Damaged e-toll tags''Refuelling Charge','Child Safety Seat',
'Super CDW / TLW','Breach','None',
'Traffic Fine Admin Fee','Parking Garage Storage','Cleaning / Valet','Tyre / Wheel Damage',
'Cession Rentals - Standard','Towing','Cession Rentals - By Agreement','Keys',
'Accidents / Thefts','Cross Border Fee','Call Out Fees','Ski / Luggage Rack',
'E-Toll Charges','Windscreen Damage Waiver','Other','Telephone Rental']

Region = ['ALL','KWA-ZULU NATAL',
'WESTERN CAPE','NORTHERN PROVINCE',
'EASTERN CAPE','GAUTENG','FREE STATE']
#theme of gui
sg.theme('darkgrey13')
#layout of gui
layout = [ 
[sg.HSeparator()],
[sg.Text("Multiple inputs must be separated by a comma as follows")],
[sg.Text("Letters must be capitalised")],
[sg.Text('Rate Code'), sg.Input('', enable_events= True, key = '-RCINPUT-')],
[sg.Text('AWD Number'), sg.Input('', enable_events= True, key = '-AWDINPUT-')],
[sg.HSeparator()],     
[sg.Text("Select a Segment"), sg.DD( Segments, size = (15,8))],
[sg.Text("Select Charge Description"),sg.DD( Charge_description, size = (15,8))],
[sg.Text("Select Region"),sg.DD( Region, size = (15,8))  ],
[sg.HSeparator()],
[sg.CalendarButton('Start Date',target= "-STARTDATE"), sg.I(key = "-STARTDATE") ],    
[sg.CalendarButton('End Date', target= "-ENDDATE"), sg.I(key = "-ENDDATE")],
[sg.HSeparator()],
[sg.Text("Select download path")],
[sg.FolderBrowse(target ="-PATH-"),sg.I(key = "-PATH-")],
[sg.Button("Submit")]]
         
window=sg.Window(title="Query Database", layout= layout,resizable=True,element_justification='right', margins=(70, 70), finalize= True)
#----------------------------------------------------------------------------------------------------------------------------------------------------------------

values={}
while True:
    event, values = window.read()

    # if the close icon is selected close the window
    
    if event == sg.WIN_CLOSED :
        break
   
    elif event == "Submit" :
        
        # validate inputs
        if(valid_awd_input(values['-AWDINPUT-'])==False ):
            sg.popup("AWD is invalid")
            continue
        if(valid_rate_code_input(values['-RCINPUT-'])==False):
            sg.popup("Rate Code is invalid")
            continue
        elif(valid_awd_input(values['-AWDINPUT-'])==False and valid_rate_code_input(values['-RCINPUT-'])==False):
            sg.popup_error("AWD and Rate Code Invalid")
            continue
        elif(validate_dates(values['-STARTDATE'],values['-ENDDATE']) == False):
            sg.popup_error("End date must be after start date; Check end date")
            continue
        elif(values['-PATH-']==''):
            sg.popup_error('Invalid Download Path')
            continue
            
            
            
    
        print(values)
        df =queryDB(values)
        print(df)
        # check if the datafarame returns a dictionary of dataframes
        if( isinstance(df, dict) ):
            options = {}
            options['strings_to_formulas'] = False
            options['strings_to_urls'] = False
            with pd.ExcelWriter(download_path(values['-PATH-']), mode = 'w', engine ='xlsxwriter' ) as writer:
                for i  in df.keys():
                    print(i,df[i])
             #for each key which is the rate code. get a table from the database
                    df[i].to_excel(writer, sheet_name= str(i)[1:-1], index = False)
                    
                    # WRITING TO ONE FILE BUT MULTIPLE EXCEL SHEETS 
            sg.popup('Query Successful')        
                
                                
        elif(not isinstance(df,dict)):
            #only one rate code was selected from the database
            df.to_excel(download_path(values['-PATH-']),index= False)
            #WRITING TO ONE EXCEL FILE
            sg.popup('Query Successful')
                                    
            
        
        
        break
       
    
    
     
window.close()        