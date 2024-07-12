# TeamViewer connections file reader
''
# TeamViewer_Connections.py
# created by mahemys; 2021.08.22
# !perfect, but works!
# MIT License; no license; free to use!
# update 2021.08.22; initial review
# update 2024.07.12; optimise
#------------------------------------------------------------
# read TeamViewer Connections.txt file
# calculate each sessions time duration
# save summary to a text file
#------------------------------------------------------------
# windows - C:\Users\{UserName}\AppData\Roaming\TeamViewer\Connections.txt
#------------------------------------------------------------
''

sample_file = 'yes' #no; yes
TextFileOut = 'Connections_Summary.txt'

import os
import numpy as np
import pandas as pd
from datetime import datetime

dt_start = datetime.now()
print(dt_start, 'start...')

if sample_file == 'yes':
    #sample file
    File_dir     = os.path.dirname(__file__)
    TextFileName = 'Connections_Sample.txt'
    TextFilePath = os.path.join(File_dir, TextFileName)
else:
    #original file
    TextFileName = 'Connections.txt'
    AppDataDir   = os.path.expandvars(r'%AppData%\TeamViewer')
    TextFilePath = os.path.join(AppDataDir, TextFileName)
print(TextFilePath)

def main():
    #read file
    t_line = 0
    if not os.path.exists(TextFilePath):
        print("{} {} {} {}".format(datetime.now(), 'NoFile', TextFileName, t_line))
    else:
        try:
            #read file in chunks, append and create df, exclude non datastring
            chunklist = []
            chunksize = 10000
            for chunk in pd.read_csv(TextFilePath, delim_whitespace=True, header=None, 
                                     comment="#", skiprows=0, skip_blank_lines=True, 
                                     low_memory=False, chunksize=chunksize):
                chunklist.append(chunk)
            
            #create df
            df = pd.concat(chunklist, axis= 0).dropna()
            #print(chunklist)
            del chunklist
            
            #add header to df
            header_array =  ['SessionTVID','SessionStartDate','SessionStartTime','SessionStopDate','SessionStopTime','SessionUser','SessionType','SessionUID']
            df.columns = header_array
            #print(df.info())
            #print(df.head())
            #print(df.shape)
            
            t_line = len(df.index)
            print("{} {} {} {}".format(datetime.now(), 'Found', TextFileName, t_line))
        except:
            print('Exception: #read file', TextFileName)
            pass
    
    print(datetime.now(), 'read complete...')
    
    if t_line == 0:
        print("{} {} {} {}".format(datetime.now(), 'no data', TextFileName, t_line))
    else:
        try: 
            #Pass values from df
            SessionTVID          = df['SessionTVID']
            SessionStartDate     = df['SessionStartDate']
            SessionStartTime     = df['SessionStartTime']
            SessionStopDate      = df['SessionStopDate']
            SessionStopTime      = df['SessionStopTime']
            SessionStartDateTime = df['SessionStartDate'] + ' ' + df['SessionStartTime']
            SessionStopDateTime  = df['SessionStopDate']  + ' ' + df['SessionStopTime']
            
            #Calculate TimeDifference...
            df['SessionStartDateTime']  = pd.to_datetime(SessionStartDateTime, format='%d-%m-%Y %H:%M:%S')
            df['SessionStopDateTime']   = pd.to_datetime(SessionStopDateTime,  format='%d-%m-%Y %H:%M:%S')
            df['SessionTimeDifference'] = abs(df['SessionStopDateTime'] - df['SessionStartDateTime'])
            df['SessionTimeTotMinutes'] = abs(df['SessionStopDateTime'] - df['SessionStartDateTime']).dt.total_seconds().div(60).round(decimals=2)
            df['SessionTimeTotSeconds'] = abs(df['SessionStopDateTime'] - df['SessionStartDateTime']).dt.total_seconds().round(decimals=2)
            #print(df.info())
            print(df.head())
            #print(df.shape)
            
            #summary of session, count, time
            TVID_unique_list = df.SessionTVID.unique().tolist()
            #print(TVID_unique_list)
            
            TVID_Total_cnt = df.groupby('SessionTVID')['SessionTVID'].count()
            print('TVID_Total_cnt', len(TVID_unique_list))
            #print(TVID_Total_cnt)
            
            TVID_Total_sec = df.groupby('SessionTVID')['SessionTimeTotSeconds'].sum().round(decimals=2)
            print('TVID_Total_sec', len(TVID_Total_sec))
            #print(TVID_Total_sec)
            
            TVID_Total_min = df.groupby('SessionTVID')['SessionTimeTotMinutes'].sum().round(decimals=2)
            print('TVID_Total_min', len(TVID_Total_min))
            #print(TVID_Total_min)
            
            TVID_Total_dur = df.groupby('SessionTVID')['SessionTimeDifference'].sum()
            print('TVID_Total_dur', len(TVID_Total_dur))
            #print(TVID_Total_dur)
            
            #create df using unique, count, time
            df1 = pd.DataFrame(TVID_Total_cnt.items(), columns=['TVID_unique', 'count'])
            df2 = pd.DataFrame(TVID_Total_dur.items(), columns=['TVID_unique', 'duration'])
            df3 = pd.DataFrame(TVID_Total_sec.items(), columns=['TVID_unique', 'totsec'])
            df4 = pd.DataFrame(TVID_Total_min.items(), columns=['TVID_unique', 'totmin'])
            dff = pd.merge(df1,df2, on='TVID_unique', how='inner')
            dff = pd.merge(dff,df3, on='TVID_unique', how='inner')
            dff = pd.merge(dff,df4, on='TVID_unique', how='inner')
            
            dff.columns = ['SessionTVID', 'Count', 'Duration', 'TotSec', 'TotMin']
            #print(dff.info())
            print(dff.head())
            #print(dff.shape)
            
            #save to file... summary
            dff.to_csv(TextFileOut, header=True, sep='\t', index_label='#')
            
            #save to file... sessions
            column_list = ['SessionTVID', 'SessionStartDateTime', 'SessionStopDateTime', 'SessionTimeDifference', 'SessionTimeTotMinutes', 'SessionTimeTotSeconds']
            df.to_csv(TextFileOut, mode='a', header=True, columns=column_list, index=True, sep='\t', index_label='#')
        except:
            print('Exception: #Calculate TimeDifference...')
            pass

if __name__ == "__main__":
    try:
        #main function...
        main()

        #print time taken
        dt_stop = datetime.now()
        dt_diff = (dt_stop - dt_start)
        print(dt_stop, 'all tasks complete...')
        print('Time taken {}'.format(dt_diff))
    except KeyboardInterrupt:
        # do nothing here
        print('{} - KeyboardInterrupt - Exit...'.format(datetime.now()))
        pass
    
