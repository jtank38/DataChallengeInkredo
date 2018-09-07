import pandas as pd
import ConfigParser
import numpy as np
import sys


class EventLog():

    def __init__(self):
        config = ConfigParser.RawConfigParser()
        config.read('D:\GitReps\DataChallengeInkredo\settings.cfg')
        self.CSVFileName = config.get('filename', 'CSVFilename')
        self.dataframe=self.ReadCSV(self.CSVFileName)
        self.ClickThroughRate(self.dataframe)

    def ReadCSV(self,CSVName):
        df=pd.read_csv(CSVName,dtype=object)
        return df


    def ClickThroughRate(self,df):
        Groupkeys = df.groupby('timestamp1').groups.keys()
        Result_a=[]
        Result_b = []
        for i in Groupkeys:
            df_date_a= df[(df['timestamp1'] == i)& (df['group']=='a')]
            df_date_b=df[(df['timestamp1'] == i)& (df['group']=='b')]
            Result_a.append((self.ClickThroughRateHelper(df_date_a),i))
            Result_b.append((self.ClickThroughRateHelper(df_date_b), i))
        print Result_a,Result_b



    def ClickThroughRateHelper(self,df):
        PagesVisit_Count=0
        Sessioncount=0
        SessionIDList=[]
        for i in df.loc[:,'session_id']:
            SessionIDList.append(i)
        SessionIDListUnique=list(set(SessionIDList))

        for session in SessionIDListUnique:
            df_sub= df[df['session_id']==session].sort_values(by='timestamp')
            Timestamplist= df_sub[df_sub['action']=='searchResultPage']['timestamp'].tolist()
            Sessioncount+=len(df_sub.index)
            try:
                MaxSearchResultTime= max([Time for Time in Timestamplist])
                df_Filtered= df_sub[df_sub.timestamp > MaxSearchResultTime]
                PagesVisit_Count+= len(df_Filtered[df_Filtered['action']=='visitPage'].index)
            except:
                PagesVisit_Count += 0

        return np.divide(float(PagesVisit_Count),float(Sessioncount)) #Considering 1000 session counts





if __name__=='__main__':
    EventLog()