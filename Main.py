import pandas as pd
import ConfigParser
import numpy as np
from collections import Counter
import sys
import operator
import matplotlib.pyplot as plt
import datetime
import matplotlib.dates as mdates




class EventLog():

    def __init__(self):
        config = ConfigParser.RawConfigParser()
        config.read('D:\GitReps\DataChallengeInkredo\settings.cfg')
        self.CSVFileName = config.get('filename', 'CSVFilename')
        self.dataframe=self.ReadCSV(self.CSVFileName)
        self.Groupkeys = self.dataframe.groupby('timestamp1').groups.keys()
        self.ClickThroughRate(self.dataframe,self.Groupkeys)
        #self.ResultPosition(self.dataframe,self.Groupkeys)
        #self.ZeroResultRates(self.dataframe,self.Groupkeys)



    def ReadCSV(self,CSVName):
        df=pd.read_csv(CSVName,dtype=object)
        df2=df.replace(np.nan, '', regex=True)
        return df2


    def ClickThroughRate(self,df,Gk):
        Result_a=[]
        Result_b = []
        for i in Gk:
            df_date_a= df[(df['timestamp1'] == i)& (df['group']=='a')]
            df_date_b=df[(df['timestamp1'] == i)& (df['group']=='b')]
            Result_a.append((self.ClickThroughRateHelper(df_date_a),i))
            Result_b.append((self.ClickThroughRateHelper(df_date_b), i))
        self.Visualize(Result_a)


    def ClickThroughRateHelper(self,df):
        PagesVisit_Count=0
        Sessioncount=0
        SessionIDList=[]
        for i in df.loc[:,'session_id']:
            SessionIDList.append(i)
        SessionIDListUnique=list(set(SessionIDList))

        for session in SessionIDListUnique[:100]:
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

    def ResultPosition(self,df,Gk):

        Result=[]
        for i in Gk:
            df_sub = df[(df['timestamp1'] == i) & (df['result_position'] != 'NA')]
            A=self.ResultPositionHelper(df_sub)
            Result.append((max((Counter(A)).iteritems(), key=operator.itemgetter(1))[0],i))

        print Result

    def ResultPositionHelper(self,df):
        SessionIDList=[]
        Result=[]
        for i in df.loc[:,'session_id']:
            SessionIDList.append(i)
        SessionIDListUnique=list(set(SessionIDList))
        for session in SessionIDListUnique:
            df_sub = df[df['session_id'] == session].sort_values(by='timestamp')
            Result += df_sub.loc[:,'result_position'].tolist()
        return Result

    def ZeroResultRates(self,df,Gkeys):
        Result_a=[]
        Result_b = []
        for i in Gkeys:
            df_date_a= df[(df['timestamp1'] == i)& (df['group']=='a')]
            df_date_b=df[(df['timestamp1'] == i)& (df['group']=='b ')]
            Result_a.append((self.ZeroResultRatesHelper(df_date_a),i))
            Result_b.append((self.ZeroResultRatesHelper(df_date_b),i))
        print Result_a,'\n',Result_b

    def ZeroResultRatesHelper(self,df):
        SessionIDList = []
        Count = 0
        for i in df.loc[:, 'session_id']:
            SessionIDList.append(i)
        SessionIDListUnique = list(set(SessionIDList))
        for session in SessionIDListUnique:
            df_sub = df[(df['session_id'] == session) & (df['action'] == 'searchResultPage')]
            for i in df_sub.loc[:,'n_results']:
                if int(i)==0:
                    Count+=1
        return Count

    def Visualize(self,data):
        rate=[]
        Time=[]
        for i in data:
            rate.append(i[0])
            Time.append(datetime.datetime.strptime(i[1], "%d/%m/%Y").strftime("%d-%m-%Y"))
        y = range(len(Time))
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d-%m-%Y'))
        plt.gca().xaxis.set_major_locator(mdates.DayLocator())
        plt.plot(rate, y)
        plt.gcf().autofmt_xdate()



if __name__=='__main__':
    EventLog()