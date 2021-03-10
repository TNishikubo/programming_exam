import datetime
import csv
import pandas as pd

WAIT=2 #タイムアウトがこの回数を超えたとき故障とみなす WAIT=2なら3回連続でタイムアウトした時に故障と判定される
PING_OVER=100 #平均がこの値を越えたら過負荷状態とみなす
AVE_RANGE=3 #平均をとる範囲



df = pd.read_csv("server_log.csv",index_col=["server_address"])
df= df.sort_values(["server_address","datetime"])
now_index=0
CrashFrag=0

for index,dt,ping in zip( df.index,df["datetime"],df["ping"]):
    dt_s=str(dt)
    y,mon,d,h,m,s=map(int,(dt_s[0:4],dt_s[4:6],dt_s[6:8],dt_s[8:10],dt_s[10:12],dt_s[12:14]))
    d_time=datetime.datetime(y,mon,d,h,m,s)
    
    if(now_index!=index):
        
        now_index=index
        cr_count=0
        ping_range=[]
        ol_count=0
        
    elif(now_index==index):
        y,mon,d,h,m,s=map(int,(dt_s[0:4],dt_s[4:6],dt_s[6:8],dt_s[8:10],dt_s[10:12],dt_s[12:14]))
        d_time=datetime.datetime(y,mon,d,h,m,s)
        
        
        
        #故障に関する処理
        if(cr_count==0 and ping=="-"):
            
            cr_count+=1
            crash_time=d_time
           
            
        elif(cr_count!=0 and ping=="-"):
            cr_count+=1
            if(cr_count==WAIT+1):
                print(str(d_time)+"_ Server"+index+" Crash")
                
            
        elif(cr_count!=0 and ping!="-"):
            crash_length=d_time-crash_time
            if(cr_count>=WAIT+1):
                print(str(d_time)+"_ Server"+index+" Crash_length: "+str(crash_length))
                CrashFrag=1
            cr_count=0
        
        #過負荷状態に関する処理
        if(ping=="-"): 
            ping_new=PING_OVER*AVE_RANGE
        else:
            ping_new=int(ping)
         
        if(len(ping_range)<AVE_RANGE):              
            ping_range.append(ping_new)
           
        else:
            ping_range.pop(0)
            ping_range.append(ping_new)
            
        
        ping_ave=sum(ping_range)/len(ping_range)
        
        
        if(ping_ave>=PING_OVER):
            ol_count+=1
            if(ol_count==1):
                ol_time=d_time
                print(str(d_time)+"_ Server"+index+" Overload")
        elif(ol_count!=0 and ping_ave<PING_OVER):
            ol_length=d_time-ol_time
            if(CrashFrag==0):#故障が発生した場合は過負荷状態の時間を出力しない
                print(str(d_time)+"_ Server"+index+" Overload_length: "+str(ol_length))
            else:
                CrashFrag=0
            ol_count=0
            
            
     
            
    
    
    
    

