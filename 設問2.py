import datetime
import csv
import pandas as pd
WAIT=2

df = pd.read_csv("server_log.csv",index_col=["server_address"])
df= df.sort_values(["server_address","datetime"])
now_index=0
cr_count=0

for index,dt,ping in zip( df.index,df["datetime"],df["ping"]):
    dt_s=str(dt)
    y,mon,d,h,m,s=map(int,(dt_s[0:4],dt_s[4:6],dt_s[6:8],dt_s[8:10],dt_s[10:12],dt_s[12:14]))
    d_time=datetime.datetime(y,mon,d,h,m,s)
    
    if(now_index!=index):
        now_index=index
        cr_count=0
    if(now_index==index):
        y,mon,d,h,m,s=map(int,(dt_s[0:4],dt_s[4:6],dt_s[6:8],dt_s[8:10],dt_s[10:12],dt_s[12:14]))
        d_time=datetime.datetime(y,mon,d,h,m,s)
        
        #print(now_index,str(index),str(d_time),str(ping))
        
        
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
                print("Server"+index+" Crash_length: "+str(crash_length))
            cr_count=0
            
                
            
            
     
            
    
    
    
    


        
    
    
    

