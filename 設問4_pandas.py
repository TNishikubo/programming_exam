import datetime
import csv
import numpy as np
import pandas as pd
import ipaddress

WAIT=2 #タイムアウトがこの回数を超えたとき故障とみなす WAIT=2なら3回連続でタイムアウトした時に故障と判定される
PING_OVER=100 #平均がこの値を越えたら過負荷状態とみなす
AVE_RANGE=3 #平均をとる範囲



df = pd.read_csv("server_log_subnet.csv",index_col=["server_address"])
df= df.sort_values(["server_address","datetime"])

allserver=set()
for index,dt,ping in zip( df.index,df["datetime"],df["ping"]):
    allserver.add(index)


now_index=0
crash_list=[]
CrashFrag=0

for index,dt,ping in zip( df.index,df["datetime"],df["ping"]):
    dt_s=str(dt)
    y,mon,d,h,m,s=map(int,(dt_s[0:4],dt_s[4:6],dt_s[6:8],dt_s[8:10],dt_s[10:12],dt_s[12:14]))
    d_time=datetime.datetime(y,mon,d,h,m,s)
    
    
    if(now_index!=index):#別のサーバーについての処理を始める前にリセットする
        same_subnet=set()
        crash_subnet=set()
        crash_subnet_time=set()
        now_index=index
        cr_count=0
        ping_range=[]
        ol_count=0
        sncr_count=0
        allstop=0
        
    elif(now_index==index):
        y,mon,d,h,m,s=map(int,(dt_s[0:4],dt_s[4:6],dt_s[6:8],dt_s[8:10],dt_s[10:12],dt_s[12:14]))
        d_time=datetime.datetime(y,mon,d,h,m,s)
        s_ip=ipaddress.ip_interface(index)
        
        
#現在見ているサーバーと同一のサブネットを持つサーバーを全て取得
        for ip in allserver:
            if(s_ip in ipaddress.ip_network(ip, strict=False)):
                same_subnet.add(ip)
        
        
        
        
        
        #故障に関する処理
        if(cr_count==0 and ping=="-"):
            
            cr_count+=1
            crash_time=d_time
            
           
            
        elif(cr_count!=0 and ping=="-"):
            cr_count+=1
            if(cr_count==WAIT+1):
                print(str(d_time)+"_ Server"+index+" Crash")
                crash_list.append([index,d_time])
            elif(cr_count>WAIT+1):
                crash_list.append([index,d_time])
                
            if(cr_count>=WAIT+1):
                
                for ip,ctime in crash_list:
                    #現在のipが故障リストの中にいるとき集合に追加
                    if(s_ip in ipaddress.ip_network(ip, strict=False)):
                        crash_subnet.add(ip)
                        subnet=ipaddress.ip_network(index, strict=False)
                    #タイムアウトの時間差が1分以内の時集合に追加    
                    if(abs(d_time-ctime)<datetime.timedelta(minutes=1)):
                        crash_subnet_time.add(ip)
                 
                
                for ip,ctime in crash_list:#サブネットスイッチ故障の判定
                    
                    #同一のサブネット内のサーバーがすべて止まったときカウント
                    if(same_subnet==crash_subnet and same_subnet==crash_subnet_time ):
                        allstop+=1
                         
                    if(allstop>=WAIT+1):
                        
                        sncr_count+=1
                        if(sncr_count==1):
                            print(str(d_time)+"_ Subnet switch"+str(subnet)+" Crash")
                            sncr_time=d_time
                        
        
                   
               
            
        elif(cr_count!=0 and ping!="-"):
            crash_length=d_time-crash_time
            if(sncr_count!=0 and cr_count>=WAIT+1):
                print(str(d_time)+"_ Subnet switch"+str(subnet)+" Crash_length: "+str(d_time-sncr_time+datetime.timedelta(minutes=WAIT)))
        
                
            if(cr_count>=WAIT+1):
                print(str(d_time)+"_ Server"+index+" Crash_length: "+str(crash_length))
                CrashFrag=1
            cr_count=0
            sncr_count=0
            allstop=0
        
        #過負荷状態に関する処理
        if(ping=="-"): 
            ping_new=PING_OVER*AVE_RANGE #平均をとる範囲にタイムアウトが存在すると必ず過負荷状態と判定される
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
    same_subnet.clear()
    crash_subnet.clear()
            