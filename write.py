import sys
import os
import a2s
import datetime
import requests
import socket
import csv
from time import time,sleep

#Manually adding submodule path from this path
dirname = os.path.dirname(os.path.realpath(__file__))
importfilepath = os.path.join(dirname,"python-valve")
sys.path.append(importfilepath)
import valve.source
import valve.source.master_server
import valve.source.messages
import valve.source.util
import valve.source.messages

#Parameters
timeout_query = 1
timeout_master = 1
regionserver= "all"
game="tf" # cstrike,tf,hl2,hl2mp,csgo
Gamemode = "dr_"# Gamemode dr_,pl_,ctf_
OutputFileName = "output.csv" #Output Filename
RuntimeMinutes = 60 #for how many minutes to run.
RunForever = True #True will run forever. Set to False to use runtime 
Format = '%Y-%m-%d-%H:%M:%S' # date format

#scan masterserver for ips, output address if it is running the desired gamemode
def SlowScan():
    x = 0 # number of "broken" servers print me to see it
    y = 0 # number of timed out servers
    z = 0 #total servers
    ips = []
    with valve.source.master_server.MasterServerQuerier(timeout=timeout_master) as msq:
        try:
            for address in msq.find(gamedir=game,empty=True,secure=True,region=regionserver):
                print(address)
                try:
                    server = a2s.info(address,timeout=timeout_query)

                    datastack = [address[0],address[1],server.map_name,server.player_count]
                    if Gamemode in datastack[2]:
                        datastack.append(datetime.datetime.now().strftime(Format))
                        print(f'!!!!{datastack} has been added')
                        response = requests.get(f"http://ip-api.com/json/{address[0]}").json()
                        region = response["countryCode"]
                        datastack.append(region)
                        ips.append(datastack)
                        x += 1
                except (AttributeError,a2s.BrokenMessageError):
                    y += 1
                except socket.timeout:
                    z += 1
                print(f"{x}:servers {y}:server errors {z}:timeouts")

            print("__________")
            return ips
        except valve.source.NoResponseError:
            print("Master server request timed out!")
# Writes input to csv use prefixcull for all servers
def ProtoWriter(list):
    try:
        with open(rawfilename,"r") as filedata:
            print("read successful")
    except:
        with open(rawfilename,"w",newline="") as filedata:
            print("NO FILE, MAKING NEW")

    with open(rawfilename,"a",newline="") as filedata:
            for ip in list:
                csv.writer(filedata).writerow(ip)
# Scans unique Ip's in .cvs file and appends new data to the list Much faster than scanning all the servers
def FastScan():
    iplist = []
    with open(rawfilename,"r") as filedata:
        csvreader = csv.reader(filedata)
        for ip in csvreader:
            if (ip[0],ip[1]) not in iplist:
                iplist.append((ip[0],ip[1]))
    print("#################################################")
    print(iplist)
    print("######## SERVERS FOUND FROM CSV #################")
    return iplist
def Listscan(list_ips=[]):
    x = 0 # number of "broken" servers print me to see it
    y = 0 # number of timed out servers
    z = 0 #total servers
    ips = []
    try:
        for address in list_ips:
            fix_address = (address[0],int(address[1]))
            try:
                server = a2s.info(fix_address,timeout=timeout_query)

                datastack = [address[0],fix_address[1],server.map_name,server.player_count]
                if Gamemode in datastack[2]:
                    datastack.append(datetime.datetime.now().strftime(Format))
                    print(f'!!!!{datastack} has been added')
                    response = requests.get(f"http://ip-api.com/json/{address[0]}").json()
                    region = response["countryCode"]
                    datastack.append(region)
                    ips.append(datastack)
                    x += 1

            except (AttributeError,a2s.BrokenMessageError):
                y += 1
            except socket.timeout:
                z += 1

        return ips
        
    except valve.source.NoResponseError:
        print("Master server request timed out!")
# Runs in slow mode first to create initial server list, then in fast mode. Running in slowmode afterwards increase the sever pool that fast mode scans for.
def MainWriter(isfast):
    if isfast == True:
        a = Listscan(list_ips=FastScan())
        ProtoWriter(a)
    elif isfast == False:
        ProtoWriter(SlowScan())
# delay is time between each fast search
# update is delay between each update where it looks for new servers. IT's very slow.
# length determines how long the program runs for before automatically stopping. You can stop the program at any time.
#Run MainWriter in fast/slow mode for n minutes
def Iterator(delay=5,update=50):

    end = time() + RuntimeMinutes*60
    x = update
    while time() < end or RunForever:
        if x >= update:
            print(f"Initiate SLOW SEARCH. This will run every {update} minutes")
            MainWriter(False)
            x = 0
        else:
            print("Initiate FAST SEARCH. I scan the CSV for servers instead")
            MainWriter(True)
            x += 1
            print(f"taking a break for {delay} minutes")
            sleep(delay*60)

    print(f"{RuntimeMinutes} : minutes complete")

#init
if __name__ == "__main__":#when launched directly.
    rawfilename = os.path.join(dirname,OutputFileName)
    Iterator()