import sys
import os
import a2s
import datetime
import requests
import socket
import csv
import re
from time import time,sleep
import Parameters as p

#Manually adding submodule path from this path
dirname = os.path.dirname(os.path.realpath(__file__))
importfilepath = os.path.join(dirname,"python-valve")
sys.path.append(importfilepath)

import valve.source
import valve.source.master_server
import valve.source.messages
import valve.source.util
import valve.source.messages

#The Writer will create the .csv dates in this format. DO not change if you already have a .csv
#Having multiple date formats in a single .csv will corrupt the .csv data.
WriterTimeFormat = '%Y-%m-%d-%H:%M:%S'
#Ensures that the gamemode is correct
def PrefixEnsure(string):
    prefix = re.split("_",string)[0].lower()+"_" 
    if(prefix == p.Gamemode.lower()):
        return True
    else:
        return False
#Scan all possible servers in the game for map matches.
def SlowScan():
    x = 0 # Servers
    y = 0 # Broken Servers
    z = 0 # Timeouts
    ips = []
    with valve.source.master_server.MasterServerQuerier(timeout=p.timeout_master) as msq:
        try:
            for address in msq.find(gamedir=p.Game,empty=True,secure=True,region=p.regionserver):
                print(address)
                try:
                    server = a2s.info(address,timeout=p.timeout_query)

                    datastack = [address[0],address[1],server.map_name,server.player_count]
                    if PrefixEnsure(datastack[2]):
                        datastack.append(datetime.datetime.now().strftime(WriterTimeFormat))
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
#Writes the incoming datastack to a csv line.
def CSVWriter(list):
    try:
        with open(rawfilename,"r") as filedata:
            print("read successful")
    except:
        with open(rawfilename,"w",newline="") as filedata:
            print("NO FILE, MAKING NEW")

    with open(rawfilename,"a",newline="") as filedata:
            for ip in list:
                csv.writer(filedata).writerow(ip)
# First part of FastScan. Searches for IP's in the CSV.
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
# Second part of fast scan. searches servers using incoming list.
def IpScan(list_ips=[]):
    x = 0 # Servers
    y = 0 # Broken Servers
    z = 0 # Timeouts
    ips = []
    try:
        for address in list_ips:
            fix_address = (address[0],int(address[1]))
            try:
                server = a2s.info(fix_address,timeout=p.timeout_query)

                datastack = [address[0],fix_address[1],server.map_name,server.player_count]
                if PrefixEnsure(datastack[2]):
                    datastack.append(datetime.datetime.now().strftime(WriterTimeFormat))
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
        CSVWriter(IpScan(FastScan()))
    elif isfast == False:
        CSVWriter(SlowScan())
# delay is time between each fast search
# update is delay between each update where it looks for new servers. IT's very slow.
# length determines how long the program runs for before automatically stopping. You can stop the program at any time.
#Run MainWriter in fast/slow mode for n minutes

#Do not touch the iterator parameters if you already have csv data. It will affect the data in unpredictable ways.
def Iterator(delay=5,FastScansTillSlow=15):

    end = time() + p.RuntimeMinutes*60
    x = FastScansTillSlow
    while time() < end or p.RunForever:
        if x >= FastScansTillSlow:
            print(f"Initiate SLOW SEARCH. This will run every {FastScansTillSlow*delay} minutes")
            MainWriter(False)
            x = 0
        else:
            print("Initiate FAST SEARCH. I scan the CSV for servers instead")
            MainWriter(True)
            x += 1
            print(f"taking a break for {delay} minutes")
            sleep(delay*60)

    print(f"{p.RuntimeMinutes} : minutes complete")
#init
if __name__ == "__main__":
    rawfilename = os.path.join(dirname,p.Filename)
    Iterator()