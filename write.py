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
import argparse

#Data format of csv
#[ip,port,mapname,playercount,time,region,scanindex]

#Arguments
parser = argparse.ArgumentParser(description="SourceMapStats writer. Defaults in parameter.py",formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("-g", "--game", type=str, help="Master server to use",default=p.Game)
parser.add_argument("-gm", "--gamemode", type=str, help="Gamemode dr_,pl_,ctf_",default=p.Gamemode)
parser.add_argument("-f", "--filename", type=str, help="filename including suffix.",default=p.Filename)
parser.add_argument("-r", "--region", type=str, help="filename including suffix.",default=p.regionserver)
parser.add_argument("-st", "--servertimeout", type=str, help="filename including suffix.",default=p.timeout_query)
parser.add_argument("-mst", "--masterservertimeout", type=str, help="filename including suffix.",default=p.timeout_master)
parser.add_argument("-rf", "--runforever", type=bool, help="Run the writer forever.",default=p.RunForever)
parser.add_argument("-tm", "--timer", type=str, help="For how many minutes to run if runforever is False.",default=p.RuntimeMinutes)
args = parser.parse_args()
config = vars(args)


def clear(): 

    # for windows 
    if os.name == 'nt': 
        _ = os.system('cls') 

    # for mac and linux(here, os.name is 'posix') 
    else: 
        _ = os.system('clear') 

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
#00 IN THE CSV means that the country could not get fetched for some reason. Possibly due to too many queries to the server
# Scan index is a counter that goes up with each scan instance. It is needed for calculating average playercount

#Ensures that the gamemode is correct
def PrefixEnsure(string):
    prefix = re.split("_",string)[0].lower()+"_" 
    if(prefix == config["gamemode"].lower()):
        return True
    else:
        return False
#Scan all possible servers in the game for map matches.

def IpReader(IP):#returns datastack
    global x,y,z,w
    global internalips
    global averagelist
    try:
        x += 1

        server = a2s.info(IP,timeout=config["servertimeout"])
        rawdatastack = [IP[0],IP[1],server.map_name,server.player_count]

        if PrefixEnsure(rawdatastack[2]):
            averagelist.append(rawdatastack[3])
            rawdatastack.append(datetime.datetime.now().strftime(WriterTimeFormat))#adds Time to datastack
            internalips.append(f'!!!!{rawdatastack} has been added')
            w += 1
            try:
                response = requests.get(f"http://ip-api.com/json/{IP[0]}").json()
                region = response["countryCode"]
            except:
                region = "00"
            rawdatastack.append(region)
            return rawdatastack

    except (AttributeError,a2s.BrokenMessageError):
        y += 1

    except socket.timeout:
        z += 1
    except:
        print("Unknown Error")

    clear()
    print(internalmode)
    print("**************")
    print(f"Playercount for gamemode {config['gamemode']}: {sum(averagelist)}")
    print(f"Servers scanned : {x}")
    print(f"Acceptable servers : {w}")
    print(f"Server errors : {y}")
    print(f"Timeouts : {z}")
    print(f"scan index : {CurrentScanIndex}")
    print("**************")
    try:
        print(rawdatastack)
    except:
        print("NONE")
    
    for inp in internalips:
        print(inp)

def SlowScan():
    global internalips
    global x,y,z,w
    ips = []
    with valve.source.master_server.MasterServerQuerier(timeout=config["masterservertimeout"]) as msq:
        try:
            return [address for address in msq.find(gamedir=config["game"],empty=True,secure=True,region=config['region'])]
        except:
            print("Master server request timed out!")


def GetMaxScanIndex():
    global CurrentScanIndex
    CurrentScanIndex = 0
    try:
        with open(rawfilename,"r") as filedata:
            csvreader = csv.reader(filedata)
            for row in csvreader:
                if(len(row) == 7 and CurrentScanIndex<int(row[6])):
                    CurrentScanIndex = int(row[6])
    except:
        with open(rawfilename,"w",newline="") as filedata:
            print("NO FILE, MAKING NEW")
            csv.writer(filedata)

#Writes the incoming datastack to a csv line.
def CSVWriter(list):
    global CurrentScanIndex
    try:
        with open(rawfilename,"r") as filedata:
            print("read successful")
    except:
        with open(rawfilename,"w",newline="") as filedata:
            print("NO FILE, MAKING NEW")

    with open(rawfilename,"a",newline="") as filedata:
        CurrentScanIndex += 1
        for ip in list:
            csv.writer(filedata).writerow(ip+[CurrentScanIndex])#Adds scanindex to datastack

# First part of FastScan. Searches for IP's in the CSV.
def FastScan():
    iplist = []
    global internalips
    with open(rawfilename,"r") as filedata:
        csvreader = csv.reader(filedata)
        for ip in csvreader:
            if (ip[0],int(ip[1])) not in iplist:
                iplist.append((ip[0],(int(ip[1]))))
    return iplist
# Second part of fast scan. searches servers using incoming list.
def IpReaderMulti(list_ips=[]):
    global w,y,z,x
    global internalips
    global averagelist
    averagelist = []
    w,y,z,x = 0,0,0,0
    internalips = []
    ips = []
    try:
        for address in list_ips:
            datastack = IpReader(address)
            if(datastack != None):
                ips.append(datastack)
        return ips
    except:
        print("Master server request timed out!")

#Do not touch the iterator parameters if you already have csv data. It will affect the data in unpredictable ways.
def Iterator(delay=5,FastScansTillSlow=15):
    global internalmode
    global WriterTimeFormat
    internalmode = ""
    WriterTimeFormat = '%Y-%m-%d-%H:%M:%S'
    end = time() + config['timer']*60
    InternalPoint = FastScansTillSlow
    while time() < end or config['runforever']:
        if InternalPoint >= FastScansTillSlow:
            GetMaxScanIndex()
            internalmode = f"SLOW SEARCH : I scan the MASTERSERVER every {FastScansTillSlow*delay} minutes"
            CSVWriter(IpReaderMulti(SlowScan()))
            InternalPoint = 0
        else:
            GetMaxScanIndex()
            internalmode = f"FAST SEARCH : I scan the CSV for servers instead"
            CSVWriter(IpReaderMulti(FastScan()))
            InternalPoint += 1
            clear()
            print(f"Search Complete! Taking a break for {delay} minutes")
            sleep(delay*60)

    print( f"{config['timer']} : minutes complete")
#init
if __name__ == "__main__":
    rawfilename = os.path.join(dirname,config['filename'])
    Iterator()