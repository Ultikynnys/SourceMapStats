import matplotlib.pyplot as plt
import matplotlib.pyplot as plot
from math import pi
import numpy as np
import csv
import re
from datetime import datetime, timedelta
import copy
import os

#Parameters
filename = "output.csv" #file to read
StartCount, EndCount = (1,5)#map range where 1 is the most popular map
AverageDays = 4 #how many days each bar represents
StartDate = "2001-10-02-19:46:11" #date range to use
EndDate = "2077-10-02-22:46:11"  #date range to use
Format = '%Y-%m-%d-%H:%M:%S' # date format
ColorForOtherMaps = (0.5,0.5,0.5)
ColorIntensity = 25



def arraypadder(array,length):
    vb = len(array)
    if(vb < length):
        for i in range(length-vb):
            array = np.append(array,0.0)
    elif(vb > length):
        for i in range(vb-length):
            array = np.delete(array,-1)
    return array

def plotter():
    global StartCount
    global EndCount
    global AverageDays
    global previousmap
    global previoustime
    global previouscolor
    global mapcount
    global force
    force = False

    previouscolor = 0
    allmaps = {}
    mapindex = StartCount-1
    previousmap = None

    if(StartCount != EndCount+1):
        if StartCount < 1:
            StartCount = 1
        if EndCount < StartCount:
            EndCount = StartCount+1
        if AverageDays < 1:
            AverageDays = 1
    elif(StartCount < 0 ):
        StartCount = 1
        EndCount = 2
    else:
        EndCount = StartCount+1
    
    data = remdup(prefixremover(timechunker()))

    for day in data:
        x = []
        y = []
        ytotal = 0
        for map in data[day]:
            if map not in allmaps.keys():
                allmaps.update({map:[[],[],1]})
            allmaps[map][0].append(data[day][map])#y axis
            ytotal += data[day][map]
            allmaps[map][1].append(day)#x axis
        for map in data[day]:
            allmaps[map][2] = ytotal #max for this timespan
            
    allmaps2 = copy.deepcopy(allmaps)
    allmaps3 = copy.deepcopy(allmaps)
    mapcount = len(list(allmaps2.items())[StartCount:EndCount])
    Totalmapcount = len(list(allmaps2.items()))

    for map in allmaps2:
        allmaps2[map][0] = sum(allmaps2[map][0])
    allmaps2 = sorted(allmaps2.items(), key=lambda x:x[1],reverse=True)
    
    #create a filler for the other maps not displayed
    for map in allmaps2[StartCount:EndCount+1]:

        _YCUR = None

        for plc in allmaps2[StartCount:]:
            datadd = np.array(allmaps3[plc[0]][0])
            if(type(_YCUR) == type(None)):
                _YCUR = np.array(datadd)
            else:
                _YCUR = arraypadder(datadd,len(_YCUR))+_YCUR
        
        _X = allmaps3[map[0]][1]
        _Y = allmaps3[map[0]][0]

        _YNORMALIZED = arraypadder(_Y,len(_YCUR))/_YCUR
        plotdraw(_X,_YNORMALIZED,map[0])
    
    force = True
    try:
        
        plotdraw(previoustime,np.ones((len(previousmap),), dtype=int)-previousmap,"Other Maps")
    except:
        plotdraw(np.append(previoustime,previoustime[-1]),np.ones((len(previousmap),), dtype=int)-previousmap,"Other Maps")

    plt.grid(True)
    plt.legend()
    plot.title(f"Top {StartCount+mapcount} maps out of {Totalmapcount}")
    plt.show()
    

def ColorGen():
    global previouscolor
    global force
    global ColorForOtherMaps
    previouscolor += ColorIntensity/(mapcount+1)


    amplitude   = np.sin(previouscolor)
    amplitude2   = np.sin((previouscolor+2*pi/3))
    amplitude3   = np.sin((previouscolor+4*pi/3))
    if( not force):
        col = ((amplitude+1)/2, (amplitude2+1)/2, (amplitude3+1)/2)
    elif(ColorForOtherMaps == None):
        col = ((amplitude+1)/2, (amplitude2+1)/2, (amplitude3+1)/2)
    else:
        col = ColorForOtherMaps
    return col

def plotdraw(_X,_Y,_label):
    global previousmap
    global previoustime
    
    previoustime = _X
    
    col = ColorGen()

    try:
       previousmap = previousmap.tolist()
    except:
        1
    
    if(previousmap == None):
        plt.bar(np.arange(len(_Y)),_Y, color=col, label=_label)
        previousmap = np.array(_Y)
    else:
        vb = len(previousmap[0:len(_Y)])
        dn = len(_Y)       
        if(vb < dn):
            b = (np.zeros(dn-vb).tolist())
            for i in b:
                previousmap.append(0.0) 
        plt.bar(np.arange(len(_Y)),_Y, color=col, label=_label,bottom=previousmap[0:len(_Y)])
        previousmap = previousmap[0:len(_Y)]+np.array(_Y)

    a = _X[0]
    b = _X[int(len(_X) / 2)]
    c = _X[-1]
    a = datetime.strptime(a,Format)
    b = datetime.strptime(b,Format)
    c = datetime.strptime(c,Format)
    plt.yticks([0,.125,.25, 0.5,.75, 1], ['0%','12.5%','25%', '50%','75%', '100%'])
    plt.xticks(np.linspace(plt.xlim()[0],plt.xlim()[1],3),[a.strftime("%Y/%m/%d"),b.strftime("%Y/%m/%d"),c.strftime("%Y/%m/%d")])

def timechunker():
    startdateobject = datetime.strptime(StartDate, Format)
    enddateobject = datetime.strptime(EndDate, Format)
    chunkedarray = []
    chunkedarray_index = 0
    chunktime = timedelta(AverageDays,0,0,0,0,0,0)#Interval means days here
    initialtime = None
    dirname = os.path.dirname(os.path.realpath(__file__))
    rawfilename = os.path.join(dirname,filename)
    with open(rawfilename,"r") as serverdata:
        rawrawstat = csv.reader(serverdata)
        rawstat = []
        for row in rawrawstat:
            datetime_object = datetime.strptime(row[4], Format)
            if(datetime_object > startdateobject and datetime_object < enddateobject):
                rawstat.append(row)
        
        
        for row in rawstat:
            if(initialtime == None):
                initialtime = datetime.strptime(row[4], Format) #get the first row time
            datetime_object = datetime.strptime(row[4], Format)
            try:
                chunkedarray[chunkedarray_index].append(row)
            except:
                chunkedarray.append([])
                chunkedarray[chunkedarray_index].append(row)
            else:
                if(datetime_object > initialtime+chunktime):
                    chunkedarray_index += 1
                    chunktime += timedelta(AverageDays,0,0,0,0,0,0)

    return chunkedarray

def prefixremover(data):
    for timelist in data:
        for row in timelist:
            row[2] = namecutter(row[2])
    return data

def remdup(data):
    datatemp2 = {}
    for timelist in data:
        firsttime = timelist[0][4]
        datatemp2[firsttime] = {}
        for row in timelist:
            if(row[2] in datatemp2[firsttime].keys()):
                datatemp2[firsttime].update({row[2]:datatemp2[firsttime].get(row[2])+int(row[3])})#adds player count to dict
            else:
                datatemp2[firsttime].update({row[2]:int(row[3])})#adds a new dict with the player count
    return datatemp2

def namecutter(x,filter=["fix","final","redux","1","2","3","4","5","6","7","8","9","0","finished","remake"]):
    y = re.search("dr_",x).start()
    z = x[y:]
    e = z.split("_")
    try:
        rex = f"{e[0]}_{e[1]}_{e[2]}"
    except IndexError:
        rex = f"{e[0]}_{e[1]}"
        return rex
    for n in filter:
        if n in e[2]:
            rex = f"{e[0]}_{e[1]}"
    return rex

if __name__ == "__main__":##when launched directly.
    plotter()