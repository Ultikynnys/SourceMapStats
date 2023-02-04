import matplotlib.pyplot as plt
import matplotlib.pyplot as plot
from math import pi
import numpy as np
import csv
import re
from datetime import datetime, timedelta
import copy
import os
from R6LIB import dictmerger,dictmax,dictlimx,arrayrectifier,weakfiller
from write import Format

#Parameters
filename = "output.csv" #file to read
filenamepng = "output.png" #name of the figure
MapsToShow = 0 #How many maps to show in order of popularity
AverageDays = 2 #how many days each bar represents
StartDate = "2001-10-02" #date range to use
EndDate = "2025-10-02"  #date range to use
XaxisDates = 5 #How many dates to show in the figure. Maximum is the amount of bars. Minimum is 2
OnlyMapsContaining = [""] #EX.["Playstation","Bazinga","Lazy"]
ColorForOtherMaps = (0.5,0.5,0.5)
ColorIntensity = 25
filter = ["fix","final","redux","1","2","3","4","5","6","7","8","9","0","finished","remake"] #Will not consider maps with these suffixes to be unique

def ColorGen():
    global previouscolor
    global force
    global ColorForOtherMaps
    previouscolor += 2*pi/(mapcount)


    amplitude   = np.sin(previouscolor*ColorIntensity)
    amplitude2   = np.sin((previouscolor+2*pi/3)*ColorIntensity)
    amplitude3   = np.sin((previouscolor+4*pi/3)*ColorIntensity)
    if( not force):
        col = ((amplitude+1)/2, (amplitude2+1)/2, (amplitude3+1)/2)
    elif(ColorForOtherMaps == None):
        col = ((amplitude+1)/2, (amplitude2+1)/2, (amplitude3+1)/2)
    else:
        col = ColorForOtherMaps
    return col

def plotdraw(_X,_Y,_label):
    Labels.append(_label)
    global previousmap
    col = ColorGen()

    if(type(previousmap) is not type(None)):
        previousmap = previousmap.tolist()
        Handles.append(plt.bar(np.arange(len(_Y)),_Y, color=col,bottom=previousmap[0:len(_Y)]))
        previousmap = previousmap[0:len(_Y)]+np.array(_Y)

    else:
        Handles.append(plt.bar(np.arange(len(_Y)),_Y, color=col))
        previousmap = np.array(_Y)


    
    plt.yticks([0,.125,.25, 0.5,.75, 1], ['0%','12.5%','25%', '50%','75%', '100%'])

    dateformat = "%Y/%m/%d"
    xtimes = [datetime.strptime(y,Format).strftime(dateformat) for y in _X]
    plt.xticks(np.linspace(plt.xlim()[0],plt.xlim()[1],len(xtimes)),xtimes)

def timechunker():
    startdateobject = datetime.strptime(StartDate, '%Y-%m-%d')
    enddateobject = datetime.strptime(EndDate, '%Y-%m-%d')
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
            if(len(chunkedarray)-1 == chunkedarray_index):
                chunkedarray[chunkedarray_index].append(row)
            else:
                chunkedarray.append([])
                chunkedarray[chunkedarray_index].append(row)

            if(datetime_object > initialtime+chunktime):
                chunkedarray_index += 1
                while(datetime_object > initialtime+chunktime):
                    chunktime += timedelta(AverageDays,0,0,0,0,0,0)
                    #print(chunktime)

    return chunkedarray

def SuffixRemover(data):
    for timelist in data:
        for row in timelist:
            row[2] = SuffixFilter(row[2])
    return data

def DuplicateMerger(data):
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

def SuffixFilter(x):
    prefix = re.split("_",x)[0]
    y = re.search(prefix,x).start()
    z = x[y:]
    e = z.split("_")
    if(len(e) == 3):
        rex = f"{e[0]}_{e[1]}_{e[2]}"
    else:
        rex = f"{e[0]}_{e[1]}"
        return rex
    for n in filter:
        if n in e[2]:
            rex = f"{e[0]}_{e[1]}"
    return rex

def plotter():
    global StartCount
    global EndCount
    global AverageDays
    global previousmap
    global previouscolor
    global mapcount
    global force
    global tripletime
    global Labels
    global Handles

    Labels = []
    Handles = []
    force = False

    previouscolor = 0
    allmaps = {}
    previousmap = None

    
    predata = timechunker()
    rawdata = SuffixRemover(predata)
    data = DuplicateMerger(rawdata)
    combineddata = dictmerger(data.values())
    mapcount = len(combineddata)

    WhiteListedData = (weakfiller(combineddata,OnlyMapsContaining))

    FilteredMaps = dictlimx(combineddata,WhiteListedData)
    
    TopMaps = dictmax(FilteredMaps,MapsToShow)#Top maps



    YDICT = [dictlimx(datachunk,TopMaps) for datachunk in data.values()]
    YLIST = arrayrectifier([list(x.values()) for x in YDICT])
    Transpose_YLIST = np.array(np.transpose(YLIST))
    YMAX = [sum(x.values()) for x in data.values()]#max player count for each bar ascending order by date
    timerange = list(data.keys())#from earliest to latest dates in
    XaxisDates2 = XaxisDates-1
    X = []
    if(XaxisDates2<1):
        XaxisDates2 = 1

    if(XaxisDates2>len(timerange)):
        X = timerange
    else:
        X = [timerange[int((len(timerange))*((x)/XaxisDates2))] for x in range(XaxisDates2)]
        X.append(timerange[-1])



    #create a filler for the other maps not displayed
    NAMEY = zip(TopMaps,Transpose_YLIST)
    for mapname,_Y in NAMEY:
        _YNORMALIZED = _Y/YMAX
        plotdraw(X,_YNORMALIZED,mapname)

    force = True
    if(len(TopMaps) != mapcount and type(previousmap) is not type(None)):
        plotdraw(X,np.ones((len(previousmap)), dtype=int)-previousmap,"Other Maps")
        plot.title(f"Top {len(TopMaps)} Maps with keywords {OnlyMapsContaining} out of {mapcount}")
        plt.legend(list(reversed(Handles)),list(reversed(Labels)))
    else:
        plot.title(f"None maps found using keywords {OnlyMapsContaining}")
    plt.grid(True)

    dirname = os.path.dirname(os.path.realpath(__file__))
    rawfilename = os.path.join(dirname,filenamepng)
    plt.savefig(rawfilename)
    plt.show()




if __name__ == "__main__":##when launched directly.
    plotter()