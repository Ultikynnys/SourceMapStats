import matplotlib.pyplot as plt
from math import pi
import numpy as np
import csv
import re
from statistics import mean 
from datetime import datetime, timedelta
import os
from R6LIB import dictmerger,dictmax,dictlimx,arrayrectifier,weakfiller
import Parameters as p

#The reader assumes that the CSV input dates are in this format.
ReaderTimeFormat = '%Y-%m-%d-%H:%M:%S'
#Makes a nice rainbow
def ColorGen():
    global previouscolor
    global force
    previouscolor += 2*pi/(mapcount)


    amplitude   = np.sin(previouscolor*p.ColorIntensity)
    amplitude2   = np.sin((previouscolor+2*pi/3)*p.ColorIntensity)
    amplitude3   = np.sin((previouscolor+4*pi/3)*p.ColorIntensity)
    if( not force):
        col = ((amplitude+1)/2, (amplitude2+1)/2, (amplitude3+1)/2)
    elif(p.ColorForOtherMaps == None):
        col = ((amplitude+1)/2, (amplitude2+1)/2, (amplitude3+1)/2)
    else:
        col = p.ColorForOtherMaps
    return col
#Using MatPlotLib this will draw the actual visual side of the output.
def plotdraw(_X,_Y,_label):
    Labels.append(f"{_label} : {round(100*mean(_Y),1)} %")
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
    xtimes = [datetime.strptime(y,ReaderTimeFormat).strftime(dateformat) for y in _X]
    plt.xticks(np.linspace(plt.xlim()[0],plt.xlim()[1],len(xtimes)),xtimes)
#"Chunks" dates together and averages them into bars.
def timechunker():
    startdateobject = datetime.strptime(p.Start_Date, '%Y-%m-%d')
    enddateobject = datetime.strptime(p.End_Date, '%Y-%m-%d')
    chunkedarray = []
    chunkedarray_index = 0
    chunktime = timedelta(p.AverageDays,0,0,0,0,0,0)#Interval means days here
    initialtime = None

    dirname = os.path.dirname(os.path.realpath(__file__))
    rawfilename = os.path.join(dirname,p.Filename)
    with open(rawfilename,"r") as serverdata:
        rawrawstat = csv.reader(serverdata)
        rawstat = []
        for row in rawrawstat:
            datetime_object = datetime.strptime(row[4], ReaderTimeFormat)
            if(datetime_object > startdateobject and datetime_object < enddateobject):
                rawstat.append(row)
        
        
        for row in rawstat:

            if(initialtime == None):
                initialtime = datetime.strptime(row[4], ReaderTimeFormat) #get the first row time

            datetime_object = datetime.strptime(row[4], ReaderTimeFormat)
            if(len(chunkedarray)-1 == chunkedarray_index):
                chunkedarray[chunkedarray_index].append(row)
            else:
                chunkedarray.append([])
                chunkedarray[chunkedarray_index].append(row)

            if(datetime_object > initialtime+chunktime):
                chunkedarray_index += 1
                while(datetime_object > initialtime+chunktime):
                    chunktime += timedelta(p.AverageDays,0,0,0,0,0,0)
                    #print(chunktime)

    return chunkedarray
#Wrapper around SuffixFilter
def SuffixRemover(data):
    for timelist in data:
        for row in timelist:
            row[2] = SuffixFilter(row[2])
    return data
#Merges maps that are to be considered the same map.
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
#Removes worfilter suffixes from input strings
def SuffixFilter(x):
    newname = ""
    prefix = re.split("_",x)[0]
    y = re.search(prefix,x).start()
    z = x[y:]
    e = z.split("_")

    for n in p.wordfilter:
        for idx, n2 in enumerate(e):
            if(n == n2 or n == n2[::-1]):
                e.pop(idx)
            elif(n in n2 and len(n)>1):
                e.pop(idx)
    
    for e2 in e:
        if(newname != ""):
            newname += f"_{e2}"
        else:
            newname +=e2

    return newname
#Does the heavy lifting and makes sure that data is correct
def plotter():
    global previousmap
    global previouscolor
    global mapcount
    global force
    global Labels
    global Handles

    Labels = []
    Handles = []
    force = False
    previouscolor = 0
    allmaps = {}
    previousmap = None
    RawVersionFitler = []

    for version in p.versionfilter:
        RawVersionFitler.append(version)
        for d in range(10):
            RawVersionFitler.append(version+str(d))
    p.wordfilter.extend(RawVersionFitler)

    predata = timechunker()
    rawdata = SuffixRemover(predata)
    data = DuplicateMerger(rawdata)
    combineddata = dictmerger(data.values())
    mapcount = len(combineddata)
    WhiteListedData = (weakfiller(combineddata,p.OnlyMapsContaining))
    FilteredMaps = dictlimx(combineddata,WhiteListedData)
    TopMaps = dictmax(FilteredMaps,p.MapsToShow)

    YDICT = [dictlimx(datachunk,TopMaps) for datachunk in data.values()]
    YLIST = arrayrectifier([list(x.values()) for x in YDICT])
    Transpose_YLIST = np.array(np.transpose(YLIST))
    YMAX = [sum(x.values()) for x in data.values()]#max player count for each bar ascending order by date
    timerange = list(data.keys())#from earliest to latest dates in
    XaxisDates2 = p.XaxisDates-1
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

    if(type(previousmap) is not type(None) and len(TopMaps) != mapcount):
        force = True
        plotdraw(X,np.ones((len(previousmap)), dtype=int)-previousmap,"Other Maps")
        plt.title(f"Top {len(TopMaps)} Maps with keywords {p.OnlyMapsContaining} out of {mapcount}")
        plt.legend(list(reversed(Handles)),list(reversed(Labels)))
        plt.grid(True)

    elif(mapcount == 0):
        plt.title(f"None maps found using keywords {p.OnlyMapsContaining}")

    else:
        plt.title(f"Top {len(TopMaps)} Maps with keywords {p.OnlyMapsContaining} out of {mapcount}")
        plt.legend(list(reversed(Handles)),list(reversed(Labels)))
        plt.grid(True)

    dirname = os.path.dirname(os.path.realpath(__file__))
    rawfilename = os.path.join(dirname,p.Filenamepng)
    plt.savefig(rawfilename)
    plt.show()

#init
if __name__ == "__main__":
    plotter()