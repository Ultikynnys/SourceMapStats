import matplotlib.pyplot as plt

from math import pi
import numpy as np
import csv
import re
from datetime import datetime, timedelta
import os
from R6LIB import *
import Parameters as p
import argparse

#Arguments
parser = argparse.ArgumentParser(description="SourceMapStats reader. Defaults in parameter.py",formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("-f", "--filename", type=str, help="Filename for reading",default=p.Filename)
parser.add_argument("-log", "--filelog", type=str, help="Filename for writing text stats",default=p.Filelog)
parser.add_argument("-fp", "--filenamepng", type=str, help="Output png of bar graphs",default=p.Filenamepng)
parser.add_argument("-mts", "--mapstoshow", type=int, help="How many maps to show",default=p.MapsToShow)
parser.add_argument("-sd", "--startdate", type=str, help="Specify start date of data",default=p.Start_Date)
parser.add_argument("-ed", "--enddate", type=str, help="Specify end date of data",default=p.End_Date)
parser.add_argument("-only", "--onlymaps", type=list, help="word whitelist",default=p.OnlyMapsContaining)
parser.add_argument("-fl", "--filter", type=list, help="Words to remove from maps",default=p.WordFilter)
parser.add_argument("-vf", "--versionfilter", type=list, help="Versions to remove",default=p.VersionFilter)
parser.add_argument("-pc", "--percision", type=int, help="Decimal place for figure",default=p.Percision)
parser.add_argument("-lb", "--labeltransparency", type=int, help="Opacity of name label",default=p.LabelTransparency)
parser.add_argument("-od", "--outputdimensions", type=tuple, help="Output dimensions of png",default=p.OutputDimensions)
parser.add_argument("-oth", "--othermapcolor", type=tuple, help="Other maps color",default=p.ColorForOtherMaps)
parser.add_argument("-avg", "--averagedays", type=int, help="How many days each bar represents",default=p.AverageDays)
parser.add_argument("-ci", "--colorintensity", type=int, help="how much color changes per map",default=p.ColorIntensity)
parser.add_argument("-ad", "--axisdates", type=int, help="How many dates to display on the x axis",default=p.XaxisDates)
parser.add_argument("-nf", "--nofilter", type=bool, help="Disables filtering completely",default=p.NoFilter)
args = parser.parse_args()
config = vars(args)
#The reader assumes that the CSV input dates are in this format.
ReaderTimeFormat = '%Y-%m-%d-%H:%M:%S'
def IndexAverage(Rawdata):
    CurrentScanIndex = 0
    CurrentScanIndexlist = []
    ScanAverage = []
    for scan in Rawdata:
        try:
            curint = int(scan[6])
            if(CurrentScanIndex == curint):
                CurrentScanIndexlist.append(int(scan[3]))
            else:
                sumR = sum(CurrentScanIndexlist)
                if(sumR != 0):
                    ScanAverage.append(sum(CurrentScanIndexlist))
                CurrentScanIndexlist = []
                CurrentScanIndex = curint
                CurrentScanIndexlist.append(int(scan[3]))
        except:
            None

    if(len(ScanAverage) != 0):
        return round(sum(ScanAverage)/len(ScanAverage),config['percision'])
    else:
        return 0
        

    
def RawData():
    iplist = []
    dirname = os.path.dirname(os.path.realpath(__file__))
    rawfilename = os.path.join(dirname,config["filename"])
    with open(rawfilename,"r") as filedata:
        csvreader = csv.reader(filedata)
        for row in csvreader:
            iplist.append(row)
    return iplist

def ColorGen():
    global previouscolor
    global force
    previouscolor += 2*pi/(mapcount)


    amplitude   = np.sin(previouscolor*config['colorintensity'])
    amplitude2   = np.sin((previouscolor+2*pi/3)*config['colorintensity'])
    amplitude3   = np.sin((previouscolor+4*pi/3)*config['colorintensity'])
    if( not force):
        col = ((amplitude+1)/2, (amplitude2+1)/2, (amplitude3+1)/2)
    elif(config['othermapcolor'] == None):
        col = ((amplitude+1)/2, (amplitude2+1)/2, (amplitude3+1)/2)
    else:
        col = config['othermapcolor']
    return col
#Using MatPlotLib this will draw the actual visual side of the output.
def plotdraw(_X,_label):

    global previousmap
    global percentageSanity
    global otherhandle
    global otherlabel
    col = ColorGen()

    if(force):
        labelpercn = round(100-percentageSanity,config['percision'])
        otherlabel = ((f"{_label} : {labelpercn} %",labelpercn))
        previousmap = previousmap.tolist()
        otherhandle = (ax.bar(np.arange(len(_Y)),_Y, color=col,bottom=previousmap[0:len(_Y)],width=np.array(YMAX)/max(YMAX)))
        previousmap = previousmap+np.array(_Y)
        percentageSanity += labelpercn


    elif(type(previousmap) is not type(None)):
        labelpercn = round(100*(sum(_Y)/sum(YMAX)),config['percision'])
        Labels.append((f"{_label} : {labelpercn} %",labelpercn))
        previousmap = previousmap.tolist()
        Handles.append(ax.bar(np.arange(len(_Y)),_Y/YMAX, color=col,bottom=previousmap[0:len(_Y)],width=np.array(YMAX)/max(YMAX)))
        previousmap = previousmap+np.array(_Y/YMAX)
        percentageSanity += labelpercn

    else:
        labelpercn = round(100*(sum(_Y)/sum(YMAX)),config['percision'])
        Labels.append((f"{_label} : {labelpercn} %",labelpercn))
        Handles.append(ax.bar(np.arange(len(_Y)),_Y/YMAX, color=col,width=np.array(YMAX)/max(YMAX)))
        previousmap = np.array(_Y/YMAX)
        percentageSanity += labelpercn


    ax.set_yticks([0,.125,.25, 0.5,.75, 1], ['0%','12.5%','25%', '50%','75%', '100%'])

    dateformat = "%Y/%m/%d"
    xtimes = [datetime.strptime(y,ReaderTimeFormat).strftime(dateformat) for y in _X]
    ax.set_xticks(np.linspace(ax.get_xlim()[0],ax.get_xlim()[1],len(xtimes)),xtimes)
#"Chunks" dates together and averages them into bars.
def timechunker(AverageDays):
    startdateobject = datetime.strptime(config["startdate"], '%Y-%m-%d')
    enddateobject = datetime.strptime(config["enddate"], '%Y-%m-%d')
    chunkedarray = []
    chunkedarray_index = 0
    chunktime = timedelta(AverageDays,0,0,0,0,0,0)#Interval means days here
    initialtime = None
    rawstat = []
    for row in CSVDATA:
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
                chunktime += timedelta(AverageDays,0,0,0,0,0,0)
                #print(chunktime)

    return chunkedarray
#Wrapper around SuffixFilter
def SuffixRemover(data):
    for timelist in data:
        for row in timelist:
            if(not config['nofilter']):
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
def SuffixFilter(_RawMapName):
    RegexCutter = r"(vsh_dr|[a-zA-Z0-9]*?)(?:_([a-zA-Z0-9]*))(?:_([a-zA-Z0-9]*))?"
    FilterPattern = r'(?:['+p.VersionFilter+r']\d)|(?:\d['+p.VersionFilter+r'])|(?:'+p.WordFilter+r')'
    m = re.match(RegexCutter, _RawMapName)
    if(m.group(3) == None or re.search(FilterPattern,m.group(3).lower())):
        return m[1]+"_"+m[2]
    else:
        return m[0]
#Does the heavy lifting and makes sure that data is correct
def plotter():
    print("START")
    global fig
    global ax
    global previousmap
    global previouscolor
    global mapcount
    global force
    global Labels
    global Handles
    global YMAX
    global _Y
    global percentageSanity
    global CSVDATA
    global otherhandle
    global otherlabel

    print("STEP 1")
    CSVDATA = RawData()
    fig,ax = plt.subplots(figsize=config['outputdimensions'])
    percentageSanity = 0
    Labels = []
    Handles = []
    force = False
    previouscolor = 0
    previousmap = None

    print("STEP 2")
    predata = timechunker(config['averagedays'])
    print("STEP 3")
    DailyAverageVal = IndexAverage(CSVDATA)
    
    print("STEP 4")

    rawdata = SuffixRemover(predata)
    nonsquaredata = DuplicateMerger(rawdata)
    combineddata = dictmerger(nonsquaredata.values())
    mapcount = len(combineddata)
    WhiteListedData = (weakfiller(combineddata,config["onlymaps"]))
    FilteredMaps = dictlimx(combineddata,WhiteListedData)
    TopMaps = dictmax(FilteredMaps,config["mapstoshow"])
    squareddata = {datex[0]:dictpadder(datex[1],TopMaps) for datex in nonsquaredata.items()}#Adds zeroes to maps that do not have any data


    print("STEP 5")
    YDICT = [dictlimx(datachunk,TopMaps) for datachunk in squareddata.values()]
    YLIST = arrayrectifier([list(x.values()) for x in YDICT])
    Transpose_YLIST = np.array(np.transpose(YLIST))
    YMAX = [sum(x.values()) for x in squareddata.values()]#max player count for each bar ascending order by date


    print("STEP 6")

    timerange = list(squareddata.keys())#from earliest to latest dates in
    XaxisDates2 = config['axisdates']-1
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
    for mapname,YCUR in NAMEY:
        _Y = YCUR
        plotdraw(X,mapname)

    print("STEP 7")
    othermapscreated = False
    if(type(previousmap) is not type(None) and len(TopMaps) != mapcount):
        force = True
        othermapscreated = True
        _Y = np.ones((len(previousmap)), dtype=int)-previousmap
        plotdraw(X,"Other Maps")
        ax.set_title(f"Top {len(TopMaps)} Maps with keywords {config['onlymaps']} out of {mapcount} \nBar width as relative Player count")

        ax.grid(True)

    elif(mapcount == 0):
        ax.set_title(f"None maps found using keywords {config['onlymaps']}")

    else:
        ax.set_title(f"Top {len(TopMaps)} Maps with keywords {config['onlymaps']} out of {mapcount} \nBar width as relative Player count")


    print("STEP 8")

    legenddata = zip(Handles,Labels)
    legenddata2 = list(reversed([{"Handle":i,"Label":x[0],"Percentage":x[1]} for i,x in legenddata]))
    try:
        legendlabels = [otherlabel[0]]+[x["Label"] for x in legenddata2]
        legendhandles = [otherhandle]+[x["Handle"] for x in legenddata2]
    except:
        legendlabels = [x["Label"] for x in legenddata2]
        legendhandles = [x["Handle"] for x in legenddata2]
    
    
    
    print("STEP 9")

    with open(config["filelog"], 'w') as f:#writes the text file
        for label in legendlabels:
            f.write(f"{label}")
            f.write('\n')

    box = ax.get_position()
    ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])
    if(DailyAverageVal > 5):    
        ax.text(0.05, 0.95, f'Average Daily Player Count : {DailyAverageVal}',
    verticalalignment='bottom', horizontalalignment='left',
    transform=ax.transAxes,
    color='Black', fontsize=15)
    ax.grid(True)      
    ax.legend(legendhandles,legendlabels,framealpha=config['labeltransparency'],loc='center left', bbox_to_anchor=(1, 0.5))
    dirname = os.path.dirname(os.path.realpath(__file__))
    rawfilename = os.path.join(dirname,config["filenamepng"])
    print("FINISHED")
    fig.savefig(rawfilename)
    plt.show()
#init
if __name__ == "__main__":
    plotter()