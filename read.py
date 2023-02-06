import matplotlib.pyplot as plt
from math import pi
import numpy as np
import csv
import re
from datetime import datetime, timedelta
import os
from R6LIB import dictmerger,dictmax,dictlimx,arrayrectifier,weakfiller
import Parameters as p

#The reader assumes that the CSV input dates are in this format.
ReaderTimeFormat = '%Y-%m-%d-%H:%M:%S'
#Makes a nice rainbow


#An estimation of Daily Average count for a given gamemode in a given game. Average of Daily Averages using unique ips.
#Takes into account writer pace and server count.
def DailyAverage(dailydata):
    totalsum = 0
    for day in dailydata:
        dailydict = {}
        for datapoint in day:
            dataip = str(datapoint[0])+":"+str(datapoint[1])
            playercount = int(datapoint[3])
            if(dataip in dailydict.keys()):
                currplayer = dailydict[dataip]
                dailydict.update({dataip:playercount+currplayer})
            else:
                dailydict.update({dataip:playercount})
        RepaterCount = len(day)/len(dailydict.keys())

        
        #add the average playercount for the day into the total sum
        totalsum+=sum(dailydict.values())/(RepaterCount)
    #take the average of the totalsum relative to days
    return round(totalsum/len(dailydata),p.Percision)




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
def plotdraw(_X,_label):
    global previousmap
    global percentageSanity
    col = ColorGen()

    if(force):
        labelpercn = round(100-percentageSanity,p.Percision)
        Labels.append((f"{_label} : {labelpercn} %",labelpercn))
        previousmap = previousmap.tolist()
        Handles.append(ax.bar(np.arange(len(_Y)),_Y, color=col,bottom=previousmap[0:len(_Y)],width=np.array(YMAX)/max(YMAX)))
        previousmap = previousmap+np.array(_Y)
        percentageSanity += labelpercn
        #print(percentageSanity)


    elif(type(previousmap) is not type(None)):
        labelpercn = round(100*(sum(_Y)/sum(YMAX)),p.Percision)
        Labels.append((f"{_label} : {labelpercn} %",labelpercn))
        previousmap = previousmap.tolist()
        Handles.append(ax.bar(np.arange(len(_Y)),_Y/YMAX, color=col,bottom=previousmap[0:len(_Y)],width=np.array(YMAX)/max(YMAX)))
        previousmap = previousmap+np.array(_Y/YMAX)
        percentageSanity += labelpercn

    else:
        labelpercn = round(100*(sum(_Y)/sum(YMAX)),p.Percision)
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
    startdateobject = datetime.strptime(p.Start_Date, '%Y-%m-%d')
    enddateobject = datetime.strptime(p.End_Date, '%Y-%m-%d')
    chunkedarray = []
    chunkedarray_index = 0
    chunktime = timedelta(AverageDays,0,0,0,0,0,0)#Interval means days here
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
                    chunktime += timedelta(AverageDays,0,0,0,0,0,0)
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

    fig,ax = plt.subplots(figsize=p.OutputDimensions)

    percentageSanity = 0
    Labels = []
    Handles = []
    force = False
    previouscolor = 0
    previousmap = None
    RawVersionFitler = []

    for version in p.versionfilter:
        RawVersionFitler.append(version)
        for d in range(10):
            RawVersionFitler.append(version+str(d))
    p.wordfilter.extend(RawVersionFitler)

    predata = timechunker(p.AverageDays)
    DailyAverageVal = DailyAverage(timechunker(1))
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
    for mapname,YCUR in NAMEY:
        _Y = YCUR
        plotdraw(X,mapname)


    othermapscreated = False
    if(type(previousmap) is not type(None) and len(TopMaps) != mapcount):
        force = True
        othermapscreated = True
        _Y = np.ones((len(previousmap)), dtype=int)-previousmap
        plotdraw(X,"Other Maps")
        ax.set_title(f"Top {len(TopMaps)} Maps with keywords {p.OnlyMapsContaining} out of {mapcount} \nBar width as relative Player count")

        ax.grid(True)

    elif(mapcount == 0):
        ax.set_title(f"None maps found using keywords {p.OnlyMapsContaining}")

    else:
        ax.set_title(f"Top {len(TopMaps)} Maps with keywords {p.OnlyMapsContaining} out of {mapcount} \nBar width as relative Player count")

    legenddata = zip(Handles,Labels)
    legenddata2 = [{"Handle":i,"Label":x[0],"Percentage":x[1]} for i,x in legenddata]
    legenddata2.sort(key=lambda k : k['Percentage'])
    legendlabels = [x["Label"] for x in legenddata2]
    legendhandles = [x["Handle"] for x in legenddata2]

    if(othermapscreated):
        legendlabels.insert(0,legendlabels.pop(-1))
        legendhandles.insert(0,legendhandles.pop(-1))
    
    
    

    box = ax.get_position()
    ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])
    ax.text(0.05, 0.95, f'Average Daily Player Count : {DailyAverageVal}',
    verticalalignment='bottom', horizontalalignment='left',
    transform=ax.transAxes,
    color='Black', fontsize=15)
    ax.grid(True)      
    ax.legend(legendhandles,legendlabels,framealpha=p.LabelTransparency,loc='center left', bbox_to_anchor=(1, 0.5))
    dirname = os.path.dirname(os.path.realpath(__file__))
    rawfilename = os.path.join(dirname,p.Filenamepng)

    fig.savefig(rawfilename)
    plt.show()
#init
if __name__ == "__main__":
    plotter()