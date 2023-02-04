import operator
import numpy as np
from collections import OrderedDict
def arraypadder(arr1,arr2):
    arr1 = list(arr1)
    arr2 = list(arr2)
    while(len(arr1)!=len(arr2)):
        if(len(arr1)<len(arr2)):
            arr1.append(0)
        else:
            arr2.append(0)
        #print("y")
    return (np.array(arr1),np.array(arr2))

def weakfiller(dict,stringlist):
    newstringlist = []
    cycles = 0
    for string in stringlist:
        cycles += 1
        #print(cycles)
        for key in dict.keys():
            if(string.lower() in key):
                newstringlist.append(key)
        
    return newstringlist

def arrayrectifier(arrlist):#pads 2d arrays with zeroes so that length of rows and columns is the same
    largestArray = 0
    for array in arrlist:
        if(len(array)>largestArray):
            largestArray = len(array)
    
    for array in arrlist:
        while(len(array)<largestArray):
            array.append(0)
            #print("y")
    return arrlist


def arrayreduction(arr1,arr2):
    arr1 = list(arr1)
    arr2 = list(arr2)
    while(len(arr1)!=len(arr2)):
        if(len(arr1)<len(arr2)):
            arr2.pop()
        else:
            arr1.pop()
        #print("y")
    return (np.array(arr1),np.array(arr2))

def dictmerger(dictlist):
    dict1 = None
    dict2 = {}
    for dict1 in dictlist:
        for key in dict1:
            if(key in dict2.keys()):
                dict2[key] = dict1.get(key, 0) + dict2.get(key, 0)
            else:
                dict2[key] = dict1.get(key, 0)
    return dict2

def dictmax(dict1,ammount): # get biggest values from dict in order from biggest to smallest
    tempdict = dict1
    maxrray = []
    for i in range(ammount):
        if(len(tempdict.items()) != 0):
            maxrray.append(max(tempdict.items(), key=operator.itemgetter(1))[0])
            tempdict.pop(maxrray[-1])
    return maxrray

def dictlimx(dict1,keylist): # remove keyvalue pairs not in keylist
    dict2 = {}
    for key in keylist:
        if(key in dict1.keys()):
            x = dict1[key]
            dict2.update({key:x})
    return dict2

