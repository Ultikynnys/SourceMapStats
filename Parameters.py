#Basic Parameters
Game = "tf" # cstrike,tf,hl2,hl2mp,csgo
Gamemode = "dr_"# Gamemode dr_,pl_,ctf_
Filename = "output.csv" #file to read and write
Filenamepng = "output.png" #name of the figure
MapsToShow = 5 #How many maps to show in order of popularity

#Reader date range to read
Start_Date = "2001-10-02" #date range to use
End_Date = "2040-10-02"  #date range to use

#Reader Filters
OnlyMapsContaining = [""] # [""] means all maps. EX.["Playstation","Bazinga","Lazy"] or gamemodes like ["ze_"]
wordfilter = ["fix","final","redux","finished","remake","optimized","finalplus","mini"] #Will not consider maps with these additional words to be unique
versionfilter = ["v","b","a","rc","x","f"] #Will not consider maps with these version suffixes to be unique. "v" would mean v0-v99,v and 0v-99v

#Advanced Parameters
timeout_query = 1 #How long to wait for each individual server to respond during slow search
timeout_master = 1 #How long to wait for the master server to respond during slow search
regionserver= "all" #Looks for servers in a specfic region
RuntimeMinutes = 60 #for how many minutes to run.
RunForever = True #True will run forever. Set to False to use runtime 
AverageDays = 2 #how many days each bar represents
ColorForOtherMaps = (0.5,0.5,0.5) # Color of the "Other Maps bars"
ColorIntensity = 25 # Changes the rate at which the colors change Low being very small changes and High being very big changes.
XaxisDates = 5 #How many dates to show in the figure. Maximum is the amount of bars. Minimum is 2