#Basic Parameters
Game = "tf" # cstrike,tf,hl2,hl2mp,csgo
Gamemode = "dr_"# Gamemode dr_,pl_,ctf_
Filename = "output.csv" #file to read and write
Filelog = "stats.log" #file to read and write
Filenamepng = "output.png" #name of the figure
MapsToShow = 15 #How many maps to show in order of popularity

#Reader date range to read
Start_Date = "2001-10-02" #date range to use
End_Date = "2040-10-02"  #date range to use

#Filters for fixing the map names
versionfilter = "abcvdf" # looks for version names with the following letters. Ex. "a" would look for a0-9 or 0-9a
wordfilter = "final|redux|rc|test|fix|skial|censored|blw|vrs|alpha|beta|fin" #Suffixes to remove Ex. Final
OnlyMapsContaining = [""] #Empty for all maps. Only show maps containing string value Ex. ["Bazinga","Playstation"]

#Advanced Parameters
OutputDimensions = (12,6)
LabelTransparency = 0.5
Percision = 2 #How many digits to display in map labels for percentage
timeout_query = 1 #How long to wait for each individual server to respond during slow search
timeout_master = 1 #How long to wait for the master server to respond during slow search
regionserver= "all" #Looks for servers in a specfic region
RuntimeMinutes = 60 #for how many minutes to run.
RunForever = True #True will run forever. Set to False to use runtime 
AverageDays = 1 #how many days each bar represents
ColorForOtherMaps = (0.5,0.5,0.5) # Color of the "Other Maps bars"
ColorIntensity = 25 # Changes the rate at which the colors change Low being very small changes and High being very big changes.
XaxisDates = 5 #How many dates to show in the figure. Maximum is the amount of bars. Minimum is 2