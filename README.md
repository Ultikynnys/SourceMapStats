
# SourceMapStats


## What is this?
A way to gather and display map relevancy for a given Source game and Gamemode.
![parsecd_JitopkOtxP](https://user-images.githubusercontent.com/29761720/216379923-94c30771-f4c8-45ac-81af-e2708c6b1598.png)



## Usage
* Run the Write.py and start collecting data.
* The data will be stored in a output.csv file by default in the same directory.
* Once you have a Output.csv with some data. You can view it with the Read.py file.
* This will show you relevant information about the data you've just collected and generate a .png 

## Installation
* Download the repository with these comands
        
        ```git clone https://github.com/R60D/SourceMapStats```
        ```cd <path of folder>```
        ```git submodule update --init```
        
* Launch the install.bat to get py dependencies
* Open the Write.py to Configure the desired map prefix. default is dr_
* Open the Read.py to Configure the desired game. default is tf

## Debugging
* Do not download the repository as a zip from github directly. Use ```git clone``` instead.

* Make sure that the python-valve folder is not empty. If it is then do this.
        ```cd <path of folder>```
        ```git submodule update --init```

* Make sure that python and pip is installed on your computer. Otherwise the install.bat wont work
* Additionally make sure that your pip works by doing pip in cmd
