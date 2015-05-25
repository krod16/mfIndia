#!/usr/bin/python3

import sqlite3
import requests
import os.path
from StringIO import StringIO

AMFI_FILE_PATH = "http://portal.amfiindia.com/spages/NAV0.txt"
DB_NAME = "mfindia.db"
AMCS_TABLE_NAME = "AMCS"
SCHEMES_TABLE_NAME = "SCHEMES"

AMC_NAME_STRING = "Mutual Fund"
SEPARATOR = ';'

AMFI_INDEX = {"code": 0,  "name": 3,  "nav": 4,  "date": 7}

def createDatabase ():
    conn = sqlite3.connect(DB_NAME)
    conn.close()
    
def updateAMCTable (amcNames):
    conn = sqlite3.connect(DB_NAME)
    query = '''CREATE TABLE IF NOT EXISTS AMCS (NAME TEXT PRIMARY KEY NOT NULL);'''
    conn.execute(query)

    for name in amcNames:
        query = '''INSERT OR IGNORE INTO AMCS (NAME) VALUES(?);'''
        conn.execute(query,  (name, ))    
        
    conn.commit()
    conn.close()
 
def updateMFInfoTable(mfList):
    conn = sqlite3.connect(DB_NAME)
    query = '''CREATE TABLE IF NOT EXISTS
                             SCHEMES (CODE           TEXT    PRIMARY KEY  NOT NULL,
                                    AMCNAME    TEXT    NOT NULL,
                                    MFNAME       TEXT    NOT NULL,
                                    NAV              REAL    NOT NULL,
                                    DATE            TEXT    NOT NULL,
                                    FOREIGN KEY(AMCNAME) REFERENCES AMCS(NAME));'''
    conn.execute(query)
    
    for mfInfo in mfList:
        query = '''INSERT OR IGNORE INTO SCHEMES (CODE, AMCNAME, MFNAME, NAV, DATE) VALUES(?, ?, ?, ?, ?);''' 
        conn.execute(query, (mfInfo["code"],  mfInfo["amcname"],  mfInfo["name"],  mfInfo["nav"],  mfInfo["date"]))    
    
    conn.commit()
    conn.close()
    
def getAMCNameFromString(amcString):
        amcName = ""
        amcString = amcString.strip()
        if amcString.endswith(AMC_NAME_STRING):
            amcName = amcString[:-len(AMC_NAME_STRING)].strip()
        
        return amcName
    
def parseMFLine(line):
    mfInfo = {}
    
    mfDataList = line.split(SEPARATOR)
    
    for key in AMFI_INDEX:
        mfInfo[key] = mfDataList [AMFI_INDEX[key]].decode('utf-8',  "ignore")

    return mfInfo

def readMFListFromFile ():
    mfList = []
    amcNames = []
    
    try:
        req = requests.get(AMFI_FILE_PATH)
    except ConnectionError:
        print ("AMFI website connection error")
    except HTTPError:
        print ("AMFI website http error")
    except Timeout:
        print ("AMFI website connection timeout")
    else:
        file = StringIO(req.content)
        lines = file.getvalue().splitlines()
    
        index = 0
        amcName = ""
        while index < len(lines):
            while len(amcName) == 0:
                line = lines[index].strip()
                amcName = getAMCNameFromString(line)
                index += 1

            if (len(amcName)):
                amcNames.append(amcName)
            
            nextAmcName = ""
            while (len(nextAmcName) == 0) and (index < len(lines)):
                line = lines[index].strip()
                if SEPARATOR in line:
                    mfInfo = parseMFLine(line)
                    mfInfo["amcname"] = amcName.decode('utf-8',  "ignore")
                    mfList.append(mfInfo)
                else:
                    nextAmcName = getAMCNameFromString(line)
                index += 1
            
            amcName = nextAmcName

    return (list(set(amcNames)),  mfList)

def refreshDatabase():
    (amcNames,  mfList) = readMFListFromFile()
    if (len(amcNames) == 0 or len(mfList) == 0):
        print ("Error in reading AMFI file")
    else:
        updateAMCTable(amcNames)
        updateMFInfoTable(mfList)
    
def searchMutualFund(mfName):
    mfName = mfName.decode("utf-8",  "ignore").strip()
    mfInfo = {}
    
    conn = sqlite3.connect(DB_NAME)
    query = '''SELECT code, amcname, mfname, nav, date FROM  SCHEMES WHERE mfname = ?;'''
    cursor = conn.execute(query,  (mfName,  ))
    for row in cursor:
        mfInfo["code"] = row[0]
        mfInfo["amcname"] = row[1]
        mfInfo["mfname"] = row[2]
        mfInfo["nav"] = row[3]
        mfInfo["date"] = row[4]
    conn.close()
    return mfInfo
   
if __name__ == "__main__":

    if not os.path.isfile(DB_NAME):
        createDatabase()
        
    #updateDbTables()
    fetchMFInfoFromDb("Franklin India  PENSION PLAN - Direct - Growth");
