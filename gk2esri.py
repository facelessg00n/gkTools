"""
Extracts Apple location data from GK zipfile and converts it to an ESRI friendly format
This allows an approximate trackloog to be replayed
"""

import argparse
from datetime import datetime
import glob
import os
import pandas as pd
import shutil
import simplekml
import sqlite3
import sys
import zipfile

pd.set_option("display.precision", 8)

# ---------Debug Mode----------------------------------------------------------------
debug = False

# -------------Location of required databases----------------------------------------
routinedCache = "/private/var/mobile/Library/Caches/com.apple.routined/Cache.sqlite"
localCache = "/private/var/mobile/Library/Caches/com.apple.routined/Local.sqlite"

targetPath = os.getcwd() + "/temp"

# ------- Convert Cocoa timestamp----------------------------------------------------
# iOS devices use Cocoa time rather than UNIX time
unix = datetime(1970, 1, 1)  # UTC
cocoa = datetime(2001, 1, 1)  # UTC
delta = cocoa - unix

# -------- Details ------------------------------------------------------------------
__description__ = """ Extracts Apple location data from GrayKey extract """
__author__ = "facelessg00n"
__version__ = "0.2"


# -------- Changes ------------------------------------------------------------------

"""
0.2 Close connection to database correctly
    Process and export visits

0.1 Initial Release

"""
# --------Functions live here--------------------------------------------------------


def checkZip(z):
    if zipfile.is_zipfile(z):
        with zipfile.ZipFile(z) as file:
            # print(file.infolist())
            zippedFiles = file.namelist()

            # print(zippedFiles)
            if routinedCache in zippedFiles:

                routiuneD = True
                print("Routine D Cache file exists")
                processRoutinedCache(z)
            else:
                routineD = False

            if localCache in zippedFiles:
                print("Local Cache database exists.")
                processlocalCache(z)
                localCacheExists = True
            else:
                print("Local cache not found")
                localCacheExists = False
    else:
        print("This does not appear to be a zipfile.")
        print("Exiting.")


def makeTempFolder():
    try:
        print("Creating temporary folder")
        os.makedirs(targetPath)
    except OSError as e:
        print(e)
        print("Temporary folder exists")
        print("Purging directory")
        shutil.rmtree(targetPath)
        try:
            print("Creating temporary folder")
            os.makedirs(targetPath)
        except:
            print("Something has gone horribly wrong")
            exit()


# --------------- Process-------------------------------------------------------------
# TODO Process local cache database
def processlocalCache(localCacheIN):
    try:
        makeTempFolder()
    except:
        pass

    with zipfile.ZipFile(localCacheIN) as file:
        file.extract(localCache, targetPath)
        inputFile = glob.glob("./**/com.apple.routined/Local.sqlite", recursive=True)
        print("Using " + str(inputFile[0]) + " as the input file for Local Cache")
        print("Conneting to database")
        con = sqlite3.connect(inputFile[0])

        print("Closing database connection to Local Cache.")
        con.close()
        # Cleanup temp folder
        shutil.rmtree(targetPath)


def processRoutinedCache(routinedIN):
    try:
        makeTempFolder()
    except:
        pass
    with zipfile.ZipFile(routinedIN) as file:
        file.extract(routinedCache, targetPath)
        inputFile = glob.glob("./**/com.apple.routined/Cache.sqlite", recursive=True)
        print("Using" + str(inputFile[0]) + " as the input file for Cache.")
        print("Conneting to database")
        con = sqlite3.connect(inputFile[0])

        print("Extracting tracklog")
        try:
            routineDTrack = pd.read_sql_query("SELECT * from ZRTCLLOCATIONMO", con)
            routineDTrack["dateTime"] = (
                pd.to_datetime(routineDTrack["ZTIMESTAMP"], unit="s") + delta
            )
            if debug:
                print(routineDTrack.info())
                print(routineDTrack.head())

            routineDTrack.rename(
                columns={
                    "ZLATITUDE": "LATITUDE",
                    "ZLONGITUDE": "LONGITUDE",
                    "ZHORIZONTALACCURACY": "HORIZONTALACCURACY",
                    "ZVERTICALACCURACY": "VERTICALACCURACY",
                    "ZSPEED": "SPEED",
                },
                inplace=True,
            )
            print(str(len(routineDTrack.index)) + " datapoints located.")
        except sqlite3.OperationalError:
            print("ERROR : Table ZRTCLLOCATIONMO not located, skipping")
            pass

        print("Processing Learned Locations")

        try:
            learnedLocations = pd.read_sql_query(
                "SELECT * from ZRTLEARNEDLOCATIONOFINTERESTMO", con
            )
            learnedLocations["dateTime"] = (
                pd.to_datetime(learnedLocations["ZPLACECREATIONDATE"], unit="s") + delta
            )

        except sqlite3.OperationalError:
            print("ERROR : Table ZRTCLLOCATIONMO not located, skipping")
            pass

        # TODO : Extract Visits data
        # -------Extract data from visits table-----------------------------------------

        routineDVisitsPD = pd.read_sql_query("SELECT * from ZRTVISITMO", con)
        visitDateCols = ["ZDETECTIONDATE", "ZENTRYDATE", "ZEXITDATE", "ZLOCATIONDATE"]
        for col in visitDateCols:
            routineDVisitsPD[col] = (
                pd.to_datetime(routineDVisitsPD[col], unit="s") + delta
            )

        routineDVisitsPD.rename(
            columns={
                "ZLOCATIONLATITUDE": "LATITUDE",
                "ZLOCATIONLONGITUDE": "LONGITUDE",
            },
            inplace=True,
        )
        print(str(len(routineDVisitsPD.index)) + " datapoints located.")

        if debug:
            print(routineDVisitsPD.head())
            print(routineDVisitsPD.info())
        # TODO : Extract Vehicle data

        # ------------Close connection to database-------------------------------------
        print("Closing database connection to routineD.")
        con.close()

        # -------Export reports--------------------------------------------------------
        print("Exporting CSV files")
        routineDTrack.to_csv(
            "trackLog.csv", index=False, date_format="%Y/%m/%d %H:%M:%S"
        )

        print("Exporting Location Visits")
        routineDVisitsPD.to_csv(
            "Visits.csv", index=False, date_format="%Y/%m/%d %H:%M:%S"
        )

        # Export to KML
        print("Exporting KML")
        kml = simplekml.Kml()
        for pointName, pointTime, lon, lat in zip(
            routineDTrack["Z_PK"],
            routineDTrack["dateTime"],
            routineDTrack["LONGITUDE"],
            routineDTrack["LATITUDE"],
        ):
            kml.newpoint(name=pointName, description=pointTime, coords=[(lon, lat)])
        kml.save("tracklog.kml")
        # Cleanup temp folder

        shutil.rmtree(targetPath)


# ------- ---------------Argument parser-----------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__description__,
        epilog="Developed by {}, version {}".format(str(__author__), str(__version__)),
    )

    parser.add_argument(
        "-f",
        "--file",
        dest="inputFilename",
        help="Path to input ZIP File",
    )
    args = parser.parse_args()

    # Display help message when no args are passed.
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    # If no input show the help text.
    if not args.inputFilename:
        parser.print_help()
        parser.exit(1)

    # Check if the input file exists.
    if not os.path.exists(args.inputFilename):
        print("ERROR: '{}' does not exist or is not a file".format(args.inputFilename))
        sys.exit(1)


# ------------Run--------------------------------------------------------------------
checkZip(args.inputFilename)
