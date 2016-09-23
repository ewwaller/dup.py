#! /usr/bin/python
# PYTHON_ARGCOMPLETE_OK

"""
    Duplicate file database.

    This program builds and maintains a database of multiple directory
    trees and provides tools to manage duplicate files.

    Copyright 2015 Eric Waller

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""
import os
import logging
import hashlib
import math
from argparse import ArgumentParser
import sqlite3
try:
    import argcomplete
except ImportError:
    pass
try:
    import gi
    gi.require_version('GExiv2', '0.10')
    from gi.repository import GExiv2
    exifAvailable=True
except ImportError:
    exifAvailable=False
except ValueError:
    exifAvailable=False
theDatabase = None
BLOCKSIZE = 65536
DRY_RUN = None
logFormat = '%(relativeCreated)6dmS (%(threadName)s) %(levelname)s : %(message)s'
default_DB = os.environ['HOME'] +'/.dup.sqlite'

class Database:
    """ Class to manage the life cycle of the sqlite3 database"""

    tables = (("files", "path", "hash"),
              ("metadata", "hash", "dateTime","latitude","longitude","altitude"),
             )
    latitude=None
    longitude=None
    
    def __init__(self, theFileName):
        """ Constructor
              Opens the database, creating it if necessary.  Checks that the required
              tables exist, and ifthey do not exist, it creates them

              theFileName:  The name of the data base file to open
              returns: None
        """
        self.con = sqlite3.connect(theFileName)
        with self.con:
            cur = self.con.cursor()
            cur.execute("select name from sqlite_master where type='table';")
            existingTables = cur.fetchall()

            # Walk through all the table names and see if they exist.  Create them if they do not

            for x in self.tables:
                if len(existingTables) > 0 and (x[0] in [z[0] for z in existingTables]):
                    logging.debug("Table %s exists in database", x[0])
                    continue
                else:

                    # Build the sql command to add the table and all of its comma delimited columns.

                    logging.info("Creating table %s", x[0])
                    (s, separator) = ("CREATE TABLE %s(" % x[0], '')
                    for z in x[1:]:
                        (s, separator) = (s + separator + z + ' text', ',')
                    cur.execute("%s)"%s)
            self.con.commit()

    def write(self, theHash, thePath):
        """ Write the path and its hash to the database if the path does
            not already exist.  If it exists, but the hash is changed,
            the hash is updated and a warning issued

            theHash:  The hash of the file
            thePath:  The full path to the file
            returns:  None
        """
        with self.con:
            cur = self.con.cursor()
            cur.execute("select * from files where path=:p", {"p":thePath})
            matchingRecords = cur.fetchall()
            if len(matchingRecords) == 0:
                logging.debug("Writing %s to the database", thePath)
                cur.execute("insert into files values(?,?);", (thePath, theHash))
            else:
                if len(matchingRecords) > 1:
                    logging.warning("Santity check failure.  Multiple database enties for %s",
                                    thePath)
                if matchingRecords[0][1] != theHash:
                    logging.warning("Hash changed for %s", thePath)
                    logging.info("Updating hash to  %s ", theHash)
                    cur.execute("update files set hash=? where path=?", (theHash, thePath))

    def DupCheck(self):
        """Check for duplicate files

           Takes no parameters.
           Returns None

           Walk through all hashes stored in the data base and look for duplicates.
           If duplicates are found, print the hash and paths to each duplicated file
        """
        logging.info("Checking for duplicate files in the database")
        cur = self.con.cursor()
        cur.execute("select * from files order by hash")
        (old_hash, pathList) = ('', [])
        for workingRecord in cur.fetchall():
            logging.debug("Checking hash %s for duplicates", workingRecord[1])
            if old_hash != workingRecord[1]:
                # The hash has changed since the the last record, see if there were multiple
                # records with that hash and print them if there were
                if len(pathList) > 1:
                    print("Hash %s"%old_hash)
                    for y in pathList:
                        print("  %s"%y)
                (pathList, old_hash) = ([workingRecord[0]], workingRecord[1])
            else:
                # The hash did not change from the last record. Add the path
                # from this record to the to the list
                pathList.append(workingRecord[0])

    def Integrity(self):
        """ Check the integrity of all the files in the database

            Takes no parameters
            Returns: None

            Walk through all the paths stored in the database, recalcualte each hash,
            and check that the recalculated hash is the same is the stored value
        """
        logging.info("Checking database integrity")
        cur = self.con.cursor()
        cur.execute("select * from files order by path")
        for workingRecord in cur.fetchall():
            logging.debug("Checking file %s for duplicates", workingRecord[0])
            if os.path.isfile(workingRecord[0]):
                hash = HashFile(workingRecord[0])
                if workingRecord[1] != hash:
                    print("File %s changed.\n  Old hash:%s\n  New hash:%s"%
                          (workingRecord[0], workingRecord[1], hash))
                    if not DRY_RUN:
                        cur.execute("update files set hash=? where path=?",
                                    (hash, workingRecord[0]))
            else:
                logging.warning("File %s no longer exists", workingRecord[0])
                if not DRY_RUN:
                    cur.execute("delete from files where path=?", (workingRecord[0], ))


    def Purge(self, thePath):
        """ Purge duplicate files from the database and the file system

            thePath: The full or partial path to the duplicate file(s) to be purged
            returns: None

            Walk through all of the file names in the database that "match" thePath.
            "match" means that full path name to the file starts with thePath.  For each
            file, it checks if it still exists and that its hash has not changed ,
            then checks to see if there are any other files in the database with the
            same hash.  It then checks those files still exist and that their hashes
            are accurate, and that they are not symlinks.  If all of these conditions
            are met, the file will be deleted and will be removed from the database.
        """
        theRealPath = os.path.abspath(thePath)
        logging.info("Purging duplicate files starting with %s", theRealPath)
        cur = self.con.cursor()
        cur.execute("PRAGMA case_sensitive_like=ON;")
        cur.execute("select * from files where path like ?", (theRealPath+'%', ))

        for workingRecord in cur.fetchall():

            # If the file we are looking for is not explicit, and is not in the specified
            # directory, DO NOT delete it

            if not sameDir(theRealPath, workingRecord[0]):
                logging.debug("%s is not in the target directory", workingRecord[0])
                continue
            logging.info("Checking if file %s can be purged", workingRecord[0])

            # If any files are stale, DO NOT delete the file

            cur2 = self.con.cursor()
            cur2.execute("select * from files where hash=?", (workingRecord[1], ))
            die, duplicates = False, cur2.fetchall()
            logging.info("Checking all matching hashes to ensure they have not changed")
            for x in duplicates:
                if os.path.islink(x[0]):
                    logging.warning("Refusing to process linked file %s", x[0])
                    die = True
                logging.debug("Checking hash for %s", x[0])
                theHash = HashFile(x[0])
                if not theHash or x[1] != theHash:
                    logging.warning("Refusing to purge %s (Hash changed)", x[0])
                    die = True
            if die: continue

            # If there is only one file with the hash, it is not a duplicate -- DO NOT delete

            if len(duplicates) < 2:
                logging.info("  %s is not a duplicate", workingRecord[0])
                continue

            # If we are here, there are duplicates.  Let's not delete this file if it is in the
            # same directory as is its

            cur2.execute("select * from files where hash=?", (workingRecord[1],))
            count = 0
            for x in cur2.fetchall():
                logging.debug("checking if %s and %s are in the same directory",
                              workingRecord[0], x[0])
                if sameDir(workingRecord[0], x[0]):
                    logging.debug("Yes")
                    count += 1
            if count != 1:
                logging.warning("Refusing to purge %s (Duplicate of file in same directory)",
                                workingRecord[0])
                continue

            # it is a duplicate.  It has not changed. Its duplicates have not changed. It is
            # not a duplicate of something in the same directory.  it is not a symlink.
            # So, delete it

            print("Purging file: %s"%workingRecord[0])

            # Last chance -- If this is a dry run, do not delete this

            if DRY_RUN:
                logging.warning("... Just kidding, this is a dry run")
                continue
            else:
                cur.execute("delete from files where path=?", (workingRecord[0], ))
                os.remove(workingRecord[0])

    def Remove(self, thePath):
        """ Remove files from the database in a given path.  No files are removed from the filesystem

            thePath:  A string that specifies the full or partial path of files to remove
                     from the database.

            Returns: None
        """
        theRealPath = os.path.abspath(thePath)
        logging.info("Removing entries starting with %s", theRealPath)
        cur = self.con.cursor()
        cur.execute("PRAGMA case_sensitive_like=ON;")
        cur.execute("select * from files where path like ?", (theRealPath+'%', ))

        for workingRecord in cur.fetchall():

            # If the file we are looking for is not explicit, and is not in the specified
            # directory, DO NOT delete it

            print("Removing %s from the database"%workingRecord[0])
            if DRY_RUN:
                logging.warning("... Just kidding, this is a dry run")
                continue
            else:
                cur.execute("delete from files where path=?", (workingRecord[0], ))

    def getExif(self):
        """ Process all of the hashed in the data base and attempt to read interesting
            exif data from those files.  Interesting, for now, means geolocation and the
            time that the photo was taken.  This depends on the GExiv2 module.  If the 
            program had been unable to load that module, this method will gracefully exit
       
            returns: None.
        """
        if not exifAvailable:
            logging.warning("Exif operations not available. Is GExiv2 installed?")
            return
        logging.info("Obtaining exif data for all hashes in the database")
        cur = self.con.cursor()
        cur.execute("select * from files order by hash")
        (old_hash, pathList) = ('', [])
        for workingRecord in cur.fetchall():
            if old_hash != workingRecord[1]:
                
                # I had many problems with bad avi files causing segfaults
                # They are not that interesting anyway, so ignore them.

                if workingRecord[0][-4:].lower()==".avi":
                    logging.warn("Refusing to process %s",workingRecord[0])
                    continue
                logging.debug("Getting exif data for  %s"% workingRecord[0])
                try:
                    exif=GExiv2.Metadata(workingRecord[0])
                    if not exif:
                        logging.warning("EXIF data not available")
                except:
                    logging.warning("Unable to read image data for %s"%workingRecord[0])
                    exif=None
                if exif:
                    for tag in [ ("dateTime",'get_date_time'),
                                 ("latitude",'get_gps_latitude'),
                                 ("longitude",'get_gps_longitude'),
                                 ("altitude",'get_gps_altitude'),
                               ]:
                        try:
                            theData=getattr(exif,tag[1])()
                            cur = self.con.cursor()
                            cur.execute("select * from metadata where hash=:p", {"p":workingRecord[1]})
                            matchingRecords = cur.fetchall()
                            if len(matchingRecords) == 0:
                                logging.debug("Writing %s: %s=%s to the database", workingRecord[0],tag[0],theData)
                                cur.execute("insert into metadata (hash, %s) values ( :hash, :data );"%tag[0],
                                            { 'hash':workingRecord[1], 'data':theData})
                            else:
                                if len(matchingRecords) > 1:
                                    logging.warning("Santity check failure.  Multiple database enties for %s",
                                                    thePath)
                                logging.info("Updating %s %s to  %s ",workingRecord[0],tag[0], theData)
                                cur.execute("update metadata set %s = :data where hash= :hash"%tag[0],
                                            {'data':theData, 'hash':workingRecord[1]})
                        except KeyError:
                            logging.info("File %s has no tag %s",workingRecord[0],tag[1])
                        except ValueError:
                            logging.warning("File %s: Cannot decode %s",workingRecord[0],tag[1])
                old_hash= workingRecord[1]

    def byDate(self, theDate):
        """ Output the paths to the files, in chronological order, to stdout.  The files start at the date passed
            into the function.

            theDate:  A string that is in "YYYY-MM-DD HH:MM:SS" format.  Tags older than this are not output

            returns: None
        """
        cur = self.con.cursor()
        cur.execute("select datetime, path from metadata, files "
                    "where files.hash = metadata.hash "
                    "and datetime not null "
                    "and datetime > ? "
                    "order by datetime;",
                    (theDate,))
        for record in cur.fetchall():
            print (record[1])

    def lat(self, theLatitude):
        """ Stores the given latitude to a class variable

            theLocation:  A string that represents the float value of the latitude 

            returns: None
        """
        self.latitude=float(theLatitude)

    def long(self, theLongitude):
        """ Stores the given longitude to a class variable, then output the 
            file names associated with the unique hashes in order of geolocation
            from the specified lat,long

            theLocation:  A string that represents the float value of the latitude 

            returns: None
        """
        self.longitude=float(theLongitude)
        if not self.latitude:
            logging.warning("Geolocation requires both latitude and longitude.  Missing latitude")
            return
        logging.info("Geolocation sorted by distance from lat=%s,long=%s",self.latitude,self.longitude)
        cur = self.con.cursor()
        cur.execute("select latitude, longitude, path from metadata, files "
                    "where files.hash = metadata.hash "
                    "and latitude not null "
                    "and longitude not null "
                    )

        theList=[]
        for record in cur.fetchall():
            Latitude,Longitude = float(record[0]), float(record[1])
            if abs(Latitude) > 0.00001 and abs(Longitude) > 0.00001:    
                logging.debug ("Latitude: %s Longitude: %s  -- %s",Latitude,Longitude,record[2])
                dPhi=math.radians(Latitude-self.latitude)
                dLambda=math.radians(Longitude-self.longitude)
                radius=6371000 # meters
                a =math.sin(dPhi/2) * math.sin(dPhi/2) + \
                   math.cos(math.radians(Latitude)) * math.cos(math.radians(self.latitude)) * \
	           math.sin(dLambda/2)*math.sin(dLambda/2)
                c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
                d = radius * c
                logging.debug("   --> distance=%f km"%(d/1000))
                theList.append((d/1000,record[2]))
        theList.sort()
        for i in theList:
            #print ("%s:%s"%(i[1],i[0]))
            print ("%s"%(i[1]))

            
    def map(self,filename):
        """ Build an HTML page that uses the Google Maps API to show the geolocation
            of all hashes that have lattitude  and longitude information.  Clicking
            on the markers will open the associated file in a separate browser window
            
            filename : The name of the output html file.

            returns: None
        """
        
        map = Map()
        cur = self.con.cursor()
        cur.execute("select latitude, longitude, path from metadata, files "
                    "where files.hash = metadata.hash "
                    "and latitude not null "
                    "and longitude not null "
                    )
        for record in cur.fetchall():
            Latitude,Longitude = float(record[0]), float(record[1])
            if abs(Latitude) > 0.00001 and abs(Longitude) > 0.00001:    
                logging.info ("Latitude: %s Longitude: %s  -- %s",Latitude,Longitude,record[2])
                map.add_point((Latitude, Longitude, record[2]))

        with open(filename, "w") as out:
            print(map, file=out)

        
    def close(self):
        """ Commit changes and close the database """
        if self.latitude and not self.longitude:
            logging.warning("Geolocation requires both latitude and longitude.  Missing longitude")

        if self.con:
            logging.debug("Closing database")
            self.con.commit()
            self.con.close()

class Map(object):
    def __init__(self):
        self._points = []
    def add_point(self, coordinates):
        self._points.append(coordinates)
    def __str__(self):
        centerLat = sum(( x[0] for x in self._points )) / len(self._points)
        centerLon = sum(( x[1] for x in self._points )) / len(self._points)
        markersCode = "\n".join(
            [ """var marker = new google.maps.Marker({{
                position: new google.maps.LatLng({lat}, {lon}),
                map: map,
                title: '{file}'
                }});
                marker.addListener('click', function() {{
                window.open('{file}', '_blank');
                }});
            """.format(lat=x[0], lon=x[1], file=x[2]) for x in self._points
            ])
        return """
            <script src="https://maps.googleapis.com/maps/api/js?v=3.exp&sensor=false"></script>
            <div id="map-canvas" style="height: 100%; width: 100%"></div>
            <script type="text/javascript">
                var map;
                function show_map() {{
                    map = new google.maps.Map(document.getElementById("map-canvas"), {{
                        zoom: 3,
                        center: new google.maps.LatLng(0,0)
                    }});
                    {markersCode}
                }}
                google.maps.event.addDomListener(window, 'load', show_map);
            </script>
        """.format(markersCode=markersCode)


def sameDir(thePath, theFile):
    """ Check two paths, one of which is a full path name for a file,
        and the other may be a path to a directory or to a file.
        Determine if they represent a file in the given directory,  whether they
        represent full pahs to two different files in the same directory,
        or whether they represent two different files in the same directory

        theFile: A string representing the full path name of a file
        thePath: A string representing the full path to a directory or a file

        returns:  True  -- if thePath is a directory and theFile is a
                           file in thePath.

                           if thePath is a file and that file is in the
                           same directory that theFile is in.
        returns:  False -- Otherwise. (If theFile is not in the directory thePath
                           or if theFile is in a differnet directory than a file
                           represented by thePath)
    """
    theFile = theFile.split("/")
    thePath = thePath.split("/")

    # if the base paths are different lengths, they are not in the same directory.
    # if both are files and the files are both in the same directory, the lengths are the same
    # if one is a directory, and the other a file in that directory, the file path will be one
    # longer.
    if len(theFile) - len(thePath) > 1: return False

    # See if the base paths are the same.  If the file path is longer,
    # don't worry about the last element -- it is the file name
    for z in range(len(theFile)-1):
        logging.debug("     %s   --   %s", thePath[z], theFile[z])
        if thePath[z] != theFile[z]: return False
    logging.debug("[sameDir]   %s", theFile)
    logging.debug("[sameDir]     is in the")
    logging.debug("[sameDir]        %s directory", thePath)
    return True


def initDB(theFileName):
    """Helper function to open instanciate the database and initialize it.
       Compatible with the lambdas used in the dispatch table in Main

       theFileName: the name of the file
       returns    : None<
    """
    global theDatabase
    theDatabase = Database(theFileName)


def HashFile(theFile):
    """Return the SHA2-256 hash of a file
       theFile:  A string representing the full path name of the file
                 to be hashed
       returns:  A string representing the hex values of the sha2-256
                 hash of the file
    """
    hasher = hashlib.sha256()
    afile = None

    logging.info("Hashing file %s", theFile)
    if not os.path.exists(theFile):
        logging.warning("File %s no longer exists", theFile)
        return None
    try:
        with open(theFile, 'rb') as afile:
            buf = afile.read(BLOCKSIZE)
            while len(buf) > 0:
                hasher.update(buf)
                buf = afile.read(BLOCKSIZE)
            afile.close()
            logging.debug("%s : %s", hasher.hexdigest(), theFile)
            return hasher.hexdigest()
    except PermissionError:
        logging.warning("Unable to read %s", theFile)
        if afile != None:
            afile.close()
        return None

def HashDir(thePath):
    """ Obtain hash for contents of all files in a directory tree

        theURL:  A string that specifies the top directory tree

        returns: None
    """
    logging.debug("Adding %s to database", thePath)
    for root, dirs, files in os.walk(thePath):
        for theFile in files:
            theFilePath = os.path.abspath("%s/%s"%(root, theFile))
            if theDatabase:
                theDatabase.write(HashFile(theFilePath), theFilePath)
    

def setLog(enableLog, LogLevelStr):
    """Set the log level to be used during this run.  This program
       uses logging to provide warning, info, and debug level messages.
       warnings are always enabled.  info level messages are considered
       to be "Verbose". debug level messages are considered to be "debug"
       or "Very Verbose".

       enableLog:   A boolean that specifies whether to set the log level.
                    This was implemented so that this function could be called
                    from a list comprehension but allow for cases where we need
                    to make the call, but don't really want to take action

       LogLevelStr: A string representing on of the members of logging that
                    define log levels ("CRITICAL", DEBUG", "ERROR", "FATAL"
                    ("INFO", "NOTSET", WARN, "WARNING")
    """
    if enableLog:
        logging.getLogger().setLevel(getattr(logging, LogLevelStr))


def main():

    # variable theParameters defines the command line options, where
    # and how their data are stored, and define the relation
    # of the command line parameters to callback functions
    # persistent storage.
    #
    #    Element 0 is the member name in the ArgumentPaser object,
    #    element 1 is the action,
    #    element 2 is the short option name,
    #    element 3 is the long option name,
    #    element 4 is the function to call if this option is selected
    #    element 5 is the help string, and
    #    element 6 is the default if the element is not set

    logging.basicConfig(level=getattr(logging, 'WARN'), format=logFormat)

    theParameters = (
        ('dryrun'   ,'store_false' ,None ,'--commit'    ,None                              ,"Really delete things"                          ,True      ),
        ('INFO'     ,'store_true'  ,'-v' ,'--verbose'   ,lambda x: setLog(x,'INFO')        ,"Generate information"                          ,None      ),
        ('DEBUG'    ,'store_true'  ,None ,'--debug'     ,lambda x: setLog(x,'DEBUG')       ,"Generate debugging information"                ,None      ),
        ('database' ,'store'       ,None ,'--database'  ,lambda x: initDB(x)               ,"Select database name"                          ,default_DB),
        ('path'     ,'store'       ,'-p' ,'--path'      ,lambda x: HashDir(x)              ,"Scan a directory tree into database"           ,None      ),
        ('check'    ,'store_true'  ,'-c' ,'--check'     ,lambda x: theDatabase.Integrity() ,"Check database for integrity"                  ,None      ),
        ('duplicate','store_true'  ,'-d' ,'--duplicate' ,lambda x: theDatabase.DupCheck()  ,"Check for duplicates in database"              ,None      ),
        ('purge'    ,'store'       ,None ,'--purge'     ,lambda x: theDatabase.Purge(x)    ,"Purge duplicate files"                         ,None      ),
        ('remove'   ,'store'       ,None ,'--remove'    ,lambda x: theDatabase.Remove(x)   ,"Remove files from database"                    ,None      ),
        ('exif'     ,'store_true'  ,None ,'--exif'      ,lambda x: theDatabase.getExif()   ,"Obtain metadata for all files"                 ,None      ),
        ('byDate'   ,'store'       ,None ,'--byDate'    ,lambda x: theDatabase.byDate(x)   ,"Output file paths by date since param"         ,None      ),
        ('lat'      ,'store'       ,None ,'--lat'       ,lambda x: theDatabase.lat(x)      ,"Latitude. required for file paths by distance" ,None      ),
        ('long'     ,'store'       ,None ,'--long'      ,lambda x: theDatabase.long(x)     ,"Longitude, required for file paths by distance",None      ),
        ('map'      ,'store'       ,None ,'--map'       ,lambda x: theDatabase.map(x)      ,"Generate geolocation map to given file name"   ,None      ),
    )

    global theDatabase
    global DRY_RUN

    # Handle all the command line nonsense.

    description = "Manage duplicate files"
    parser = ArgumentParser(description=description)
    for x in theParameters:
        if x[2]: parser.add_argument(x[2] ,x[3], action=x[1], dest=x[0], help=x[5], default=x[6])
        else: parser.add_argument(x[3], action=x[1], dest=x[0], help=x[5], default=x[6])
    argcomplete.autocomplete(parser)
    args = parser.parse_args()
    DRY_RUN = args.dryrun
    [x[4](getattr(args, x[0])) for x in theParameters if x[4] and getattr(args, x[0])]
    theDatabase.close()
    logging.info("Done")

if __name__ == "__main__":
    main()
