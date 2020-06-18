import pandas as pd
from sqlalchemy import create_engine
import sys, logging, logging.handlers, os, getopt

def main(argv):
        global file, table1, table2, csv_database

        global infile1, infile2, infile3, infile4, infile5, query, outfile, logger, delimiter, getvalues, prefixcol, replacefromcsv
        global isupdate, con, updatequery
        isupdate = False
        infile1 = ''
        infile2 = ''
        infile3 = ''
        infile4 = ''
        infile5 = ''

        logger = logging.getLogger()
        logging.captureWarnings(True)
        if not os.path.isdir("logs"):
                os.makedirs("logs")
        logging.basicConfig(handlers=[
                logging.handlers.RotatingFileHandler('logs/sqloncsv.log', maxBytes=1000000, backupCount=20)],
                            format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO)

        logger.critical("sqloncsv version 1.0.0")
        sys.stdout.write("sqloncsv version 1.0.0\n")
        logger.critical('log level set to ERROR')
        sys.stdout.write("log level set to ERROR\n")

        #"update 'phones' set `DEVICE NAME` = 'BLAH'"
        #--infile1 "cc-ipc.csv" --outfile out.csv --query "update 'phones' set `DEVICE NAME` = 'BLAH'"
        try:
                opts, args = getopt.getopt(argv, "hi:u:d:t:q:s:l:e",
                                           ["help", "isupdate", "infile1=", "infile2=", "infile3=", "infile4=", "infile5=",
                                            "outfile=", "query=", "updatequery=", "isupdate", "loglevel=", "exporttable="])

        except:
                logger.critical('FATAL ERROR Invalid Options')
                sys.stdout.write('FATAL ERROR Invalid Options\n')
                logger.critical('sqloncsv --infile1 cc-ipc.csv --query "update \'phones\' set `DEVICE NAME` = \'NEWNAME\'"')
                sys.stdout.write('sqloncsv --infile1 cc-ipc.csv --query "update \'phones\' set `DEVICE NAME` = \'NEWNAME\'"\n')
                sys.exit()
        else:
                for opt, arg in opts:
                        #sys.stdout.write("opt " + opt + " arg " + arg + '\n')
                        if opt in ('-h', "--help"):
                                sys.stdout.write('sqloncsv --infile1 cc-ipc.csv --query "update \'phones\' set `DEVICE NAME` = \'NEWNAME\'"\n')
                                sys.exit()
                        elif opt in ("i", "--isupdate"):
                                isupdate = True
                        elif opt in ("--infile1"):
                                infile1 = arg
                        elif opt in ("-d", "--infile2"):
                                infile2 = arg
                        elif opt in ("-t", "--infile3"):
                                infile3 = arg
                        elif opt in ("-q", "--infile4"):
                                infile4 = arg
                        elif opt in ("-s", "--infile5"):
                                infile5 = arg
                        elif opt in ("-o", "--outfile"):
                                outfile = arg
                        elif opt in ("q", "--query"):
                                query = arg
                        elif opt in ("u", "--updatequery"):
                                updatequery = arg
                                #sys.stdout.write("updatequery " + updatequery + '\n')
                        elif opt in ("l", "--loglevel"):
                                loglevel = arg.upper()
                                logger.critical('log level requested is ' + loglevel)
                                if loglevel == 'DEBUG':
                                        logger.setLevel(logging.DEBUG)
                                elif loglevel == 'INFO':
                                        logger.setLevel(logging.INFO)
                                elif loglevel == 'WARNING':
                                        logger.setLevel(logging.WARNING)
                                elif loglevel == 'ERROR':
                                        logger.setLevel(logging.ERROR)
                                elif loglevel == 'CRITICAL':
                                        logger.setLevel(logging.CRITICAL)
                                else:
                                        logger.critical('FATAL ERROR')
                                        logger.critical(
                                                'valid log level values are DEBUG, INFO, WARNING, ERROR, and CRITICAL')
                                        sys.stdout.write('FATAL ERROR\n')
                                        sys.stdout.write(
                                                'valid log level values are DEBUG, INFO, WARNING, ERROR, and CRITICAL\n')
                                        sys.exit()
                                logger.critical('log level changed to ' + loglevel)
                                sys.stdout.write('log level changed to ' + loglevel + '\n')

                        else:
                                sys.stdout.write(opt + 'is not a valid option')
        removedbfile()
        processinputfiles()
        #if isupdate:
        executesql()
        #writedatatofiles()
        removedbfile()

#def writedatatofiles():
#        global infile1, infile2, infile3, infile4, infile5, csv_database, outfile, con, isupdate


def executesql():
        global infile1, infile2, infile3, infile4, infile5, csv_database, outfile, con, query, isupdate, updatequery

        con = csv_database.connect()

        if isupdate:
                sys.stdout.write("\nUpdate SQL Query \"" + updatequery + '\"\n')
                logger.critical("Update SQL Query \"" + updatequery + "\"")
                rs = con.execute(updatequery)
                #changes = con.execute("SELECT changes()")
                #sys.stdout.write("changes " + str(changes) + "\n")
                #df1b = pd.read_sql("SELECT changes()", con)
                #sys.stdout.write("\nRows Updated " + str(df1b.iloc[0]) + "\n\n")
                print("\nUpdates: " + str(rs.rowcount) + "\n")
                #print(df1b)
                #for row in rs:
                #        sys.stdout.write(str(row) + "\n")
                #sys.stdout.write("\n" + str(con.rowcount) + " rows updated" + "\n\n")

        sys.stdout.write("\nSQL Query \"" + query + '\"\n\n')
        logger.critical("SQL Query \"" + query + "\"")
        df1a = pd.read_sql(query, con)
        df1a.to_csv(outfile, index=False)
        con.close()
        #df1a.to_csv(outfile)
        csv_database.dispose()

def processinputfiles():
        global infile1, infile2, infile3, infile4, infile5, query, csv_database, outfile
        csv_database = create_engine('sqlite:///csv_database.db')
        if infile1 != '':
                logger.critical("Loading File 1 " + infile1)
                loadfiletodb(infile1)
        if infile2 != '':
                logger.critical("Loading File 2 " + infile2)
                loadfiletodb(infile2)
        if infile3 != '':
                logger.critical("Loading File 3 " + infile3)
                loadfiletodb(infile3)
        if infile4 != '':
                logger.critical("Loading File 4 " + infile4)
                loadfiletodb(infile4)
        if infile5 != '':
                logger.critical("Loading File 5 " + infile5)
                loadfiletodb(infile5)

def loadfiletodb(file):
        global csv_database
        for df1 in pd.read_csv(file, dtype=str, iterator=True, index_col=1):
                df1.to_sql(file, csv_database, if_exists='replace')

def removedbfile():
        try:
                os.remove("csv_database.db")
                sys.stdout.write("Temp DB file removed" + '\n')
        except:
                sys.stdout.write("Temp DB file does not exist" + '\n')

if __name__ == "__main__":
        main(sys.argv[1:])