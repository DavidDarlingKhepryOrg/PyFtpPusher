'''
Python 3.4 FTP Pusher script
'''

import os
import logging
import sys

import ftputil
from ftputil.error import FTPError
import keyring
import pysftp

# =============================
# Define module-level variables
# =============================

ftpUrl = 'ftp.frackingdata.info'
ftpPath = None
ftpUserName = 'downloads@frackingdata.info'
ftpPassword = None # if None then pull password from keyring
ftpTimeout = 15
ftpCreatePathIfNonExistant = True
ftpDeleteFilesOnUpload = False
ftpRemovePreExistingFtpFiles = False
useSSH = False

srcPathName = None
srcFileNames = ['test1.txt']

# instantiate and initialize
# logging objects and handlers
dftMsgFormat = '%(asctime)s\t%(levelname)s\t%(module)s\t%(funcName)s\t%(lineno)d\t%(message)s'
dftDateFormat = '%Y-%m-%d %H:%M:%S'

nmlLogLevel = logging.DEBUG

# obtain any command-line arguments
# overriding any values set so far
nextArg = ''
for argv in sys.argv:
    if nextArg != '':
        if nextArg == 'ftpUrl':
            ftpUrl = argv
        if nextArg == 'ftpUserName':
            ftpUserName = argv
        if nextArg == 'ftpPassword':
            ftpPassword = argv
        if nextArg == 'ftpTimeout':
            ftpTimeout = int(argv)
        if nextArg == 'useSSH':
            useSSH = argv == 'True'
        if nextArg == 'srcFileNames':
            srcFileNames = argv.split(',')
        if nextArg == 'srcPathName':
            srcPathName = argv
        nextArg = ''
    else:
        if argv.lower() == '--ftpurl' or argv.lower() == '-ftpurl':
            nextArg = 'ftpUrl'
        if argv.lower() == '--ftpusername' or argv.lower() == '-ftpusername':
            nextArg = 'ftpUserName'
        if argv.lower() == '--ftppassword' or argv.lower() == '-ftppassword':
            nextArg = 'ftpPassword'
        if argv.lower() == '--ftptimeout' or argv.lower() == '-ftptimeout':
            nextArg = 'ftpTimeout'
        if argv.lower() == '--usessh' or argv.lower() == '-usessh':
            nextArg = 'useSSH'
        if argv.lower() == '--srcfilenames' or argv.lower() == '-srcfilenames':
            nextArg = 'srcFileNames'
        if argv.lower() == '--srcpathname' or argv.lower() == '-srcpathname':
            nextArg = 'srcPathName'

# ==============================
# implement minimum log filter
# for restraining logging output
# ==============================
class MinLogLevelFilter(logging.Filter):
    '''
    Minimum Log Level Filter class
    '''
    def __init__(self, level):
        '''

        :param level:
        '''
        self.level = level

    def filter(self, record):
        '''

        :param record:
        '''
        return record.levelno >= self.level

# ==============================
# implement maximum log filter
# for restraining logging output
# ==============================
class MaxLogLevelFilter(logging.Filter):
    '''
    Maximum Log Level Filter class
    '''
    def __init__(self, level):
        '''

        :param level:
        '''
        self.level = level

    def filter(self, record):
        '''

        :param record:
        '''
        return record.levelno <= self.level


# ==========================
# Mainline execution routine
# ==========================
def main(isTestMode=False):
    '''

    :param pgmLogger:
    :param isTestMode:
    '''

    # set the default logger's values
    logging.basicConfig(level=logging.INFO,
                        format=dftMsgFormat,
                        datefmt=dftDateFormat)


    # instantiate the logger object
    logger = logging.getLogger('')

    logger.handlers = []  # remove any existing log handlers

    # attach stdout to the logger
    # so that outputting to the log also
    # outputs to the stdout console
    logStdOut = logging.StreamHandler(sys.stdout)
    logStdOut.setFormatter(logging.Formatter(dftMsgFormat, dftDateFormat))
    logStdOut.addFilter(MaxLogLevelFilter(logging.WARNING))
    logStdOut.setLevel(logging.DEBUG)
    logger.addHandler(logStdOut)

    # attach stderr to the logger
    # so that outputting to the log also
    # outputs to the stderr console
    logStdErr = logging.StreamHandler(sys.stderr)
    logStdErr.setFormatter(logging.Formatter(dftMsgFormat, dftDateFormat))
    logStdErr.addFilter(MinLogLevelFilter(logging.ERROR))
    logStdErr.setLevel(logging.ERROR)
    logger.addHandler(logStdErr)

    logger.setLevel(nmlLogLevel)

    # can the program connect to the FTP site?
    ftpConn, errStr = getFtpConn(ftpUrl=ftpUrl,
                                 ftpUserName=ftpUserName,
                                 ftpPassword=ftpPassword,
                                 ftpTimeout=ftpTimeout,
                                 useSSH=useSSH,
                                 pgmLogger=logger,
                                 isTestMode=isTestMode)

    if ftpConn is not None:
        # can the program
        # close the FTP site?
        errStr = clsFtpConn(ftpConn,
                            pgmLogger=logger,
                            isTestMode=isTestMode)

    if errStr is None:
        # source file by source file
        # push the files to the FTP site
        for srcFileName in srcFileNames:
            srcFileNameExpanded = getPathExpanded(srcFileName, srcPathName)
            if os.path.exists(srcFileNameExpanded):
                errStr = putFtpFile(srcFileName=srcFileNameExpanded,
                                    ftpUrl=ftpUrl,
                                    ftpUserName=ftpUserName,
                                    ftpPassword=ftpPassword,
                                    ftpTimeout=ftpTimeout,
                                    useSSH=useSSH,
                                    ftpPath=ftpPath,
                                    deleteFilesOnUpload=ftpDeleteFilesOnUpload,
                                    pgmLogger=logger,
                                    isTestMode=isTestMode)
            else:
                errStr = "srcFileName does not exist: %s" % srcFileNameExpanded
                logger.error(errStr)

    return errStr


# ==================================
# Close the specified FTP connection
# ==================================
def clsFtpConn(ftpConn,
               pgmLogger=logging,
               isTestMode=False):
    '''

    :param ftpConn:
    :param pgmLogger:
    :param isTestMode:
    '''

    errStr = None

    if ftpConn != None:
        try:
            ftpConn.close()
        except FTPError as err:
            errStr = str(err)
            pgmLogger.error("CLOSE of FTP connection encountered an error: %s" % (errStr))

    return errStr


# ========================
# Obtain an FTP connection
# ========================
def getFtpConn(ftpUrl,
               ftpUserName=None,
               ftpPassword=None,
               ftpTimeout=15,
               useSSH=False,
               pgmLogger=logging,
               isTestMode=False):
    '''

    :param ftpUrl:
    :param ftpUserName:
    :param ftpPassword:
    :param ftpTimeout:
    :param useSSH:
    :param pgmLogger:
    :param isTestMode:
    '''

    errStr = None
    ftpConn = None

    if ftpPassword is None:
        password, errStr = getPwdViaKeyring(ftpUrl, ftpUserName, True)
    else:
        password = ftpPassword

    if errStr is None:
        if useSSH:
            try:
                ftpConn = pysftp.Connection(ftpUrl, username=ftpUserName, password=password)
                ftpConn.timeout = ftpTimeout
            except Exception as err:
                errStr = str(err)
                pgmLogger.error("SFTP connection encountered an error: %s" % (errStr))
        else:
            try:
                ftpConn = ftputil.FTPHost(ftpUrl, ftpUserName, password)
            except FTPError as err:
                errStr = str(err)
                pgmLogger.error("FTP connection encountered an error: %s" % (errStr))

    return ftpConn, errStr


# ==================================
# Upload an FTP file to the FTP site
# ==================================
def putFtpFile(srcFileName,
               ftpUrl=ftpUrl,
               ftpUserName=ftpUserName,
               ftpPassword=ftpPassword,
               ftpTimeout=ftpTimeout,
               useSSH=useSSH,
               ftpPath=ftpPath,
               deleteFilesOnUpload=False,
               pgmLogger=logging,
               isTestMode=False):
    '''

    :param srcFileName:
    :param ftpUrl:
    :param ftpUserName:
    :param ftpPassword:
    :param ftpTimeout:
    :param useSSH:
    :param ftpPath:
    :param deleteFilesOnUpload:
    :param pgmLogger:
    :param isTestMode:
    '''

    errStr = None
    successful = False

    if useSSH:
        errStr = putFtpFileViaPysftp(srcFileName=srcFileName,
                                     ftpUrl=ftpUrl,
                                     ftpUserName=ftpUserName,
                                     ftpPassword=ftpPassword,
                                     ftpTimeout=ftpTimeout,
                                     useSSH=useSSH,
                                     ftpPath=ftpPath,
                                     deleteFilesOnUpload=deleteFilesOnUpload,
                                     pgmLogger=pgmLogger,
                                     isTestMode=isTestMode)
    else:
        errStr = putFtpFileViaFtpUtil(srcFileName=srcFileName,
                                      ftpUrl=ftpUrl,
                                      ftpUserName=ftpUserName,
                                      ftpPassword=ftpPassword,
                                      ftpTimeout=ftpTimeout,
                                      useSSH=useSSH,
                                      ftpPath=ftpPath,
                                      deleteFilesOnUpload=deleteFilesOnUpload,
                                      pgmLogger=pgmLogger,
                                      isTestMode=isTestMode)

    return errStr


# =====================================
# Upload an FTP file to a non-SFTP site
# =====================================
def putFtpFileViaFtpUtil(srcFileName,
                         ftpUrl=ftpUrl,
                         ftpUserName=ftpUserName,
                         ftpPassword=ftpPassword,
                         ftpTimeout=ftpTimeout,
                         useSSH=useSSH,
                         ftpPath=ftpPath,
                         deleteFilesOnUpload=False,
                         pgmLogger=None,
                         isTestMode=False):
    '''

    :param srcFileName:
    :param ftpUrl:
    :param ftpUserName:
    :param ftpPassword:
    :param ftpTimeout:
    :param useSSH:
    :param ftpPath:
    :param deleteFilesOnUpload:
    :param pgmLogger:
    :param isTestMode:
    '''

    errStr = None

    if ftpPath is None:
        ftpPath = ''

    srcFileNameExpanded = getPathExpanded(srcFileName)

    if not os.path.exists(srcFileNameExpanded):
        errStr = 'File "%s" does NOT exist, put of file to FTP site FAILED.' % srcFileNameExpanded
        pgmLogger.error(errStr)

    if errStr is None:
        ftpConn, errStr = getFtpConn(ftpUrl,
                                     ftpUserName,
                                     ftpPassword,
                                     ftpTimeout,
                                     useSSH,
                                     pgmLogger,
                                     isTestMode)

    if errStr is None:
        # reset ftpPath value
        ftpPathTemp = ftpPath
        # if the original ftpPath does not exist
        if not ftpConn.path.exists(ftpPathTemp):
            pgmLogger.warning('FTP PATH (original) "%s" does NOT exist!' % ftpPathTemp)
            if ftpPathTemp.startswith('/'):
                # perhaps the ftpPath shouldn't
                # have started with a slash
                ftpPathTemp = ftpPathTemp[1:]
            else:
                # perhaps the ftpPath should
                # have started with a slash
                ftpPathTemp = '/' + ftpPathTemp
            # if the modified ftpPath does not exist
            if not ftpConn.path.exists(ftpPathTemp):
                pgmLogger.warning('FTP PATH (modified) "%s" does NOT exist!' % ftpPathTemp)
                if ftpCreatePathIfNonExistant:
                    # reset ftpPath value
                    ftpPathTemp = ftpPath
                    try:
                        # attempt creation of original ftpPath
                        ftpConn.makedirs(ftpPathTemp, mode=None)
                        pgmLogger.info('FTP PATH (original) "%s" was created!' % ftpPathTemp)
                    except Exception as err:
                        pgmLogger.warning('FTP PATH (original) "%s" was NOT created!' % ftpPathTemp)
                        try:
                            if ftpPathTemp.startswith('/'):
                                # perhaps the ftpPath shouldn't
                                # have started with a slash
                                ftpPathTemp = ftpPathTemp[1:]
                            else:
                                # perhaps the ftpPath should
                                # have started with a slash
                                ftpPathTemp = '/' + ftpPathTemp
                            # attempt creation of modified ftpPath
                            ftpConn.makedirs(ftpPathTemp, mode=None)
                            pgmLogger.info('FTP PATH (modified) "%s" was created!' % ftpPathTemp)
                        except Exception as err:
                            pgmLogger.warning('FTP PATH (modified) "%s" was NOT created!' % ftpPathTemp)
                            errStr = 'FTP PATH (original) "%s" and FTP PATH (modified) "%s" were NOT created!' % (ftpPath, ftpPathTemp)
                            pgmLogger.error(errStr)
                            pgmLogger.error("Program will retry FTP PATH creation on the next iteration!")
                            if err != None:
                                pgmLogger.error(str(err))
                else:
                    errStr = 'FTP PATH (original) "%s" and FTP PATH (modified) "%s" do NOT exist!' % (ftpPath, ftpPathTemp)
                    pgmLogger.error(errStr)
                    pgmLogger.error("Program will retry FTP PATH existence on the next iteration!")
            else:
                pgmLogger.info('FTP PATH (modified) "%s" does exist!' % ftpPathTemp)
        else:
            pgmLogger.info('FTP PATH (original) "%s" does exist!' % ftpPathTemp)


        # reset ftpPath to the
        # temporary ftpPath that "worked"
        ftpPath = ftpPathTemp

    baseName = os.path.basename(srcFileNameExpanded)
    ftpFileFullPath = ftpConn.path.join(ftpPath, baseName)

    if errStr is None and ftpRemovePreExistingFtpFiles:
        if ftpConn.path.exists(ftpFileFullPath):
            try:
                ftpConn.remove(ftpFileFullPath)
                pgmLogger.info("FTP REMOVE success of existing file %s" % ftpFileFullPath)
            except Exception as err:
                errStr = str(err)
                pgmLogger.error("FTP REMOVE failure of existing file %s" % ftpFileFullPath)
                pgmLogger.error(errStr)

    if errStr is None:
        try:
            ftpConn.upload(source=srcFileNameExpanded,
                                target=ftpFileFullPath,
                                callback=None)
            pgmLogger.info('FTP UPLOAD success of local file: "%s" to FTP path "%s":' % (srcFileNameExpanded, ftpFileFullPath))
        except Exception as err:
            errStr = str(err)
            pgmLogger.error("FTP UPLOAD failure of local file %s to FTP path: %s" % (srcFileNameExpanded, ftpFileFullPath))
            pgmLogger.error(errStr)

    if errStr is None:
        if deleteFilesOnUpload:
            try:
                deleteFileIfItExists(srcFileNameExpanded,
                                                     pgmLogger=pgmLogger)
                pgmLogger.info('DELETE of file: "%s" SUCCEEDED after UPLOAD to FTP path "%s"' % (srcFileNameExpanded, ftpFileFullPath))
            except Exception as err:
                errStr = str(err)
                pgmLogger.error('DELETE of file: "%s" FAILED after UPLOAD to FTP path "%s"' % (srcFileNameExpanded, ftpFileFullPath))
                pgmLogger.error(errStr)

    if ftpConn != None:
        try:
            ftpConn.close()
        except Exception as err:
            errStr = str(err)
            pgmLogger.error("FTP connection closure FAILED.")
            pgmLogger.error(errStr)

    return errStr


# ==================================
# Upload an FTP file to an SFTP site
# ==================================
def putFtpFileViaPysftp(srcFileName,
                        ftpUrl=ftpUrl,
                        ftpUserName=ftpUserName,
                        ftpPassword=ftpPassword,
                        ftpTimeout=ftpTimeout,
                        useSSH=useSSH,
                        ftpPath=ftpPath,
                        deleteFilesOnUpload=False,
                        pgmLogger=None,
                        isTestMode=False):
    '''

    :param srcFileName:
    :param ftpUrl:
    :param ftpUserName:
    :param ftpPassword:
    :param ftpTimeout:
    :param useSSH:
    :param ftpPath:
    :param deleteFilesOnUpload:
    :param pgmLogger:
    :param isTestMode:
    '''

    errStr = None

    if ftpPath is None:
        ftpPath = ''

    srcFileNameExpanded = getPathExpanded(srcFileName)

    if not os.path.exists(srcFileNameExpanded):
        errStr = 'File "%s" does NOT exist, put of file to FTP site FAILED.' % srcFileNameExpanded
        pgmLogger.error(errStr)

    if errStr is None:
        ftpConn, errStr = getFtpConn(ftpUrl,
                                     ftpUserName,
                                     ftpPassword,
                                     ftpTimeout,
                                     useSSH,
                                     pgmLogger,
                                     isTestMode)

    if errStr is None:
        # reset ftpPath value
        ftpPathTemp = ftpPath
        # if the original ftpPath does not exist
        if not ftpConn.exists(ftpPathTemp):
            pgmLogger.warning('FTP PATH (original) "%s" does NOT exist!' % ftpPathTemp)
            if ftpPathTemp.startswith('/'):
                # perhaps the ftpPath shouldn't
                # have started with a slash
                ftpPathTemp = ftpPathTemp[1:]
            else:
                # perhaps the ftpPath should
                # have started with a slash
                ftpPathTemp = '/' + ftpPathTemp
            # if the modified ftpPath does not exist
            if not ftpConn.exists(ftpPathTemp):
                pgmLogger.warning('FTP PATH (modified) "%s" does NOT exist!' % ftpPathTemp)
                if ftpCreatePathIfNonExistant:
                    # reset ftpPath value
                    ftpPathTemp = ftpPath
                    try:
                        # attempt creation of original ftpPath
                        ftpConn.makedirs(ftpPathTemp)
                        pgmLogger.info('FTP PATH (original) "%s" was created!' % ftpPathTemp)
                    except Exception as err:
                        pgmLogger.warning('FTP PATH (original) "%s" was NOT created!' % ftpPathTemp)
                        try:
                            if ftpPathTemp.startswith('/'):
                                # perhaps the ftpPath shouldn't
                                # have started with a slash
                                ftpPathTemp = ftpPathTemp[1:]
                            else:
                                # perhaps the ftpPath should
                                # have started with a slash
                                ftpPathTemp = '/' + ftpPathTemp
                            # attempt creation of modified ftpPath
                            ftpConn.makedirs(ftpPathTemp)
                            pgmLogger.info('FTP PATH (modified) "%s" was created!' % ftpPathTemp)
                        except Exception as err:
                            pgmLogger.warning('FTP PATH (modified) "%s" was NOT created!' % ftpPathTemp)
                            errStr = 'FTP PATH (original) "%s" and FTP PATH (modified) "%s" were NOT created!' % (ftpPath, ftpPathTemp)
                            pgmLogger.error(errStr)
                            pgmLogger.error("Program will retry FTP PATH creation on the next iteration!")
                            if err != None:
                                pgmLogger.error(str(err))
                else:
                    errStr = 'FTP PATH (original) "%s" and FTP PATH (modified) "%s" do NOT exist!' % (ftpPath, ftpPathTemp)
                    pgmLogger.error(errStr)
                    pgmLogger.error("Program will retry FTP PATH existence on the next iteration!")
            else:
                pgmLogger.info('FTP PATH (modified) "%s" does exist!' % ftpPathTemp)
        else:
            pgmLogger.info('FTP PATH (original) "%s" does exist!' % ftpPathTemp)

        # reset ftpPath to the
        # temporary ftpPath that "worked"
        ftpPath = ftpPathTemp

    baseName = os.path.basename(srcFileNameExpanded)
    ftpFileFullPath = ftpPath + '/' + baseName

    if errStr is None and ftpRemovePreExistingFtpFiles:
        if ftpConn.exists(ftpFileFullPath):
            try:
                ftpConn.remove(ftpFileFullPath)
                pgmLogger.info("FTP REMOVE success of existing file %s" % ftpFileFullPath)
            except Exception as err:
                errStr = str(err)
                pgmLogger.error("FTP REMOVE failure of existing file %s" % ftpFileFullPath)
                pgmLogger.error(errStr)

    if errStr is None:
        try:
            with ftpConn.cd(ftpPath):
                ftpConn.put(srcFileNameExpanded)
            pgmLogger.info('FTP UPLOAD success of local file: "%s" to FTP path "%s":' % (srcFileNameExpanded, ftpFileFullPath))
        except Exception as err:
            errStr = str(err)
            pgmLogger.error("FTP UPLOAD failure of local file %s to FTP path: %s" % (srcFileNameExpanded, ftpFileFullPath))
            pgmLogger.error(errStr)

    if errStr is None:
        if deleteFilesOnUpload:
            try:
                deleteFileIfItExists(srcFileNameExpanded,
                                                     pgmLogger=pgmLogger)
                pgmLogger.info('DELETE of file: "%s" SUCCEEDED after UPLOAD to FTP path "%s"' % (srcFileNameExpanded, ftpFileFullPath))
            except Exception as err:
                errStr = str(err)
                pgmLogger.error('DELETE of file: "%s" FAILED after UPLOAD to FTP path "%s"' % (srcFileNameExpanded, ftpFileFullPath))
                pgmLogger.error(errStr)

    if ftpConn != None:
        try:
            ftpConn.close()
        except Exception as err:
            errStr = str(err)
            pgmLogger.error("FTP connection closure FAILED.")
            pgmLogger.error(errStr)

    return errStr


# ========================
# Delete file if it exists
# ========================
def deleteFileIfItExists(fileNameExpanded,
                         pgmLogger=logging):
    '''

    :param fileNameExpanded:
    :param pgmLogger:
    '''

    errStr = None

    if fileNameExpanded != None:
        # if file exists
        if os.path.exists(fileNameExpanded):
            # delete it
            try:
                os.remove(fileNameExpanded)
                pgmLogger.info('File "%s" deleted successfully' % fileNameExpanded)
            except Exception as err:
                errStr = str(err)
                pgmLogger.error('File "%s" delete FAILED' % fileNameExpanded)
                pgmLogger.error(errStr)
        else:
            pgmLogger.warning('File "%s" NOT deleted as it does NOT seem to exist' % fileNameExpanded)

    return errStr


# ==========================================
# Expand the specified folder path as needed
# ==========================================
def getPathExpanded(path,
                    parentPath = '',
                    pgmLogger=logging):
    '''

    :param path:
    :param parentPath:
    :param pgmLogger:
    '''

    # default the return value
    pathExpanded = path
    # if it even has a value
    if pathExpanded != None and pathExpanded != '':
        # if the home folder is specified
        if pathExpanded.startswith("~"):
            # expand the file path with the home folder
            pathExpanded = os.path.expanduser(pathExpanded)
        # split the folder into its drive and tail
        drive, tail = os.path.splitdrive(pathExpanded)
        # if it's a sub-folder
        if drive == '' and not tail.startswith("/"):
            if parentPath != None:
                pathExpanded = os.path.join(parentPath, pathExpanded)
        # obtain the folder's absolute path
        pathExpanded = os.path.abspath(pathExpanded)
    # return expanded folder path
    return pathExpanded


#===============================
# Get password in user's keyring
#===============================
def getPwdViaKeyring(key,
                     login,
                     redactPasswords=True,
                     logResults=False,
                     pgmLogger=logging,
                     isTestMode=False):
    '''

    :param key:
    :param login:
    :param redactPasswords:
    :param logResults:
    :param pgmLogger:
    :param isTestMode:
    '''

    errStr = None
    password = None

    # get the password for the specified key and login
    try:
        password = keyring.get_password(key, login)
        if password != None:
            if logResults:
                pgmLogger.info("SUCCESS: Retrieval of password for key: %s and login: %s" % (key, login))
                if redactPasswords:
                    pgmLogger.info("Key: %s, Login: %s, Password: %s" % (key, login, '[redacted]'))
                else:
                    pgmLogger.info("Key: %s, Login: %s, Password: %s" % (key, login, password))
#                 if isTestMode:
#                     password = '[redacted]'
        else:
            errStr = "ERROR: key: %s for login: %s not in keyring" % (key, login)
            pgmLogger.error(errStr)
    except Exception as err:
        errStr = str(err)
        pgmLogger.error("FAILURE: Retrieval of password for key: %s and login: %s" % (key, login))
        pgmLogger.error(errStr)

    return password, errStr


# ============================================================================
# execute the mainline processing routine
# ============================================================================
if __name__ == "__main__":
    main()
