# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ConfigurationSystem/Client/LocalConfiguration.py,v 1.9 2007/05/17 17:29:35 acasajus Exp $
__RCSID__ = "$Id: LocalConfiguration.py,v 1.9 2007/05/17 17:29:35 acasajus Exp $"

import sys
import os
import getopt
import types

from DIRAC import gLogger
from DIRAC import S_OK, S_ERROR

from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.ConfigurationSystem.private.Refresher import gRefresher
from DIRAC.ConfigurationSystem.Client.PathFinder import getServiceSection, getAgentSection

class LocalConfiguration:

  def __init__( self, defaultSectionPath = "" ):
    self.currentSectionPath = defaultSectionPath
    self.mandatoryEntryList = []
    self.optionalEntryList = []
    self.commandOptionList = []
    self.unprocessedSwitches = []
    self.__registerBasicOptions()
    self.isParsed = False
    self.componentName = "Unknown"
    self.loggingSection = "/DIRAC"

  def __getAbsolutePath( self, optionPath ):
    if optionPath[0] == "/":
      return optionPath
    else:
      return "%s/%s" % ( self.currentSectionPath, optionPath )

  def addMandatoryEntry( self, optionPath ):
    self.mandatoryEntryList.append( optionPath )

  def addDefaultEntry( self, optionPath, value ):
    if optionPath[0] == "/":
      if not gConfigurationData.extractOptionFromCFG( optionPath ):
        self.__setOptionValue( optionPath, value )
    else:
      self.optionalEntryList.append( ( optionPath,
                                     str( value ) ) )

  def __setOptionValue( self, optionPath, value ):
    gConfigurationData.setOptionInCFG( self.__getAbsolutePath( optionPath ),
                                       str( value ) )

  def __registerBasicOptions( self ):
    self.registerCmdOpt( "o:", "option=", "Option=value to add",
                         self.__setOptionByCmd  )
    self.registerCmdOpt( "s:", "section=", "Set base section for relative parsed options",
                         self.__setSectionByCmd )
    self.registerCmdOpt( "c:", "cert=", "Use server certificate to connect to Core Services",
                         self.__setUseCertByCmd )
    self.registerCmdOpt( "h", "help", "Shows this help",
                         self.__showHelp )

  def registerCmdOpt( self, shortOption, longOption, helpString, function = False):
    #TODO: Can't overwrite switches (FATAL)
    self.commandOptionList.append( ( shortOption, longOption, helpString, function ) )

  def getPositionalArguments( self ):
    if not self.isParsed:
      self.__parseCommandLine()
    return self.commandArgList

  def getUnprocessedSwitches( self ):
    if not self.isParsed:
      self.__parseCommandLine()
    return self.unprocessedSwitches

  def loadUserData(self):
    try:
      retVal = self.__addUserDataToConfiguration()
      gLogger.forceInitialization( self.componentName, self.loggingSection )
      if not retVal[ 'OK' ]:
        return retVal

      for optionTuple in self.optionalEntryList:
        optionPath = self.__getAbsolutePath( optionTuple[0] )
        if not gConfigurationData.extractOptionFromCFG( optionPath ):
          gConfigurationData.setOptionInCFG( optionPath, optionTuple[1] )

      isMandatoryMissing = False
      for optionPath in self.mandatoryEntryList:
        optionPath = self.__getAbsolutePath( optionPath )
        if not gConfigurationData.extractOptionFromCFG( optionPath ):
          gLogger.fatal( "Missing mandatory option in the configuration", optionPath )
          isMandatoryMissing = True
      if isMandatoryMissing:
        return S_ERROR()
    except Exception, e:
      gLogger.exception()
      return S_ERROR( str( e ) )
    return S_OK()


  def __parseCommandLine( self ):
    gLogger.debug( "Parsing command line" )
    shortOption = ""
    longOptionList = []
    for optionTuple in self.commandOptionList:
      if shortOption.find( optionTuple[0] ) < 0:
        shortOption += "%s" % optionTuple[0]
      else:
        gLog.warn( "Short option -%s has been already defined" % optionTuple[0] )
      if not optionTuple[1] in longOptionList:
        longOptionList.append( "%s" % optionTuple[1] )
      else:
        gLog.warn( "Long option --%s has been already defined" % optionTuple[1] )

    try:
      opts, args = getopt.gnu_getopt( sys.argv[1:], shortOption, longOptionList )
    except getopt.GetoptError, v:
      # print help information and exit:
      gLogger.fatal( "Error when parsing command line arguments: %s" % str( v ) )
      self.__showHelp()
      sys.exit(2)

    self.AdditionalCfgFileList = [ arg for arg in args if arg[-4:] == ".cfg" ]
    self.commandArgList = [ arg for arg in args if not arg[-4:] == ".cfg" ]
    self.parsedOptionList = opts
    self.isParsed = True


  def __addUserDataToConfiguration( self ):
    if not self.isParsed:
      self.__parseCommandLine()

    errorsList = []

    gConfigurationData.loadFile( os.path.expanduser( "~/.diracrc" ) )
    for fileName in self.AdditionalCfgFileList:
      retVal = gConfigurationData.loadFile( fileName )
      if not retVal[ 'OK' ]:
        errorsList.append( retVal[ 'Message' ] )

    if gConfigurationData.getServers():
      retVal = self.__getRemoteConfiguration()
      if not retVal[ 'OK' ]:
        return retVal
    else:
      gLogger.info( "Running without remote configuration" )

    if self.componentType == "service":
      self.__setDefaultSection( getServiceSection( self.componentName ) )
    elif self.componentType == "agent":
      self.__setDefaultSection( getAgentSection( self.componentName ) )
    elif self.componentType == "script":
      self.__setDefaultSection( "/Scripts/%s" % self.componentName )
    else:
      self.__setDefaultSection( "/" )

    self.unprocessedSwitches = []

    for optionName, optionValue in self.parsedOptionList:
      optionName = optionName.replace( "-", "" )
      for definedOptionTuple in self.commandOptionList:
        if optionName == definedOptionTuple[0].replace( ":", "" ) or \
          optionName == definedOptionTuple[1].replace( "=", "" ):
          if definedOptionTuple[3]:
            retVal = definedOptionTuple[3]( optionValue )
            if type( retVal ) != types.DictType:
              errorsList.append( "Callback for switch '%s' does not return S_OK or S_ERROR" % optionName )
            elif not retVal[ 'OK' ]:
              errorsList.append( retVal[ 'Message' ] )
          else:
            self.unprocessedSwitches.append( ( optionName, optionValue ) )

    if len( errorsList ) > 0:
      return S_ERROR( "\n%s" % "\n".join( errorsList ) )
    return S_OK()

  def __getRemoteConfiguration( self ):
    needCSData = True
    if self.componentName == "Configuration/Server" :
      if gConfigurationData.isMaster():
        gLogger.debug( "CServer is Master!" )
        needCSData = False
      else:
        gLogger.debug( "CServer is slave" )
    if needCSData:
      retDict = gRefresher.forceRefreshConfiguration()
      if not retDict['OK']:
        gLogger.fatal( retDict[ 'Message' ] )
        return S_ERROR()

    return S_OK()

  def __setDefaultSection( self, sectionPath ):
    self.currentSectionPath = sectionPath
    self.loggingSection = self.currentSectionPath

  def setConfigurationForServer( self, serviceName ):
    self.componentName = serviceName
    self.componentType = "service"
    gLogger.initialize( self.componentName, "/DIRAC" )

  def setConfigurationForAgent( self, agentName ):
    self.componentName = agentName
    self.componentType = "agent"
    gLogger.initialize( self.componentName, "/DIRAC" )

  def setConfigurationForScript( self, scriptName ):
    self.componentName = scriptName
    self.componentType = "script"
    gLogger.initialize( self.componentName, "/DIRAC" )

  def __setSectionByCmd( self, value ):
    if value[0] != "/":
      return S_ERROR( "%s is not a valid section. It should start with '/'" % value )
    self.currentSectionPath = value
    return S_OK()

  def __setOptionByCmd( self, value ):
    valueList = [ sD.strip() for sD in value.split("=") if len( sD ) > 0]
    if len( valueList ) <  2:
      # FIXME: in the method above an exception is raised, check consitency
      return S_ERROR( "-o expects a option=value argument.\nFor example %s -o Port=1234" % sys.argv[0] )
    self.__setOptionValue( valueList[0] , valueList[1] )
    return S_OK()

  def __setUseCertByCmd( self, value ):
    useCert = "no"
    if value.lower() in ( "y", "yes", "true" ):
      useCert = "yes"
    self.__setOptionValue( "/DIRAC/Security/UseServerCertificate", useCert )
    return S_OK()

  def __showHelp( self, dummy ):
    gLogger.info( "Usage:" )
    gLogger.info( "  %s (<options>|<cfgFile>)*" % sys.argv[0] )
    gLogger.info( "Options:" )
    for optionTuple in self.commandOptionList:
      gLogger.info( "  -%s  --%s  :  %s" % optionTuple[:3] )
    posix._exit( 0 )

