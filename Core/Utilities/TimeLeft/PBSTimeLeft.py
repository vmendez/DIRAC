""" The PBS TimeLeft utility interrogates the PBS batch system for the
    current CPU and Wallclock consumed, as well as their limits.
"""

__RCSID__ = "$Id$"

import os
import re
import time

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.TimeLeft.TimeLeft import runCommand

class PBSTimeLeft( object ):

  #############################################################################
  def __init__( self ):
    """ Standard constructor
    """
    self.log = gLogger.getSubLogger( 'PBSTimeLeft' )
    self.jobID = os.environ.get( 'PBS_JOBID' )
    self.queue = os.environ.get( 'PBS_O_QUEUE' )
    pbsPath = os.environ.get( 'PBS_O_PATH' )
    if pbsPath:
      os.environ['PATH'] += ':' + pbsPath

    self.cpuLimit = None
    self.wallClockLimit = None
    self.log.verbose( 'PBS_JOBID=%s, PBS_O_QUEUE=%s' % ( self.jobID, self.queue ) )
    self.startTime = time.time()

  #############################################################################
  def getResourceUsage( self ):
    """Returns a dictionary containing CPUConsumed, CPULimit, WallClockConsumed
       and WallClockLimit for current slot.  All values returned in seconds.
    """
    cmd = 'qstat -f %s' % ( self.jobID )
    result = runCommand( cmd )
    if not result['OK']:
      return result

    cpu = None
    cpuLimit = None
    wallClock = None
    wallClockLimit = None

    lines = str( result['Value'] ).split( '\n' )
    for line in lines:
      info = line.split()
      if re.search( '.*resources_used.cput.*', line ):
        if len( info ) >= 3:
          cpuList = info[2].split( ':' )
          newcpu = ( float( cpuList[0] ) * 60 + float( cpuList[1] ) ) * 60 + float( cpuList[2] )
          if not cpu or newcpu > cpu:
            cpu = newcpu
        else:
          self.log.warn( 'Problem parsing "%s" for CPU consumed' % line )
      if re.search( '.*resources_used.pcput.*', line ):
        if len( info ) >= 3:
          cpuList = info[2].split( ':' )
          newcpu = ( float( cpuList[0] ) * 60 + float( cpuList[1] ) ) * 60 + float( cpuList[2] )
          if not cpu or newcpu > cpu:
            cpu = newcpu
        else:
          self.log.warn( 'Problem parsing "%s" for CPU consumed' % line )
      if re.search( '.*resources_used.walltime.*', line ):
        if len( info ) >= 3:
          wcList = info[2].split( ':' )
          wallClock = ( float( wcList[0] ) * 60 + float( wcList[1] ) ) * 60 + float( wcList[2] )
        else:
          self.log.warn( 'Problem parsing "%s" for elapsed wall clock time' % line )
      if re.search( '.*Resource_List.cput.*', line ):
        if len( info ) >= 3:
          cpuList = info[2].split( ':' )
          newcpuLimit = ( float( cpuList[0] ) * 60 + float( cpuList[1] ) ) * 60 + float( cpuList[2] )
          if not cpuLimit or newcpuLimit < cpuLimit:
            cpuLimit = newcpuLimit
        else:
          self.log.warn( 'Problem parsing "%s" for CPU limit' % line )
      if re.search( '.*Resource_List.pcput.*', line ):
        if len( info ) >= 3:
          cpuList = info[2].split( ':' )
          newcpuLimit = ( float( cpuList[0] ) * 60 + float( cpuList[1] ) ) * 60 + float( cpuList[2] )
          if not cpuLimit or newcpuLimit < cpuLimit:
            cpuLimit = newcpuLimit
        else:
          self.log.warn( 'Problem parsing "%s" for CPU limit' % line )
      if re.search( '.*Resource_List.walltime.*', line ):
        if len( info ) >= 3:
          wcList = info[2].split( ':' )
          wallClockLimit = ( float( wcList[0] ) * 60 + float( wcList[1] ) ) * 60 + float( wcList[2] )
        else:
          self.log.warn( 'Problem parsing "%s" for wall clock limit' % line )

    consumed = {'CPU':cpu, 'CPULimit':cpuLimit, 'WallClock':wallClock, 'WallClockLimit':wallClockLimit}
    self.log.debug( consumed )

    if None not in consumed.values():
      self.log.debug( "TimeLeft counters complete:", str( consumed ) )
      return S_OK( consumed )

    if cpuLimit or wallClockLimit:
      # We have got a partial result from PBS, we can only restore WallClock
      if not wallClock:  
        consumed['WallClock'] = int( time.time() - self.startTime )
      self.log.debug( "TimeLeft counters restored:", str( consumed ) )
      return S_OK( consumed )
    else:
      msg = 'Could not determine some parameters'
      self.log.info( msg, ':\nThis is the stdout from the batch system call\n%s' % ( result['Value'] ) )
      retVal = S_ERROR( msg )
      retVal['Value'] = consumed
      return retVal

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
