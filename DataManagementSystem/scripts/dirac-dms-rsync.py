#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-dms-rsync
# Author :  Marko Petric
########################################################################
"""
  Porvides basic rsync funcionality for DIRAC
"""

__RCSID__ = "$Id$"

import os
import sys
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s Source Destination' % Script.scriptName,
                                     ' ',
                                     ' e.g.: Download',
                                     '   %s LFN Path' % Script.scriptName,
                                     '  or Upload',
                                     '   %s Path LFN SE' % Script.scriptName,
                                     'Arguments:',
                                     '  LFN:      Logical File Name (Path to directory)',
                                     '  Path:     Local path to the file (Path to direcotry)',
                                     '  SE:       DIRAC Storage Element' ]
                                 )
                      )
Script.parseCommandLine( ignoreErrors = False )


from DIRAC import S_OK, S_ERROR
from DIRAC import gConfig, exit as dexit
from DIRAC.Resources.Catalog.FileCatalogFactory import FileCatalogFactory

def getSetOfLocalDirectoriesAndFiles( path ):
  """
  Return a set of all directories and subdirectories and a set of files contained therein for a given local path
  """

  fullPath = os.path.abspath(path)

  if not os.path.isdir(fullPath):
    return S_ERROR("The path: " + fullPath + " does not exist!")

  directories = set()
  files = set()

  for dirname, dirnames, filenames in os.walk(path):
  # add path to all subdirectories first.
    for subdirname in dirnames:
      fullSubdirname = os.path.join(dirname,subdirname)
      fullSubdirname = os.path.abspath(fullSubdirname)
      fullSubdirname = fullSubdirname.replace(fullPath,'').lstrip('/')
      directories.add(fullSubdirname)
  # add path to all filenames.
    for filename in filenames:
      fullFilename = os.path.join(dirname,filename)
      fullFilename = os.path.abspath(fullFilename)
      fullFilename = fullFilename.replace(fullPath,'').lstrip('/')
      files.add((fullFilename,long(os.path.getsize(fullPath + "/" +  fullFilename))))

  tree = {}
  tree["Directories"]=directories
  tree["Files"]=files

  return S_OK(tree)

def getFileCatalog():
  """
    Returns the DIRAC file catalog
  """
  fcType = gConfig.getValue("/LocalSite/FileCatalog","")
  
  res = gConfig.getSections("/Resources/FileCatalogs",listOrdered = True)
  if not res['OK']:
    return S_ERROR(res['Message'])
    
  if not fcType:
    if res['OK']:
      fcType = res['Value'][0]
  
  if not fcType:
    return S_ERROR("No file catalog given and defaults could not be obtained")
  
  result = FileCatalogFactory().createCatalog(fcType)
  if not result['OK']:
    return S_ERROR(result['Message'])
    
  fc = result['Value']
  
  return S_OK(fc)


def getSetOfRemoteSubDirectoriesAndFiles(path,fc,directories,files):
  """
    Recusivly traverses all the subdirectories of a directory and returns a set of directories and files
  """
  result =  fc.listDirectory(path)
  if result['OK']:
    if result['Value']['Successful']:
      for entry in result['Value']['Successful'][path]['Files']:
        size = result['Value']['Successful'][path]['Files'][entry]['MetaData']['Size']
        files.add((entry,size))
      for entry in result['Value']['Successful'][path]['SubDirs']:
        directories.add(entry)
        res = getSetOfRemoteSubDirectoriesAndFiles(entry,fc,directories,files)
        if not res['OK']:
          return S_ERROR('Error: ' + res['Massage'])
      return S_OK()
    else:
      return S_ERROR("Error:" + result['Message'])
  else:
    return S_ERROR("Error:" + result['Message'])

def getSetOfRemoteDirectoriesAndFiles(path):
  """
    Return a set of all directories and subdirectories and the therein contained files for a given LFN
  """
  res = getFileCatalog()
  if not res['OK']:
    return S_ERROR(res['Message'])
  
  fc = res['Value']
   
  directories = set()
  files = set()

  res = getSetOfRemoteSubDirectoriesAndFiles(path,fc,directories,files)
  if not res['OK']:
    return S_ERROR('Could not list remote directory: ' + res['Massage'])

  return_directories = set()
  return_files = set()

  for myfile in files:
    return_files.add((myfile[0].replace(path,'').lstrip('/'),myfile[1]))
    
  for mydirectory in directories:
    return_directories.add(mydirectory.replace(path,'').lstrip('/'))

  tree = {}
  tree["Directories"]=return_directories
  tree["Files"]=return_files
  
  return S_OK(tree)

def isInFileCatalog(fc, path ):
  """
    Check if the file is in the File Catalog
  """
  
  result = fc.listDirectory(path) 
  if result['OK']:
    if result['Value']['Successful']:
      return S_OK()
    else:
      return S_ERROR()
  else:
    return S_ERROR()

def getContentToSync(upload,source_dir,dest_dir):

  if upload:
    res = getSetOfRemoteDirectoriesAndFiles(dest_dir)
    if not res['OK']:
      return S_ERROR(res['Message'])
    to_dirs = res['Value']['Directories']
    to_files =  res['Value']['Files']
    
    res = getSetOfLocalDirectoriesAndFiles(source_dir)
    if not res['OK']:
      return S_ERROR(res['Message'])
    from_dirs = res['Value']['Directories']
    from_files =  res['Value']['Files']
    
  else:
    res = getSetOfLocalDirectoriesAndFiles(dest_dir)
    if not res['OK']:
      return S_ERROR(res['Message'])
    to_dirs = res['Value']['Directories']
    to_files =  res['Value']['Files']
    
    res = getSetOfRemoteDirectoriesAndFiles(source_dir)
    if not res['OK']:
      return S_ERROR(res['Message'])
    from_dirs = res['Value']['Directories']
    from_files =  res['Value']['Files']
    
  print 'to_dirs'
  print to_dirs
  print 'from_dirs'
  print from_dirs
  print 'to_files'
  print to_files
  print 'from_files'
  print from_files
  
    
  dirs_delete = list(to_dirs - from_dirs)
  dirs_delete.sort(key = lambda s: -s.count('/'))
  dirs_create = list(from_dirs - to_dirs)
  dirs_create.sort(key = lambda s: s.count('/'))
  
  files_delete = list(to_files - from_files)
  files_create = list(from_files - to_files)
    
  return [dirs_delete, dirs_create, files_delete, files_create]



print getContentToSync(True,'.','/ilc/user/p/petric')