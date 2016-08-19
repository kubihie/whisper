#!/usr/bin/python

import os
import sys
import math
import time
import bisect
import signal
import optparse
import traceback

try:
  import whisper
except ImportError:
  raise SystemExit('[ERROR] Please make sure whisper is installed properly')

# Ignore SIGPIPE
signal.signal(signal.SIGPIPE, signal.SIG_DFL)

option_parser = optparse.OptionParser(usage='''%prog [options] path''')
option_parser.add_option('--newfile', default=None, action='store', help="Create a new database file without removing the existing one")

(options, args) = option_parser.parse_args()

if len(args) < 1:
  option_parser.print_usage()
  sys.exit(1)

path = args[0]

if not os.path.exists(path):
  sys.stderr.write("[ERROR] File '%s' does not exist!\n\n" % path)
  option_parser.print_usage()
  sys.exit(1)

info = whisper.info(path)
archives = info['archives']

#create new file
if options.newfile is None:
  tmpfile = path + '.tmp'
  if os.path.exists(tmpfile):
    print 'Removing previous temporary database file: %s' % tmpfile
    os.unlink(tmpfile)
  newfile = tmpfile
else:
  newfile = options.newfile

print 'Retrieving all data from the archives'
for archive in archives:
  now = int( time.time() )
  fromTime = now - archive['retention']
  untilTime = now
  timeinfo,values = whisper.fetch(path, fromTime, untilTime)
  archive['data'] = (timeinfo,values)

  timeinfo, values = archive['data']
  values = ",".join(map(str, values))
  values = values.replace("None", "0")
  values = values.split(",")
  values = map(float, values)
  datapoints = zip( range(*timeinfo), values )
  datapoints = filter(lambda x: x[1] > 0, datapoints)

  newfile_secondsPerPoint = archive['secondsPerPoint']
  newfile_retention = datapoints[-1][0] - datapoints[0][0] + newfile_secondsPerPoint
  new_archive = [whisper.parseRetentionDef(str(newfile_secondsPerPoint) + ":" + str(newfile_retention) + "s")]
  print(new_archive)
  print 'Creating new whisper database: %s' % newfile
  whisper.create(newfile, new_archive)
  size = os.stat(newfile).st_size
  print 'Created: %s (%d bytes)' % (newfile,size)

  whisper.update_many_ex(newfile, datapoints, datapoints[0][0])

if options.newfile is not None:
  sys.exit(0)

backup = path + '.bak'
print 'Renaming old database to: %s' % backup
os.rename(path, backup)

try:
  print 'Renaming new database to: %s' % path
  os.rename(tmpfile, path)
except:
  traceback.print_exc()
  print '\nOperation failed, restoring backup'
  os.rename(backup, path)
  sys.exit(1)
