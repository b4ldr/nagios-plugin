#!/usr/bin/env python
"""
Check Serials of to servers to make sure they are in sync
"""

import logging, argparse, sys, os
import dns.query, dns.message, dns.rdatatype, dns.rcode
from datetime import datetime, timedelta
#from contextlib import contextmanager
#Python 2.4 hack
try:
    import sqlite3
except:
    from pysqlite2 import dbapi2 as sqlite3
LOG = logging.getLogger(__name__)

#### constants


#### internal routines

def get_serial(server, zone, results):
    '''Get 'zone' serial number from 'server' '''
    dnsmsg = dns.message.make_query(zone, dns.rdatatype.SOA)
    answer = dns.query.udp(dnsmsg, server)
    if (answer.answer and (answer.rcode() == dns.rcode.NOERROR)):
        return int(answer.answer[0][0].serial)
    else:
        results['error'].append( "%s no response from %s" % (zone, server))
        return 0

#@contextmanager
def cursor(db_file):
    '''return a database cursor - commit and close after'''
    conn = sqlite3.connect(db_file,
        detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    conn.row_factory = sqlite3.Row

    yield conn.cursor()

    conn.commit()
    conn.close()

def get_zones(db_file):
    '''Return an array of zones from the db in db'''
    #with cursor(db_file) as database:
    for database in cursor(db_file):
        database.execute('SELECT DISTINCT zone FROM zones')
        return [ r[0] for r in database.fetchall() ]
#### main functions

def init(zones, slaves, db_file ):
    '''(re)Initialise database with values for all 'zones' '''

    if not os.path.exists(db_file):
        #with cursor(db_file) as database:
        for database in cursor(db_file):
            database.execute('''
                CREATE TABLE IF NOT EXISTS
                zones (time timestamp, zone text, serial integer, slave text)''')
            for slave in slaves.split(','):
                for zone in zones:
                    if len(zone) < 2 or zone[0:4] == "tmp-":
                        continue
                    if zone == 'root':
                        zone = '.'
                    database.execute('INSERT INTO zones VALUES (?, ?, ?, ?)', (0, zone, 0, slave))

def refresh(db_file, master, slaves, results):
    ''' Refreshe the database with the curent serial for each zone'''
    #with cursor(db_file) as database:
    for database in cursor(db_file):
        for slave in slaves.split(','):
            for zone in get_zones(db_file):
                serial = get_serial(master, zone, results)
                if serial:
                    database.execute('INSERT INTO zones VALUES (?, ?, ?, ?)',
                            (datetime.now(), zone, get_serial(master, zone, results), slave))

def dump(db_file):
    '''Dump the content of the current database'''
    #with cursor(db_file) as database:
    for database in cursor(db_file):
        for row in database.execute('''
            SELECT zone, slave, max(serial), min(time), max(time)
            FROM zones WHERE time > 0 GROUP BY zone'''):
            print ('row %s' % dict(row))

def nagios_exit(results, verbose):
    '''Parse the results and exit correctly for nagios'''
    if len(results['error']) > 0:
        if verbose > 0:
            print "ERROR: %d zones: %s" % (len(results['error']),
                    ", ".join(results['error']))
        else:
            print "ERROR: %d zones" % len(results['error'])
        sys.exit(2)
    elif len(results['warn']) > 0:
        if verbose > 0:
            print "WARN: %d zones: %s" % (len(results['warn']),
                    ", ".join(results['warn']))
        else:
            print "WARN: %d zones" % len(results['warn'])
        sys.exit(1)
    else:
        if verbose > 1:
            print "OK: %d zones: %s" % (len(results['ok']),
                    ", ".join(results['ok']))
        else:
            print "OK: %d zones" % len(results['ok'])
        sys.exit(0)

def check(db_file, delta, slaves, results ):
    '''Check the serials of all zones.  ensuring the serial of the master
    matches the serial of the slave view from 'delta' minutes ago'''
    #with cursor(db_file) as database:
    for database in cursor(db_file):
        for slave in slaves.split(','):
            for zone in get_zones(db_file):
                database.execute('''SELECT serial FROM zones
                        WHERE time < ? AND zone = ? AND slave = ?
                        ORDER BY time DESC LIMIT 1''',
                        (datetime.now() - delta, zone, slave))

                master_serial = database.fetchall()[0][0]
                slave_serial = get_serial(slave, zone, results)

                if slave_serial:
                    if slave_serial < master_serial:
                        results['error'].append('%s behind: %i %i' %
                                (zone, master_serial, slave_serial))
                    else:
                        results['ok'].append('%s: %i %i' %
                                (zone, master_serial, slave_serial))
        #clean out some old entries
        database.execute("delete from zones where time < ? and time <> 0", 
                    (datetime.now() - delta * 2,))

def parse_args():
    '''Parse command line arguments'''
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-v', '--verbose', action='count',
            help='increase verbosity')
    parser.add_argument('-D', '--dump',
            help='Dump the current database')
    parser.add_argument('-m', '--master', required=True,
            help='zone master server')
    parser.add_argument('-s', '--slaves', required=True,
            help='comma seperated list of slave servers')
    parser.add_argument('-d', '--database', default='/var/db/nagios/db.sqlite3',
            help='SQLite Databse base name file')
    parser.add_argument('-t', '--timedelta', type=int, default='10',
            help='No# Minutes to allow changes to propogate')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-z', '--zonedir',
            help='Directory containing zone files')
    group.add_argument('-Z', '--zones',
            help='comma seperated list of zones to check')

    return parser.parse_args()
def main():
    '''Main program execution'''
    args = parse_args()
    master = args.master
    slaves = args.slaves
    zone_dir = args.zonedir
    results = { 'error': [], 'warn': [], 'ok': [] }
    log_level = args.verbose
    db_file = args.database
    delta = timedelta(minutes=args.timedelta)
    if not zone_dir:
        zones = args.zones.split(',')
    else:
        db_file = args.database + ".%s" %  zone_dir.rsplit('/', 2)[1]
        zones = os.listdir(zone_dir)
    #db_file = db_file + ".%s" % slave
    init(zones, slaves, db_file)
    refresh(db_file, master, slaves, results)
    check(db_file, delta, slaves, results )
    nagios_exit(results, log_level)



if __name__ == '__main__':
    main()
