#!/usr/bin/env python
"""

Version 0.1
2018-07-24
"""
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import datetime
import subprocess
import re
import argparse

from sqlalchemy import DDL, func
from sqlalchemy.orm import sessionmaker

from network_tracker_config import engine, interface
from models import Helper, Entry, SABase


arp_line_re = re.compile(
    '(?P<ip_address>'
        '(\d{1,3}\.){3}\d{1,3}'
    ')'
    '\t'
    '(?P<mac_address>'
        '([\d\w]{2}:){5}[\d\w]{2}'
    ')'
    '\t'
    '(?P<name>'
        '[^\n]+'
    ')',
    re.I,
)

def arp_scan(interface='en0', sudo=False):
    return subprocess.check_output(
        '{sudo}arp-scan --localnet --interface {interface} {redirect}'.format(
            interface=interface,
            sudo='sudo ' if sudo else '',
            redirect='' if sudo else '2>&1 | grep -vE "You need to be root|Operation not permitted"',
        ),
        shell=True,
    ).decode().strip()

def run_update():
    sess = sessionmaker(bind=engine)()

    Helper.set_sess(sess)
    now = datetime.datetime.now()

    most_recent_record_time = sess.query(func.max(Entry.timeto)).one_or_none()

    last_by_mac_connected_and_not_connected = {
        e.device.mac: e
        for e in sess.query(Entry).filter(Entry.timeto==most_recent_record_time).all()
    }

    try:
        scan = arp_scan(interface)
    except subprocess.CalledProcessError:
        scan = arp_scan(interface, sudo=True)

    lines = scan.split('\n')

    added_to_network = list()
    stayed_in_network = list()

    seen_macs = set()
    for line in lines:
        line = line.strip()
        if not line:
            continue
        m = arp_line_re.match(line)
        if not m:
            continue
        ip_address = m.group('ip_address')
        mac_address = m.group('mac_address').upper()
        name = m.group('name')

        if mac_address in seen_macs:
            continue
        seen_macs.add(mac_address)

        # stayed in network
        if mac_address in last_by_mac_connected_and_not_connected:
            entry = last_by_mac_connected_and_not_connected.pop(mac_address)
            stayed_in_network.append((entry, ip_address, name))
        else:
            added_to_network.append((ip_address, mac_address, name))

    # stayed in network, but check if anything changed.
    for entry, ip_address, name in stayed_in_network:
        if entry.ip.ip != ip_address or entry.device.arp_name != name:
            sess.add(Entry(
                'connected', ip_address, mac_address, name, now,
            ))
        else:
            entry.timeto = now

    for ip_address, mac_address, name in added_to_network:
        sess.add(Entry(
            'connected', ip_address, mac_address, name, now
        ))

    for entry in last_by_mac_connected_and_not_connected.values():
        if entry.status.status == 'connected':
            sess.add(Entry(
                'not connected', entry.ip.ip, entry.device.mac, entry.device.arp_name, now
            ))
        else:
            entry.timeto = now

    sess.commit()


def create_tables():
    engine.execute(DDL('CREATE SCHEMA IF NOT EXISTS network'))
    SABase.metadata.create_all()
    engine.execute(DDL('''
    DROP VIEW IF EXISTS network_history;
    CREATE OR REPLACE VIEW network_history AS
        select
            e.eid
            , e.timefrom
            , e.timeto
            , s.status
            , ip.ip
            , d.mac
            , d.name
        from entry e
        inner join status s using(sid)
        inner join ip using(ipid)
        inner join devices d using(did)
        where e.timeto=(select max(timeto) from entry)
        order by regexp_replace(ip, '.*\.', '')::int
    ;
    -- thanks to https://stackoverflow.com/a/18939742/2821804
    CREATE OR REPLACE FUNCTION uppercase_mac_on_insert() RETURNS trigger AS $uppercase_mac_on_insert$
        BEGIN
            NEW.mac = upper(NEW.mac);
            RETURN NEW;
        END;
    $uppercase_mac_on_insert$ LANGUAGE plpgsql
    ;
    -- https://stackoverflow.com/a/40479291/2821804
    DROP TRIGGER IF EXISTS uppercase_mac_on_insert_trigger on network.devices
    ;
    CREATE TRIGGER uppercase_mac_on_insert_trigger BEFORE INSERT OR UPDATE ON devices
        FOR EACH ROW EXECUTE PROCEDURE uppercase_mac_on_insert()
    ;
    '''))

def drop_tables():
    SABase.metadata.drop_all()


def run_main():
    args = parse_cl_args()

    if args.run_update:
        run_update()
    elif args.create_tables:
        create_tables()
    elif args.drop_tables:
        drop_tables()

    success = True
    return success

def parse_cl_args():
    argParser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )

    argParser.add_argument(
        '--run-update',
        default=False,
        action='store_true',
    )
    argParser.add_argument(
        '--create-tables',
        default=False,
        action='store_true',
    )
    argParser.add_argument(
        '--drop-tables',
        default=False,
        action='store_true',
    )

    args = argParser.parse_args()
    return args

if __name__ == '__main__':
    success = run_main()
    exit_code = 0 if success else 1
    exit(exit_code)
