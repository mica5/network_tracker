#!/usr/bin/env python
from sqlalchemy import (
    BigInteger, Text, Integer, DateTime,
    ForeignKey,
    Column
)

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.schema import MetaData
from sqlalchemy.orm import sessionmaker, relationship

from network_tracker_config import engine


SABase = declarative_base(
    metadata=MetaData(
        bind=engine,
        schema='network',
    ),
)


class Helper:
    @classmethod
    def get_row(cls, column, value):
        sess = cls.get_sess()
        query = sess.query(cls).filter(column==value)

        # if it exists, then return it
        row = query.one_or_none()
        if row is not None:
            return row

        # otherwise, create one
        row = cls()
        setattr(row, str(column).split('.')[-1], value)

        sess = cls.get_sess()
        sess.add(row)
        sess.commit()

        return row

    sess = None
    @classmethod
    def set_sess(cls, sess):
        cls.sess = sess

    @classmethod
    def get_sess(cls):
        if cls.sess.is_active:
            return cls.sess
        cls.sess = sessionmaker(bind=cls.metadata.bind)()
        return cls.sess


class IPAddress(SABase, Helper):
    @classmethod
    def get_row(cls, ip_address):
        return super(IPAddress, cls).get_row(cls.ip, ip_address)

    __tablename__ = 'ip'
    ipid = Column(
        'ipid',
        Integer,
        autoincrement=True,
        doc='ip address id',
        primary_key=True,
    )
    ip = Column(
        'ip',
        Text,
        unique=True,
    )
    def __repr__(self):
        return '{}({})'.format(
            self.__class__.__name__,
            self.ip,
        )
    def __str__(self):
        return repr(self)


class Status(SABase, Helper):
    @classmethod
    def get_row(cls, status_str):
        return super(Status, cls).get_row(cls.status, status_str)

    __tablename__ = 'status'
    sid = Column(
        'sid',
        Integer,
        autoincrement=True,
        doc='status name id',
        primary_key=True,
    )
    status = Column(
        'status',
        Text,
        unique=True,
    )
    def __repr__(self):
        return '{}({})'.format(
            self.__class__.__name__,
            self.status,
        )
    def __str__(self):
        return repr(self)


class Device(SABase, Helper):
    @classmethod
    def get_row(cls, mac_address):
        return super(Device, cls).get_row(cls.mac, mac_address)

    __tablename__ = 'devices'
    did = Column(
        'did',
        Integer,
        autoincrement=True,
        primary_key=True,
    )
    mac = Column(
        'mac',
        Text,
        unique=True,
    )
    name = Column(
        'name',
        Text,
    )
    arp_name = Column(
        'arp_name',
        Text,
    )
    def __repr__(self):
        return '{}({})'.format(
            self.__class__.__name__,
            self.mac,
        )
    def __str__(self):
        return repr(self)


class Entry(SABase, Helper):
    __tablename__ = 'entry'
    eid = Column(
        'eid',
        BigInteger,
        autoincrement=True,
        primary_key=True,
    )
    timefrom = Column(
        'timefrom',
        DateTime,
        nullable=False,
        default="date_trunc('second', now())::timestamp",
    )
    timeto = Column(
        'timeto',
        DateTime,
        nullable=False,
        default="date_trunc('second', now())::timestamp",
    )

    sid = Column('sid', Integer, ForeignKey('status.sid'))
    status = relationship('Status')

    ipid = Column('ipid', Integer, ForeignKey('ip.ipid'))
    ip = relationship('IPAddress')

    did = Column('did', Integer, ForeignKey('devices.did'))
    device = relationship('Device')

    def __init__(self, status_str, ip_address, mac_address, arp_name, time):
        # if status doesn't exist, create it
        status = Status.get_row(status_str)
        self.sid = status.sid

        ip = IPAddress.get_row(ip_address)
        self.ipid = ip.ipid

        device = Device.get_row(mac_address)
        device.arp_name = arp_name
        self.get_sess().commit()
        self.did = device.did

        self.timefrom = self.timeto = time

    def __str__(self):
        return '{}({})'.format(
            self.__class__.__name__,
            ', '.join((repr(attr) for attr in [
                self.status.status,
                self.ip.ip,
                self.device.mac,
                self.timefrom,
            ]))
        )
    def __repr__(self):
        return str(self)
