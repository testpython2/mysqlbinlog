#/bin/python
#coding=utf-8

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import QueryEvent, RotateEvent, FormatDescriptionEvent

import sys

import json

import datetime

import pymysql

from pymysqlreplication.row_event import (
DeleteRowsEvent,
UpdateRowsEvent,
WriteRowsEvent,
)





MYSQL_SETTINGS = {
    "host": "192.168.1.210",
    "port": 3306,
    "user": "root",
    "passwd": "admin",
    'charset': 'utf8'

}



db=pymysql.connect(user='root',passwd='admin',host='192.168.1.220',port=3307,db='binlog',charset="UTF8")

con=db.cursor()
reload(sys)
sys.setdefaultencoding('utf-8')
# print sys.getdefaultencoding()




def compare_items(items):
    # caution: if v is NULL, may need to process
    (k, v) = items
    if v is None:
        return "'%s' IS NULL" % k
    else:
        return "`%s`='%s'" % (k,v)


def main():
    # server_id is your slave identifier, it should be unique.
    # set blocking to True if you want to block and wait for the next event at
    # the end of the stream
    stream = BinLogStreamReader(connection_settings=MYSQL_SETTINGS,
                                server_id=1,
                                blocking=True,
                                only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],
                                log_file="mysql-bin.000001",
                                log_pos=1
                                )

    # for binlogevent in stream:
    #     if isinstance(binlogevent, QueryEvent):
    #
    #         ddl = str(binlogevent.query)
    #         if ddl.split(" ")[0].upper() == "ALTER":
    #             print binlogevent.schema
    #             print ddl
    #         if ddl.split(" ")[0].upper() == "TRUNCATE":
    #
    #             print binlogevent.schema
    #             print ddl
    #             if ddl.split(" ")[1].upper() == "TABLE":
    #                 print ddl.split(" ")[2]
    #             else:
    #                 print ddl.split(" ")[1]

    for binlogevent in stream:
          for row in binlogevent.rows:
            if isinstance(binlogevent,WriteRowsEvent):

                template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
                    binlogevent.schema, binlogevent.table,
                    ', '.join(map(lambda key: '`%s`' % key, row['values'].keys())),
                    ', '.join(map(lambda v: "'%s'" % v,row["values"].values()))
                )
                time=datetime.datetime.fromtimestamp(binlogevent.timestamp)
                sql="""INSERT INTO binlog.binlogsql(tablename,objec_schema,action,time,sqlinfo) VALUES ("%s","%s","%s","%s","%s")""" \
                    % (binlogevent.table,binlogevent.schema,'INSERT',time,template)


                con.execute(sql)
                db.commit()
                # db.close()
                print template


            elif isinstance(binlogevent, DeleteRowsEvent):
                template = 'DELETE FROM `{0}`.`{1}` WHERE {2} ;'.format(
                    binlogevent.schema, binlogevent.table, ' AND '.join(map(compare_items, row['values'].items()))


                )
                time = datetime.datetime.fromtimestamp(binlogevent.timestamp)
                sql = """INSERT INTO binlog.binlogsql(tablename,objec_schema,action,time,sqlinfo) VALUES ("%s","%s","%s","%s","%s")""" \
                      % (binlogevent.table, binlogevent.schema, 'DELETE', time, template)

                con.execute(sql)
                db.commit()


                print template
            elif isinstance(binlogevent, UpdateRowsEvent):
                template='UPDATE `{0}`.`{1}` set {2} WHERE {3} ;'.format(
                    binlogevent.schema,binlogevent.table,','.join(map(compare_items,row["after_values"].items())),
                    ' AND '.join(map(compare_items,row["before_values"].items())),datetime.datetime.fromtimestamp(binlogevent.timestamp)
                )
                time = datetime.datetime.fromtimestamp(binlogevent.timestamp)
                sql = """INSERT INTO binlog.binlogsql(tablename,objec_schema,action,time,sqlinfo) VALUES ("%s","%s","%s","%s","%s")""" \
                      % (binlogevent.table, binlogevent.schema, 'UPDATE', time, template)

                con.execute(sql)
                db.commit()
                print template
                # print map(compare_items,row["after_values"].items())
    stream.close()


if __name__ == "__main__":
    main()

db.close()