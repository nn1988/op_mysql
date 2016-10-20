#
# coding=utf-8
# Created by Lee_np on 16/10/13.


import pymysql


def mysql_open(user_name, pwd, mysql_name):
    """Open the  target MySQL

    Args:
        user_name: string type, your user ID in MySQL
        pwd: string type, your password
        mysql_name: string type, database name

    Return:
        db: pymysql.connections.Connection
    """

    items = [
        ('host', '127.0.0.1'),
        ('port', 3306),
        ('user', user_name),
        ('password', pwd),
        ('db', mysql_name),
        ('charset', 'utf8mb4')
    ]
    config = dict(items)
    db_mysql = pymysql.connect(**config)

    return db_mysql


def sql_create_table():
    sql_str = ("create stored_bid_table("
               "bid_id int auto_increment,"
               "bid_title text,"
               "bid_type text,"
               "bid_location_pv text,"
               "bid_location_city text"
               "bid_category text,"
               "bid_date date,"
               "bid_industry text,"
               "bid_create_data datetime,"
               "primary key (bid_id)) character set = utf8")
    return sql_str


def mysql_read_file():
    """Read the record file form disk

    Just for the test case
    """

    list_items = []
    try:
        file_pt = 'shuju.txt'
        file_hd = open(file_pt, 'r')
        file_hd2 = open('cities3.txt', 'r')
        hd2_list = list(file_hd2)

        for j, i in enumerate(file_hd):
            sections = i.split(',')
            resource_items = []
            int_list_len = len(sections)
            if int_list_len == 0:
                print 'In the list end or index Error!'
            elif int_list_len == 5:
                resource_items = [
                    ('title', str(sections[0])),
                    ('type', str(sections[1])),
                    ('location_pv', str(sections[2])),
                    ('location_city', str(hd2_list[j])),
                    ('category', str(sections[3])),
                    ('date', str(sections[int_list_len - 1])),
                    ('dust', str(sections[4:-1]))
                ]
            elif int_list_len > 5:
                i = 1
                str_dust = str(sections[4])
                while i < (int_list_len-5):
                    str_dust = str_dust + ',' + str(sections[4+i])
                    i += 1
                resource_items = [
                    ('title', str(sections[0])),
                    ('type', str(sections[1])),
                    ('location_pv', str(sections[2])),
                    ('location_city', str(hd2_list[j])),
                    ('category', str(sections[3])),
                    ('date', str(sections[int_list_len - 1])),
                    ('dust', str_dust)
                ]
            else:
                break
            dict_items = dict(resource_items)
            list_items.append(dict_items)
    except IOError:
        print 'File name error! Please check it'

    return list_items

# def mysql_execute(db_curosr, str_sql):
#     db_curosr.execute(str_sql)


def mysql_use():
    """Open the mysql

    Two situations, local or remote MySQL
    """

    user_name = 'root'
    pwd = 'jsgh'
    mysql_name = 'bid_data'

    bid_db = mysql_open(user_name, pwd, mysql_name)
    bid_curosr = bid_db.cursor()
    bid_curosr.execute('TRUNCATE TABLE show_bid_table')
    bid_db.commit()

    sql_str = ("insert into show_bid_table"
               "(bid_title, bid_type, bid_location_pv, bid_location_city, "
               "bid_category, bid_date, bid_industry) values"
               "(%(title)s, %(type)s, %(location_pv)s, %(location_city)s, "
               "%(category)s, %(date)s, %(dust)s)")

    sql_str1 = "select bid_id from show_bid_table order by bid_id desc"

    list_items = mysql_read_file()
    for i in list_items:
        value = i

        try:
            bid_curosr.execute(sql_str, value)
            bid_db.commit()

        except pymysql.err:
            print pymysql.err
            bid_db.rollback()
        bid_curosr.execute(sql_str1)
        data = bid_curosr.fetchone()
        print data[0]
    bid_db.close()


if __name__ == '__main__':
    mysql_use()
    # db_cursor = mysql_open('root', 'jsgh', 'bid_data').cursor()
    # db_cursor.execute(sql_create_table)
    # db_cursor.close()
