#!/bin/python

'''
Author: "Asim Kazmi" <asim.kazmi@elastica.co>
Rev : 1.0 2014-11-10

Desc: Firewall Data Generation Tool to create large Synthetic Log files for Different Firewalls.

Revisions:
1.0 ---- Supports log generation for CWS In this revision. This rev does not include any templates for additional firewalls.
'''

import re
import sys
import time
import random
import logging
logger=logging.getLogger("Data_Gen_Logger:")

unix_epoch = '2014-01-01 00:00:01'


def main(no_users, logs_per_user, invalid_rows):
    try:
        filename = str(int(time.time())) + str(no_users) + "_data.tsv"
        new_file = open("../cws_data/" + filename, 'w')
        user_ip_file = open("user_ips.csv", 'r')
        service_url_file = open("unique_urls_cws.csv", "r")
        useragent_file = open("unique_user_agents.csv", "r")
        destip_file = open("unique_dest_ip.csv", "r")
        row_index = 0
        no_users = int(no_users)
        logs_per_user = int(logs_per_user)
        invalid_rows = int(invalid_rows)
        users = user_ip_file.readlines()
        services = service_url_file.readlines()
        user_agent = useragent_file.readlines()
        dest_ip = destip_file.readlines()
        print "**************************************"
        print "Generating Valid Data"

        #Writing Valid Rows to the file
        for user in range(0, no_users):
            user_index = 0
            for logs in range(0, logs_per_user):
                s_bytes = random.randint(100, 1000)
                r_bytes = random.randint(100, 1000)
                service = services[random.randint(0,200000)]
                user_agents = user_agent[random.randint(0,990)]
                new_file.write(
                    "%s\t%s\t\t%s\tGET\t%s\t%s\t80\t\t\t%s\t-\t\t%d\t%d\timage/jpeg\t%s\tc:infr\tdefault\tallow\t\t\t%s\t1278716032\t\n" % (
                    get_new_date(unix_epoch), users[user].strip(), users[user].strip(), service.split(":")[0],
                    service.split("//")[1].strip(), user_agents.strip(), s_bytes, r_bytes,
                    dest_ip[logs_per_user].strip(), users[user].strip()))
                user_index += 1
            print "Total Rows Added for user %s : %d" % (users[user].strip(), user_index)
            row_index += 1

        # Writing Invalid Rows to the file
        print "Total Valid Rows Added: %d" % row_index
        print "Generating Invalid Data"
        for rows in range(0, invalid_rows):
            s_bytes = random.randint(100, 1000)
            r_bytes = random.randint(100, 1000)
            new_file.write(
                "%s\t%s\t\t%s\tGET\t%s\t********************\t80\t\t\t%s\t-\t\t%d\t%d\timage/jpeg\t0.0.0.0\tc:infr\tdefault\tallow\t\t\t%s\t1278716032\t\n" % (
                    get_new_date(unix_epoch), users[user].strip(), users[user].strip(), service.split(":")[0],
                    user_agents.strip(), s_bytes, r_bytes, users[user].strip()))
        print "Total Rows Added %d" % invalid_rows
        print "Data File of name ../cws_data/%s with %d user(s)." % (
            filename, no_users)
        print "**************************************"
    except Exception, e:
        logger.exception(e)
        raise e
    finally:
        new_file.close()

def get_new_date(current_epoch):
    date, time = current_epoch.split()
    date_elems = date.split('-')
    time_elems = time.split(':')

    year = int(date_elems[0])
    month = int(date_elems[1])
    day = int(date_elems[2])

    hour = int(time_elems[0])
    mins = int(time_elems[1])
    secs = int(time_elems[2])

    secs = random.randint(0,59)
    mins = random.randint(0,59)
    hour = random.randint(0,23)

    month_impl = str(month)
    if len(month_impl) == 1:
        month_impl = '0' + month_impl

    day_impl = str(day)
    if len(day_impl) == 1:
        day_impl = '0' + day_impl

    hour_impl = str(hour)
    if len(hour_impl) == 1:
        hour_impl = '0' + hour_impl

    secs_impl = str(secs)
    if len(secs_impl) == 1:
        secs_impl = '0' + secs_impl

    min_impl = str(mins)
    if len(min_impl) == 1:
        min_impl = '0' + min_impl

    global unix_epoch
    unix_epoch = str(year) + '-' + month_impl + '-' + day_impl + ' ' + hour_impl + ':' + min_impl + ':' + secs_impl
    return unix_epoch


if __name__ == '__main__':
    if len(sys.argv) == 4:
        main(sys.argv[1], sys.argv[2], sys.argv[3])
    else:
        print "Usage: python data_gen_cws.py Users Logs_Per_User Invalid_Rows"
