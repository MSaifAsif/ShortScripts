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

unix_epoch = '2014-01-01 00:00:01'


def main(no_users, no_services):
    try:
        filename = str(int(time.time()))+str(no_users)+"_data.tsv"
        new_file = open(filename, 'w')
        user_ip_file = open("user_ips.csv", 'r')
        service_url_file = open("unique_urls_cws.csv", "r")
        useragent_file = open("unique_user_agents.csv", "r")
        destip_file = open("unique_dest_ip.csv", "r")
        index = 0
        no_users=int(no_users)
        no_services=int(no_services)
        users = user_ip_file.readlines()
        services = service_url_file.readlines()
        user_agent= useragent_file.readlines()
        dest_ip = destip_file.readlines()
        print "**************************************"
        print "Generating Valid Data"
        for user in range(0, no_users):
            for service in range (0, no_services):
                s_bytes = random.randint(100,1000)
                r_bytes = random.randint(100,1000)
                new_file.write("%s\t%s\t\t%s\tGET\t%s\t%s\t80\t\t\t%s\t-\t\t%d\t%d\timage/jpeg\t%s\tc:infr\tdefault\tallow\t\t\t%s\t1278716032\t\n" % (get_new_date(unix_epoch), users[user].strip(), users[user].strip(), services[service].split(":")[0], services[service].split("//")[1].strip(),user_agent[service].strip(),s_bytes,r_bytes,dest_ip[service].strip(),users[user].strip()))
                index += 1
        print "Total Rows Added %d" % index
        empty_index = index
        index = 0
        print "Generating Invalid Data"
        print empty_index / 10
        while index !=  1000:
            s_bytes = random.randint(100,1000)
            r_bytes = random.randint(100,1000)
            new_file.write("%s\t%s\t\t%s\tGET\t%s\t********************\t80\t\t\t%s\t-\t\t%d\t%d\timage/jpeg\t0.0.0.0\tc:infr\tdefault\tallow\t\t\t%s\t1278716032\t\n" % (get_new_date(unix_epoch), users[user].strip(), users[user].strip(), services[service].split(":")[0], user_agent[service].strip(), s_bytes, r_bytes, users[user].strip()))
            empty_index -= 1
            index +=1


        print "Total Rows Added %d" % index
        print "Data File of name %s with %d user(s) and %d service URL(s) Generated." % (filename, no_users, no_services)
        print "**************************************"
    except Exception, e:
        print e
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

    if day == 28:
        day = 1
        month = month + 1

    if month == 12:
        month = 1
        year = year + 1

    secs = secs + 1

    if secs == 59:
        secs = 0
        mins = mins + 1

    if mins == 59:
        mins = 0
        hour = hour + 1

    if hour == 23:
        hour = 0
        day = 1

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
    if len(sys.argv) == 3:
        main(sys.argv[1], sys.argv[2])
    else:
        print "Usage: python data_gen_1031.py Users Services"
