#!/bin/python

"""
Author: "Asim Kazmi" <asim.kazmi@elastica.co>
Rev : 1.0 2014-10-30
Rev: 2.0 2014-11-11

Desc: Firewall Data Generation Tool to create large Synthetic Log files for Different Firewalls.

Revisions:
1.0 - Supports log generation for CWS In this revision. This rev does not include support for additional firewalls.
2.0 - Support added for randomizing output of time stamp, useragents, destination IPs etc.
"""

import os
import sys
import re
import time
import random
import logging
import gzip
import ipaddress
from time import gmtime, strftime
import time

logger = logging.getLogger("Data Gen Logger:")
timestamp_format = '2014-11-01 00:00:01'


def main(no_users, logs_per_user, invalid_rows, format):
    try:
        data_path = "../cws_data/"
        filename = str(int(time.time())) + "-day-" + str(timestamp_format[8]) + str(timestamp_format[9]) + "-data"
        new_file = open(data_path + filename, 'wb')
        user_ip_file = open("user_ips.csv", 'r')
        service_url_file = open("unique_urls_cws.csv", "r")
        useragent_file = open("unique_user_agents.csv", "r")
        destip_file = open("dest_ips_valid.csv", "r")
        no_users = int(no_users)
        logs_per_user = int(logs_per_user)
        invalid_rows = int(invalid_rows)
        users = user_ip_file.readlines()
        services = service_url_file.readlines()
        user_agent = useragent_file.readlines()
        dest_ip = destip_file.readlines()

        # Verify the provided format from console input. CWS, WSA, ASA etc.
        if str(format) == "CWS":
            write_cws_file(filename, no_users, logs_per_user, invalid_rows, users, services, user_agent, dest_ip,
                           new_file)
        else:
            print "\nInvalid Format. Please provide a value like CWS"
    except Exception, e:
        # logger.exception(e)
        print str(e)
        sys.exit(1)
    finally:
        print "Exiting Program...\n"
        sys.exit(0)


def write_cws_file(filename, no_users, logs_per_user, invalid_rows, users, services, user_agent, dest_ip, new_file):
    row_index = 0
    service_range_start = 1
    service_range_end = 100
    intipadd = 168427521
    prepare_stream_logger(logger, None, logging.DEBUG)
    try:
        print "**************************************"
        print "Generating Valid Data..."

        # Writing Valid Rows to the file
        # User Loop to pick number of users from the user file based on the input value
        for user in range(0, no_users):
            user_index = 0
            agent_index = 0
            dest_index = 0
            userip = ipaddress.IPv4Address(intipadd)
            # Loop to Generate Log per user.

            for logs in range(0, logs_per_user):

                # Randomizing sent and received bytes
                s_bytes = random.randint(1000, 5000)
                r_bytes = random.randint(1000, 5000)
                service_list = random.randint(service_range_start, service_range_start)

                # Writing the Data Rows for each user. Parameterized Fields include Username, Service name, Sent Bytes,
                # Received bytes, User Agent, destination IP and uri scheme. User Agensts are selected one for each user.
                new_file.write(
                    "%s\t%s\t\t%s\tGET\t%s\t%s\t80\t\t\t%s\t-\t%d\t%d\t200\timage/jpeg\t%s\tc:infr\tdefault\tallow\t\t\t180.143.124.98\t1278716032\t\n" % (
                        get_new_date(timestamp_format), str(userip), str(userip),
                        services[service_list].split(":")[0],
                        services[service_list].split("//")[1].strip(), user_agent[agent_index].strip(), s_bytes,
                        r_bytes, dest_ip[dest_index].strip()))

                user_index += 1
                row_index += 1
                dest_index += 1
                agent_index += 1

                # Since the total number of user agents may not be equal to the total number of users.
                if agent_index >= len(user_agent) - 1:
                    agent_index = 0

                # Since the total number of destination IPs may not be equal to the total number of users.
                if dest_index >= len(dest_ip) - 1:
                    dest_index = 0

            service_range_start += 100

            if service_range_start > 218001:
                    service_range_start = 0
                    service_range_end = 0
            if service_range_start == 218001:
                service_range_end += 89
            else:
                service_range_end += 100
            intipadd += 1

            # print "Total Rows Added for user %s : %d" % (users[user].strip(), user_index)

        print "Total Valid Rows Added: %d" % row_index

        # Writing Invalid Rows to the file
        print "Generating Invalid Data..."

        for rows in range(0, invalid_rows):
            s_bytes = random.randint(100, 500)
            r_bytes = random.randint(100, 500)

            # To make a data row invalid, serviceip contains hardcoded * signs and 0.0.0.0 as destination IP.
            # Username is set by default to INVALID_User. These logs should not be detected during data processing.

            new_file.write(
                "%s\t10.0.1.1\t\tInvalid_User\tGET\t%s\t********************\t80\t\t\t%s\t-\t%d\t%d\t500\timage/jpeg\t0.0.0.0\tc:infr\tdefault\tallow\t\t\t190.180.100.73\t1278716032\t\n" % (
                    get_new_date(timestamp_format), services[random.randint(0, len(services)-1)].split(":")[0],
                    user_agent[random.randint(0, len(user_agent)-1)].strip(), s_bytes, r_bytes))

        print "Total Invalid Rows Added %d" % invalid_rows

        # Sorting the file based on time and creating a gzip.
        os.system("sort ../cws_data/%s -k 1 > ../cws_data/%s_sorted.tsv" % (filename, filename))

        print "Starting to Zip the file: " + strftime('%Y-%m-%d %H:%M:%S', gmtime())
        os.system("gzip ../cws_data/%s_sorted.tsv" % filename)
        print "File completely Zipped: " + strftime('%Y-%m-%d %H:%M:%S', gmtime())

        # Removing the unsorted file.
        #os.system("rm ../cws_data/%s" % filename)
        total_rows = int(row_index + invalid_rows)
        print "Total Rows Added to file: %d" % total_rows
        print "Data File of name ../cws_data/%s_sorted.tsv.gz with %d user(s)." % (filename, no_users)
        print "**************** **********************"

    except Exception, e:
        logger.exception(e)
        print str(e)
    finally:
        new_file.close()


# This function Generates Random time stamp witin a day range
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

    # Randomizing the timestamp with random hours, mins and seconds.
    secs = random.randint(0, 59)
    mins = random.randint(0, 59)
    hour = random.randint(0, 23)

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

    global timestamp_format
    timestamp_format = str(
        year) + '-' + month_impl + '-' + day_impl + ' ' + hour_impl + ':' + min_impl + ':' + secs_impl
    return timestamp_format

def prepare_stream_logger(logger, extra, debug_level):
    '''
    Prepare a stream logger

    :param logger: logger to use
    :type logger: logging.Logger instance

    :param extra: Extra information to append in the log. Is a string
    :type extra: str

    :param filename: Filename to be used to logging. Absolute path needs to be given otherwise in cwd
    :type filename: str

    :param debug_level: Debug level to set on the logger
    :type debug_level: one of logging.DEBUG, logging.ERROR, logging.INFO, logging.WARNING
    '''
    logger.setLevel(debug_level)
    #logger.addFilter(UserLogFilter(extra))

    sh = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s-%(name)s-%(levelname)s-%(message)s')
    sh.setFormatter(formatter)
    logger.addHandler(sh)

if __name__ == '__main__':
    if len(sys.argv) == 5:
        main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
    else:
        print "Usage: python data_gen_cws.py Users Logs_Per_User Invalid_Rows Format"
        print "Example: python data_gen_cws.py 10 10 10 CWS"



