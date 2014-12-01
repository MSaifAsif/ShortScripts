#!/bin/python

"""
Author: "Asim Kazmi" <asim.kazmi@elastica.co>
Rev : 1.0 2014-10-30
Rev: 2.0 2014-11-11
Rev: 3.0 2014-11-20
Rev: 4.0 2014-11-26
Rev: 5.0 2014-12-01

Desc: Firewall Data Generation Tool to create large Synthetic Log files for Different Firewalls.

Revisions:
1.0 - Supports log generation for CWS In this revision. This rev does not include support for additional firewalls.
2.0 - Support added for randomizing output of time stamp, useragents, destination IPs etc.
3.0 - Added modularity in code and config file reading capability.
4.0 - Added functions to bind service urls with destination IPs. Also removed some redundant variables.
5.0 - Added support for generating data for multiple days. Added support for WSA file format.
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
from calendar import timegm
from ConfigParser import SafeConfigParser

# Global Declaration
logger = logging.getLogger("Data Gen Logger:")


def main(genutility, logutility):
    logutility.prepare_stream_logger(genutility.logger_level)
    try:
        genutility.prepare_data()
    except Exception, e:
        ScriptLogger.exception(e)


#----------------------------------------------------------------------------------------------------------------------#

class DataGenUtility(object):
    def __init__(self, config_file='data_gen.conf'):
        """
            Initializes required variables for data generation.
            :param config_file: Name of file that contains configuration parameters
            :return:
        """
        self.data_path = ''
        self.logger_path = ''
        self.timestamp_format = ''
        self.users = ''
        self.logs_per_user = ''
        self.invalid_rows = ''
        self.log_format = ''
        self.hours_per_day = ''
        self.apps_per_user = ''
        self.user_agents_per_user = ''
        self.logger_level = ''
        self.timeformat = ''
        self.IPstart = ''
        self.servicefile_path = ''
        self.useragentfile_path = ''
        self.no_of_days = ''
        self.zip_flag = ''
        # Private helper function.
        self._parse_config(config_file)

    #==================================================================================================================#

    def _parse_config(self, config_file):
        """
            Parse configfile and setup env.
            :param config_file: Name of file that contains configuration parameters
            :return:
        """
        parser = SafeConfigParser()
        parser.read(config_file)

        self.data_path = parser.get('SYSTEM', 'DATA_PATH')
        self.logger_path = parser.get('SYSTEM', 'LOGGER_PATH')
        self.servicefile_path = parser.get('SYSTEM', 'URL_FILE_PATH')
        self.useragentfile_path = parser.get('SYSTEM', 'AGENT_FILE_PATH')
        self.time_format = parser.get('SYSTEM', 'TIME_FORMAT')
        self.no_of_days = parser.get('SYSTEM', 'NO_OF_DAYS')
        self.users = parser.get('SYSTEM', 'TOTAL_USERS')
        self.logs_per_user = parser.get('SYSTEM', 'LOGS_PER_USER')
        self.invalid_rows = parser.get('SYSTEM', 'INVALID_ROWS')
        self.log_format = parser.get('SYSTEM', 'LOG_FORMAT')
        self.hours_per_day = parser.get('SYSTEM', 'HOURS_PER_DAY')
        self.apps_per_user = parser.get('SYSTEM', 'APPS_PER_USER')
        self.user_agents_per_user = parser.get('SYSTEM', 'USER_AGENT_PER_USER')
        self.logger_level = parser.get('SYSTEM', 'LOGGER_LEVEL')
        self.IPstart = parser.get('SYSTEM', 'USER_IP')
        self.zip_flag = parser.get('SYSTEM', 'ZIP_FILE')
        self.filename = str(int(time.time())) + '-day-' + str(self.time_format[8]) + str(self.time_format[9]) + '-data'
        self.new_file = open(self.data_path + self.filename, 'wb')


    #==================================================================================================================#

    def prepare_data(self):
        """

        :return:
        """
        try:

            service_url_file = open(self.servicefile_path, "r")
            useragent_file = open(self.useragentfile_path, "r")
            no_users = int(self.users)
            logs_per_user = int(self.logs_per_user)
            invalid_rows = int(self.invalid_rows)
            services = service_url_file.readlines()
            user_agent = useragent_file.readlines()
            if str(self.log_format) != 'CWS' and str(self.log_format) != 'WSA':
                print("Log Format Does Not Match the Required Formats.")
            else:
                self.create_data(services, user_agent)

        except Exception, e:
            logger.exception(e)
            self.new_file.close()
            sys.exit(1)
        finally:
            print "Exiting Program...\n"

    #==================================================================================================================#


    def create_data(self, services, user_agent):
        """
        Function to create the required data for any log file format.
        :param filename: File
        :param services:
        :param user_agent:
        :param new_file:
        :return:
        """
        row_index = 0

        try:
            print "**************************************"
            print "Generating Valid Data...\n"
            start_time = strftime('%Y-%m-%d %H:%M:%S', gmtime())
            print "Data Generation Started at: %s" % start_time

            # Writing Valid Rows to the file
            # User Loop to pick number of users from the user file based on the input value
            for days in range(0, int(self.no_of_days)):

                service_range_start = 1
                service_range_end = int(self.apps_per_user)
                agent_index = int(self.user_agents_per_user) - 1
                intip = int(ipaddress.IPv4Address(u'%s' % self.IPstart))

                for user in range(0, int(self.users)):

                    user_ip = str(ipaddress.IPv4Address(intip))

                    # Loop to Generate Log per user.
                    for logs in range(0, int(self.logs_per_user)):
                        # Randomizing sent and received bytes
                        s_bytes = random.randint(1000, 5000)
                        r_bytes = random.randint(1000, 5000)
                        service_list = random.randint(service_range_start, service_range_end)
                        start = (agent_index - (int(self.user_agents_per_user) - 1 ))

                        # Generating the services, protocols, Agents and Destination IPs from dataset.
                        proto_feed = services[service_list].split(":")[0].strip()
                        service_feed = services[service_list].split("//")[1].split(",")[0].strip()
                        agent_feed = user_agent[(random.randint(start, agent_index))].strip()
                        dest_feed = services[service_list].split(",")[1].strip()

                        if str(self.log_format) == "CWS":
                            self.write_cws_file(days, user_ip, proto_feed, service_feed, agent_feed, dest_feed, s_bytes, r_bytes)
                        elif str(self.log_format) == "WSA":
                            self.write_wsa_file(days, user_ip, proto_feed, service_feed, agent_feed, dest_feed, s_bytes, r_bytes)
                        else:
                            print "No File generated"
                            break

                        row_index += 1

                    # Since the total number of user agents may not be equal to the total number of users.
                    if agent_index >= len(user_agent) - 1:
                        agent_index = 0

                    agent_index += int(self.user_agents_per_user)
                    service_range_start += int(self.apps_per_user)

                    if service_range_start > (int(len(services)) - 1) - int(self.apps_per_user):
                        service_range_start = 0
                        service_range_end = int(self.apps_per_user)
                    if service_range_start == (int(len(services)) - 1) - int(self.apps_per_user):
                        service_range_end += 89
                    else:
                        service_range_end += int(self.apps_per_user)
                    intip += 1
                    if agent_index >= int(len(user_agent) - 1):
                        agent_index = 0
                # print "Total Rows Added for user %s : %d" % (users[user].strip(), user_index)


                # Writing Invalid Rows to the file
                for rows in range(0, int(self.invalid_rows)):
                    s_bytes = random.randint(100, 500)
                    r_bytes = random.randint(100, 500)

                    if str(self.log_format) == "CWS":
                        self.write_cws_invalid_file(user_ip, proto_feed, service_feed, agent_feed, dest_feed, s_bytes,
                                                    r_bytes)
                    elif str(self.log_format) == "WSA":
                        self.write_wsa_invalid_file(user_ip, proto_feed, service_feed, agent_feed, dest_feed, s_bytes,
                                                    r_bytes)
                    else:
                        print "No File generated"
                        break

            print "Data Generation Finished at: " + strftime('%Y-%m-%d %H:%M:%S', gmtime())
            self.os_operations(self.new_file)

            print "\nTotal Valid Rows Added: %d" % row_index
            print "Total Invalid Rows Added %d" % int(self.invalid_rows)
            print "Total Rows Added to file: %d\n" % int(
                (int(self.no_of_days) * int(self.users) * (int(self.logs_per_user)) + int(self.invalid_rows)))

            if str(self.zip_flag) == 'True':
                zip_ext = '.gz'
            else:
                zip_ext = ''

            print "Data File of name %s%s_sorted.tsv%s with %d user(s)." % (
                    self.data_path, self.filename, zip_ext, int(self.users))
            print "**************************************"

        except Exception, e:
            logger.exception(e)
        finally:
            self.new_file.close()

    #==================================================================================================================#


    def write_cws_file(self, days, user_ip, proto_feed, service_feed, agent_feed, dest_feed, s_bytes, r_bytes):
        """
        Writing the Data Rows for each user. Parameterized Fields include Username, Service name, Sent Bytes,
        Received bytes, User Agent, destination IP and uri scheme. User Agensts are selected one for each user.
        Verify the provided format from console input. CWS, WSA, ASA etc.

        :param user_ip:
        :param proto_feed:
        :param service_feed:
        :param agent_feed:
        :param dest_feed:
        :param s_bytes:
        :param r_bytes:
        :return:
        """
        self.new_file.write(
            '%s\t%s\t\t%s\tGET\t%s\t%s\t80\t\t\t%s\t-\t%d\t%d\t200\timage/jpeg\t%s\tc:infr\tdefault\tallow\t\t\t180.143.124.98\t1278716032\t\n' % (
                self.get_new_date(days).strip(), str(user_ip), str(user_ip), proto_feed, service_feed, agent_feed,
                s_bytes, r_bytes, dest_feed))

    #==================================================================================================================#

    def write_wsa_file(self, days, user_ip, proto_feed, service_feed, agent_feed, dest_feed, s_bytes, r_bytes):
        """
        Writing the Data Rows for each user. Parameterized Fields include Username, Service name, Sent Bytes,
        Received bytes, User Agent, destination IP and uri scheme. User Agensts are selected one for each user.
        Verify the provided format from console input. CWS, WSA, ASA etc.

        :param user_ip:
        :param proto_feed:
        :param service_feed:
        :param agent_feed:
        :param dest_feed:
        :param s_bytes:
        :param r_bytes:
        :return:
        """
        self.new_file.write(
            '%s 222 %s TCP_CLIENT_REFRESH_MISS 200 %d POST %s %s DIRECT %s application/image DEFAULT_CASE_12-DefaultGroup-DefaultGroup-NONE-NONE-NONE-DefaultGroup <-,-,-,""-"",-,-,-,-,""-"",-,-,-,""-"",-,-,""-"",""-"",-,-,-,-,""-"",""-"",""-"",""-"",""-"",""-"",55.50,0,-,""-"",""-""> - %s\n' % (
                self.get_new_date(days), str(user_ip), (s_bytes + r_bytes), service_feed, str(user_ip),
                dest_feed, agent_feed))


    #==================================================================================================================#

    def write_cws_invalid_file(self, user_ip, proto_feed, service_feed, agent_feed, dest_feed, s_bytes, r_bytes):
        """
        To make a data row invalid, serviceip contains hardcoded * signs and 0.0.0.0 as destination IP.
        Username is set by default to INVALID_User. These logs should not be detected during data processing.
        :param user_ip:
        :param proto_feed:
        :param service_feed:
        :param agent_feed:
        :param dest_feed:
        :param s_bytes:
        :param r_bytes:
        :return:
        """

        self.new_file.write(
            '%s\t10.0.1.1\t\tInvalid_User\tGET\t%s\t********************\t80\t\t\t%s\t-\t%d\t%d\t500\timage/jpeg\t0.0.0.0\tc:infr\tdefault\tallow\t\t\t190.180.100.73\t1278716032\t\n' % (
                self.get_new_date(), proto_feed, agent_feed, s_bytes, r_bytes))

    #==================================================================================================================#

    def write_wsa_invalid_file(self, user_ip, proto_feed, service_feed, agent_feed, dest_feed, s_bytes, r_bytes):
        """
        # To make a data row invalid, serviceip contains hardcoded * signs and 0.0.0.0 as destination IP.
        :param user_ip:
        :param proto_feed:
        :param service_feed:
        :param agent_feed:
        :param dest_feed:
        :param s_bytes:
        :param r_bytes:
        :return:
        """
        # Username is set by default to INVALID_User. These logs should not be detected during data processing.

        self.new_file.write(
            '%s 222 INVALID_USER TCP_CLIENT_REFRESH_MISS 200 %d POST somegoodwebsite.com INVALID_USER DIRECT 0.0.0.0 application/image DEFAULT_CASE_12-DefaultGroup-DefaultGroup-NONE-NONE-NONE-DefaultGroup <-,-,-,""-"",-,-,-,-,""-"",-,-,-,""-"",-,-,""-"",""-"",-,-,-,-,""-"",""-"",""-"",""-"",""-"",""-"",55.50,0,-,""-"",""-""> - %s\n' % (
                self.get_new_date(), (s_bytes + r_bytes), agent_feed))

    #==================================================================================================================#

    def get_new_date(self, days):
        """
        # This function Generates Random time stamp witin a day range.
        :return:
        """
        date, ttime = self.time_format.split()
        date_elems = date.split('-')
        time_elems = ttime.split(':')

        year = int(date_elems[0])
        month = int(date_elems[1])
        day = int(date_elems[2])

        hour = int(time_elems[0])
        mins = int(time_elems[1])
        secs = int(time_elems[2])

        if day == 30:
            day = 1
            month += 1
        else:
            day = int(days) + 1

        if month == 12:
            month = 1
            year += 1

        # Randomizing the timestamp with random hours, mins and seconds.
        num_hours = int(self.hours_per_day)
        if num_hours > 24:
            num_hours = 24

        secs = random.randint(0, 59)
        mins = random.randint(0, 59)
        hour = random.randint(0, num_hours - 1)

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

        if str(self.log_format) == "CWS":
            timeformat = str(
                year) + '-' + month_impl + '-' + day_impl + ' ' + hour_impl + ':' + min_impl + ':' + secs_impl
        elif str(self.log_format) == "WSA":
            timeformat = (year, int(month_impl), int(day_impl), int(hour_impl), int(min_impl), int(secs_impl))
            timeformat = timegm(timeformat)
        return timeformat

    #==================================================================================================================#


    def os_operations(self, new_file):
        """
        :param new_file:
        :return:
        """
        # Sorting the file based on time.
        print "\nFile Sorting Started at: " + strftime('%Y-%m-%d %H:%M:%S', gmtime())
        os.system(
            "sort %s%s -k 1 > %s%s_sorted.tsv" % (self.data_path, self.filename, self.data_path, self.filename))
        print "File Sorting Finished at: " + strftime('%Y-%m-%d %H:%M:%S', gmtime())

        # Create Zipped file.
        if str(self.zip_flag) == 'True':
            print "\nStarting to Zip the file: " + strftime('%Y-%m-%d %H:%M:%S', gmtime())
            os.system("gzip %s%s" % (self.data_path, self.filename))

        endtime = strftime('%Y-%m-%d %H:%M:%S', gmtime())
        #print "File completely Zipped: %s\n" % endtime

        # Removing the unsorted file.
        os.system("rm ../cws_data/%s" % self.filename)

        return new_file


    #==================================================================================================================#

#----------------------------------------------------------------------------------------------------------------------#


class ScriptLogger(object):
    def prepare_stream_logger(extra, logger_level):
        """
        Prepare a stream logger

        :param logger: logger to use
        :type logger: logging.Logger instance

        :param extra: Extra information to append in the log. Is a string
        :type extra: str

        :param filename: Filename to be used to logging. Absolute path needs to be given otherwise in cwd
        :type filename: str

        :param debug_level: Debug level to set on the logger
        :type debug_level: one of logging.DEBUG, logging.ERROR, logging.INFO, logging.WARNING
        """

        logger.setLevel(logger_level)
        # logger.addFilter(UserLogFilter(extra))

        sh = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s-%(name)s-%(levelname)s-%(message)s')
        sh.setFormatter(formatter)
        logger.addHandler(sh)

#----------------------------------------------------------------------------------------------------------------------#


if __name__ == '__main__':
    datagenrunner = DataGenUtility()
    logrunner = ScriptLogger()
    main(datagenrunner, logrunner)
