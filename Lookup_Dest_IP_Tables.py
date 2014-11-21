#!/bin/python

"""
 Author: "Asim Kazmi" <asim.kazmi@elastica.co>
Rev : 1.0 2014-10-30


Desc: A Simple function to get valid IPs for a given set of urls.

Revisions:
1.0 - Takes a text file with urls separated on each line and provides valid IP addresses of URLS.

"""



import socket
import sys


def main(file_path):
    try:
        addfile = open(file_path, "r")
        writefile = open("dest_ips_valid.csv", "w")
        urls = addfile.readlines()

        # Reads the urls line by line and check if its valid. URLs should not contain protocol like http or https.
        for allurls in xrange(0, len(urls) - 1):
            try:
                destip = socket.getaddrinfo(urls[allurls].strip(), 80, 0, 0, socket.IPPROTO_TCP)
            except Exception, msg:
                try:
                    destip = socket.getaddrinfo(urls[allurls].strip(), 443, 0, 0, socket.IPPROTO_TCP)
                    print "************ " + destip
                except Exception:
                    print urls[allurls]
                    continue

            # Based on the output of getaddrinfo, extract the IP out of complete return string tuple.
            for tuple in destip:
                for inner_tuple in tuple:
                    if isinstance(inner_tuple, __builtins__.tuple):
                        if len(inner_tuple) == 2:
                            first_val, second_val = inner_tuple
                            writefile.write("%s\n" % first_val)

    except Exception, e:
        print str(e)
    finally:
        writefile.close()

if __name__ == '__main__':
    if len(sys.argv) == 2:
        main(sys.argv[1])
    else:
        print "Usage: python Lookup_Dest_IP_Tables.py <URL File path>"
