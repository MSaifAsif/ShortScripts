__author__ = 'Asimkazmi'


import socket
import sys


def main(file_path):
    try:
        addfile = open(file_path, "r")
        urlfile = open(file_path,'r')
        writefile = open("valid_url_dest_ips.csv", "w")
        urls = addfile.readlines()

        # Reads the urls line by line and check if its valid. URLs should not contain protocol like http or https.
        for allurls in xrange(0, len(urls) - 1):
            url = urls[allurls].split('/')[2].strip()

            if str(urls[allurls].split(':')[0].strip()) == 'http':
                proto = 80
            else:
                proto = 443
            try:
                destip = socket.getaddrinfo(url, proto, 0, 0, socket.IPPROTO_TCP)
            except Exception, msg:
                try:
                    destip = socket.getaddrinfo(url, proto, 0, 0, socket.IPPROTO_TCP)
                    print "************ " + destip
                except Exception, msg:
                    #print msg
                    writefile.write("%s, \n" % urls[allurls].strip())
                    continue

            # Based on the output of getaddrinfo, extract the IP out of complete return string tuple.
            for tuple in destip:
                for inner_tuple in tuple:
                    if isinstance(inner_tuple, __builtins__.tuple):
                        if len(inner_tuple) == 2:
                            first_val, second_val = inner_tuple
                            writefile.write("%s,%s\n" % (urls[allurls].strip(), first_val))

    except Exception, e:
        print str(e)
    finally:
        writefile.close()

if __name__ == '__main__':
    if len(sys.argv) == 2:
        main(sys.argv[1])
    else:
        print "Usage: python Lookup_Dest_IP_Tables.py <URL File path>"
