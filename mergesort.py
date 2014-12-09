
__author__ = 'Saif Asif'

import os
import sys
import csv
csv.field_size_limit(sys.maxint)
import heapq
from optparse import OptionParser

# temporary directory to store the sub-files
# This will be deleted once sorting is done. Do not delete from outside
TMP_DIR = '.sort_temp'
MAX_SPLIT_SIZE = 1 * 1024 * 1024 # in MiB

if not os.path.exists(TMP_DIR):
    try:
        os.mkdir(TMP_DIR)
    except Exception as e:
        print str(e)


def sort(input_filename, columns, output_filename=None, has_header=True, delimiter=',', quoting=csv.QUOTE_MINIMAL):
    """
    Sort the CSV file on disk rather than in memory
    The merge sort algorithm is used to break the file into smaller sub files and
    @param input_filename: the CSV filename to sort
    @param columns: a list of column indices (0 based) to sort on
    @param output_filename: optional filename for sorted file. If not given then input file will be overriden.
    @param max_size: the maximum size (in MB) of CSV file to load in memory at once
    @param has_header: whether the CSV contains a header to keep separated from sorting
    @param delimiter: character used to separate fields, default ','
    @param quoting: is qouted or not
    @return:
    """

    reader = csv.reader(open(input_filename), delimiter=delimiter)
    if has_header:
        header = reader.next()
    else:
        header = None

    filenames = csvsplit(reader, MAX_SPLIT_SIZE)
    print 'Merging %d splits' % len(filenames)
    for filename in filenames:
        memorysort(filename, columns)
    sorted_filename = mergesort(filenames, columns)
  
    writer = csv.writer(open(output_filename or input_filename, 'w'), delimiter=delimiter, quoting=quoting)
    if header:
        writer.writerow(header)
    generate_result(writer, sorted_filename)


def generate_result(writer, sorted_filename): 
    """generate final output file
    """
    for row in csv.reader(open(sorted_filename)):
        writer.writerow(row)
    os.remove(sorted_filename)
    try:
        os.rmdir(TMP_DIR)
    except OSError:
        pass


def csvsplit(reader, max_size):
    """Split into smaller CSV files of maximum size and return the list of filenames
    """
    writer = None
    current_size = 0
    split_filenames = []

    # break CSV file into smaller merge files
    for row in reader:
        if writer is None:
            filename = os.path.join(TMP_DIR, 'split%d.csv' % len(split_filenames))
            writer = csv.writer(open(filename, 'w'))
            split_filenames.append(filename)

        writer.writerow(row)
        current_size += sys.getsizeof(row)
        if current_size > max_size:
            writer = None
            current_size = 0
    return split_filenames


def memorysort(filename, columns):
    """Sort this CSV file in memory on the given columns
    """
    rows = [row for row in csv.reader(open(filename))]
    rows.sort(key=lambda row: get_key(row, columns))
    writer = csv.writer(open(filename, 'wb'))
    for row in rows:
        writer.writerow(row)


def get_key(row, columns):
    """Get sort key for this row
    """
    return [row[column] for column in columns]


def decorated_csv(filename, columns):
    """Iterator to sort CSV rows
    """
    for row in csv.reader(open(filename)):
        yield get_key(row, columns), row


def mergesort(sorted_filenames, columns, nway=2):
    """Merge these 2 sorted csv files into a single output file
    """
    merge_n = 0
    while len(sorted_filenames) > 1:
        merge_filenames, sorted_filenames = sorted_filenames[:nway], sorted_filenames[nway:]
        readers = map(open, merge_filenames)

        output_filename = os.path.join(TMP_DIR, 'merge%d.csv' % merge_n)
        print 'created splitee: ', output_filename
        writer = csv.writer(open(output_filename, 'w'))
        merge_n += 1

        for _, row in heapq.merge(*[decorated_csv(filename, columns) for filename in merge_filenames]):
            writer.writerow(row)
        
        sorted_filenames.append(output_filename)
        for filename in merge_filenames:
            print 'deleting splitee:', filename
            os.remove(filename)
    return sorted_filenames[0]


def main():

    cmdOptionsParser = OptionParser()
    # coulumns names
    cmdOptionsParser.add_option('-c', '--column', dest='columns', action='append', type='int', help='index of CSV to sort on')

    # header
    cmdOptionsParser.add_option('-n', '--no-header', dest='has_header', action='store_false', default=True, help='set CSV file has no header')

    # custom delimiter defaultes to comma
    cmdOptionsParser.add_option('-d', '--delimiter', default=',', help='set CSV delimiter (default ",")')
    parsedArguments, candidate_files = cmdOptionsParser.parse_args()

    # Coloumn and file names are required params
    if not candidate_files:
        cmdOptionsParser.error('What CSV file should be sorted?')
    elif not parsedArguments.columns:
        cmdOptionsParser.error('Which columns should be sorted on?')
    else:
        # escape backslashes
        parsedArguments.delimiter = parsedArguments.delimiter.decode("string_escape")
        sort(candidate_files[0], columns=parsedArguments.columns, has_header=parsedArguments.has_header, delimiter=parsedArguments.delimiter)

 
if __name__ == '__main__':
    main()
