
import time



def clean_transactions(line):
    try:
        fields = line.split(',')
        if len(fields)!=5 or fields[2]=='time':
            return False


        return True

    except:
        return False

def remove_header(line):
    try:

        if line[1][0][0]=='time':
            return False


        return True

    except:
        return False

def clean_vout(line):
    try:
        fields = line.split(',')
        if len(fields)!=4:
            return False

        return True

    except:
        return False

def clean_vin(line):
    try:
        fields = line.split(',')
        if len(fields)!=3:
            return False

        return True

    except:
        return False


def filter_small(line):
    try:

        if line[1][0][0] == line[1][1][0]:
            return True

        return False

    except:
        return False


def filter_large(line):
    try:

        if line[1][0][0] != line[1][1][0]:
            return True

        return False

    except:
        return False



def group_transactions(time_epoch):
    if time_epoch != 'time':
        year = time.strftime("%Y",time.gmtime(int(time_epoch)))
        yearNum = int(year)
        if yearNum == 2009:
            return "2009"
        if yearNum == 2010:
            return "2010"
        if yearNum == 2011:
            return "2011"
        if yearNum == 2012:
            return "2012"
        if yearNum == 2013:
            return "2013"
        if yearNum == 2014:
            return "2014"


