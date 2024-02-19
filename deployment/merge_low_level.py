import os, sys


def format_memory_stat(memInfo):
    return memInfo.split(" ", 1)[1].strip(" ").split(" ")[0]


def format_cpu_stat(cpuInfo):
    totalCpuTime = sum(cpuInfo)
    idleCpuTime = sum(cpuInfo[3:5])
    return (1 - idleCpuTime / totalCpuTime) * 100


def format_disk_stat(diskInfo):
    diskInfo = diskInfo.split("sda ")[1].split(" ")
    diskInfo = [x for i, x in enumerate(diskInfo) if i in [0, 3, 4, 7]]
    return [int(x) for x in diskInfo]


def format_net_stat(netInfo):
    return [int(x) for x in netInfo.strip("IpExt: ").split(" ")][6:8]



def get_memory_records(memBlock):
    return [memBlock[0], memBlock[1], memBlock[2]]



def split_block(block):
    '''Splits a block of data into multiple fine-grained records separated by $
       Ecah records correponds to one step in the monitoring process'''
    return [x.split("\n") for x in block.strip("\n").split("$\n")]



def main(f1, f2, f3, f4, res):
    # Open the files
    resFile =  open(res, 'w')
    memFile = open(f1, 'r')
    cpuFile = open(f2, 'r')
    diskFile = open(f3, 'r')
    netFile = open(f4, 'r')

    # Read the contents of the files
    memStats = memFile.read()
    cpuStats = cpuFile.read()
    diskStats = diskFile.read()
    netStats = netFile.read()

    # Split the blocks by records
    memStats = split_block(memStats)
    cpuStats = cpuStats.split("\n")
    diskStats = diskStats.split("\n")
    netStats = netStats.split("\n")
    

    csvLines = []

    previous = [0] * 8

    for i in range(len(memStats)):
        memLine = get_memory_records(memStats[i])
        memLine = [int(format_memory_stat(x)) for x in memLine]
        csvLines.append(memLine)

        cpuLine = cpuStats[i]
        cpuLine = [int(x) for x in cpuLine.split(" ")[2:-2]]
        cpuInfo = [x - y for (x, y) in zip(cpuLine, previous)]
        previous = cpuLine
        cpuInfo = format_cpu_stat(cpuInfo)
        csvLines[i].append(cpuInfo)

        diskLine = diskStats[i]
        diskLine = format_disk_stat(diskLine)
        csvLines[i] += diskLine

        netLine = netStats[i]
        netLine = format_net_stat(netLine)
        csvLines[i] += netLine



    res = []
    for i in range(len(csvLines)-1, 0, -1):
        for j in range(4, len(csvLines[i])):
            csvLines[i][j] -= csvLines[i-1][j]
    
    for j in range(4, len(csvLines[i])):
        csvLines[0][j] = 0

    
    csvLines = [["MemTotal", "MemFree", "MemAvailable", "CpuUsage", "DiskReads", "DiskReadTime", "DiskWrites", "diskWriteTime", "NetIn", "NetOut"]] + csvLines

    for l in csvLines:
        resFile.write(",".join([str(x) for x in l])+"\n")

    resFile.close()
    memFile.close()
    cpuFile.close()
    diskFile.close()
    netFile.close()




if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])