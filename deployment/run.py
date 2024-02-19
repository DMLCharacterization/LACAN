import sys

def getMachineNum(machineName):
    return machineName.split(".")[0].split("-")[1]


def getClusterName(machineName):
    return machineName.split("-")[0].strip(" \n")


def main():

    # Dictionaries for automating the generation of Ip addresses
    cluster_dict = {'suno': '172.16.130.', 'uvb': '172.18.132.', 'taurus':'172.16.48.', 'hercule':'172.16.48.',\
        'nova':'172.16.52.', 'orion':'172.16.50.', 'sagittaire':'172.16.49.', 'dahu':'172.16.20.', \
        'yeti':'172.16.19.', 'chetemi':'172.16.37.', 'chiclet': '172.16.39.', 'chifflet':'172.16.38.', \
        'chifflot': '172.16.36.', 'granduc': '172.16.176.', 'petitprince': '172.16.177.', \
        'graoully': '172.16.70.', 'graphene': '172.16.64.', 'graphique': '172.16.67.', 'graphite': '172.16.68.', \
        'grcinq':'172.16.75.', 'grele': '172.16.74.', 'grimani': '172.16.73.', 'grimoire': '172.16.71.', \
        'grisou': '172.16.72.', 'grvingt': '172.16.76.', 'econome': '172.16.192.', 'ecotype': '172.16.193.', \
        'paranoia': '172.16.100.', 'parapide': '172.16.98.', 'parapluie': '172.16.99.', 'parasilo': '172.16.97.', \
        'paravance': '172.16.96.'}

    # Import the filename containing the names of the machines
    filename = sys.argv[1]

    # Read the list of machines from the file
    machineList = []
    with open(filename, "r") as rf:
        machineList = [x.strip("\n") for x in rf.readlines()]
    
    # Set the master node arbitrarily. Here we take the first machine as the master node
    master = machineList[0]
    slaves = machineList[1:]

    # Create the hosts files to be copied afterwards in the /etc/hosts of each node
    fileContent = "127.0.0.1	localhost.localdomain	localhost\n"
    fileContent += cluster_dict[getClusterName(master)] + getMachineNum(master) + "    master\n"
    for s in slaves:
        fileContent += cluster_dict[getClusterName(s)] + getMachineNum(s) + "    slave" + getMachineNum(s) + "\n"

    fileContent += '''# The following lines are desirable for IPv6 capable hosts
::1     ip6-localhost ip6-loopback
fe00::0 ip6-localnet
ff00::0 ip6-mcastprefix
ff02::1 ip6-allnodes
ff02::2 ip6-allrouters
ff02::3 ip6-allhosts
'''


    # Write the IP addresses in the hosts.tmp file
    with open("hosts.tmp", "w") as wf:
        wf.write(fileContent)


    # Write the master and slaves
    slavesFileContent = "master\n" + "".join(list(map(lambda x: "slave" + getMachineNum(x) + "\n", slaves)))
    with open("slaves", "w") as wf:
        wf.write(slavesFileContent)

    # Write the IP addresse
    with open("ip_addresses", "w") as wf:
        for s in slaves:
            wf.write(cluster_dict[getClusterName(s)]+getMachineNum(s)+'\n')
        #wf.write("127.0.0.1\n")
    





if __name__=="__main__":
    main()
    