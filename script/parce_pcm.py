import sys
import pprint
# configs
SOCKET = ["0"]

data = {}
for key in ["total", "miss", "hit", "rate"]:
    data[key] = {}
    data[key]['Skt'] = 0
    data[key]['PCIRdCur'] = 0
    data[key]['RFO'] = 0
    data[key]['CRd'] = 0
    data[key]['DRd'] = 0
    data[key]['ItoM'] = 0
    data[key]['PRd'] = 0
    data[key]['WiL'] = 0
    data[key]['PCIeRd'] = 0
    data[key]['PCIeWr'] = 0

if __name__ == '__main__':
    file = sys.argv[1]
    print("Opening {}".format(file))

    with open(file) as f:
        for line in f:
            if line[0] not in SOCKET:
                continue
            info = "unknown"
            if "(Total)" in line:
                info = "total"
            if "(Miss)" in line:
                info = "miss"
            if "(Hit)" in line:
                info = "hit"
            line = line.replace("(Total)", "")
            line = line.replace("(Miss)", "")
            line = line.replace("(Hit)", "")
            Skt, PCIRdCur, RFO, CRd, DRd, ItoM, PRd, WiL, PCIeRd, PCIeWr = line.split(',')
            data[info]['Skt'] += int(Skt)
            data[info]['PCIRdCur'] += int(PCIRdCur)
            data[info]['RFO'] += int(RFO)
            data[info]['CRd'] += int(CRd)
            data[info]['DRd'] += int(DRd)
            data[info]['ItoM'] += int(ItoM)
            data[info]['PRd'] += int(PRd)
            data[info]['WiL'] += int(WiL)
            data[info]['PCIeRd'] += int(PCIeRd)
            data[info]['PCIeWr'] += int(PCIeWr)
    

        for field in data["rate"]:
            if data["total"][field] != 0:
                data["rate"][field] = 100.0 * data["hit"][field] / data["total"][field]
    
    pprint.pprint(data)


