import logging
import math
import sys

class CalculateGaps:
    def __init__(self, config_file=None):
        self.GAPS = {}

    def _calc_gaps(self,energy, undulator=None,debug=0, config_file="/users/blissadm/local/beamline_control/configuration/undulators.dat"):

        self.GAPS = {}
        try:
            f = open(config_file) 
            array = []
            nb_line = 0
            for line in f:
                if not line.startswith('#') and line.strip() :
                    array.append(line.split())
                    nb_line += 1
                else:
                    pass
            f.close()
        except IOError:
            logging.exception("Cannot read undulators file")

        if nb_line == 1:
            larr = map(float,array[0][2:])
            gg = self._calc_gap(energy, larr, array[0][0], debug)
            if gg:
                self.GAPS[array[0][0]] = gg 
            else:
                self.GAPS[array[0][0]] = int(array[0][1].strip("."))
        elif nb_line > 1:
            gap = {}
            for i in array:
                larr = map(float,i[2:])
                gg = self._calc_gap(energy, larr, i[0], debug)
                if gg == 0:
                    gg = int(i[1].strip("."))
                if undulator is not None:
                   if i[0] == undulator:
                       gmax = int(i[1].strip("."))
                       if gg != 0:
                           gap[i[0]] = gg
                   else:
                       gap[i[0]] = int(i[1].strip("."))
                else:
                    gap[i[0]] = gg
            self.GAPS = gap
        else:
            logging.exception("Undulators file format error")

        return self.GAPS

    def _calc_gap(self, energy, arr, name, debug=0):
        gamma = 0.511/6040
        contst_en = 6.04
        dist = 57.8            #Calculate beam size at 57.8 m from source
        sigma_x = 0.000012
        sigma_y = 0.000006
        sigma_h = 0.406
        sigma_v = 0.01
        const=13.056*arr[1]*100/pow(contst_en,2)
        k2 = (math.pi/arr[1])/1000
        #Transform energy in wavelength
        h_over_e = 12.3984
        energy = h_over_e/energy
        target=energy/const
        nsols=0

        #make calculations for 1st to 11th harmonics
        kk = [0]*13
        gg = [0]*13
        psi = [0]*13
        kout = [0]*13
        iharm = [0]*13
        hsiz = [0]*13
        vsiz = [0]*13
        gg_final = [0]*13
        #calculate gap now ...
            
        for i in range(1,13,2):
            targ = target*i
            targ = (targ-1)*2
            if targ > 0:
                kk[i]=math.sqrt(targ)
                bo = kk[i]/(arr[1]*93.4)
                gg[i] = -1*math.log(bo/arr[2])/k2
                #gap is not quite right - correction factors
                gg[i] += arr[5]
                #Power [GeV] at 200 mA
                psi[i] = 0.633*(pow(contst_en,2))*(pow(bo,2))*0.2*arr[1]*arr[0]
                if gg[i] > arr[4]:
                    nsols += 1
                    gg_final[nsols]=gg[i]
                    psi[nsols]=psi[i]
                    kout[nsols]=kk[i]
                    iharm[nsols]=i
                    consta = 1+pow(kk[i],2)/2
                    constb = consta/(2*i*arr[0])
                    sigmarp=math.sqrt(constb)*gamma
                    #Answer is in rad, we want mrad
                    sigma_pv=2.35*1000*math.sqrt(pow(sigmarp,2)+pow(sigma_y,2))
                    sigma_ph=2.35*1000*math.sqrt(pow(sigmarp,2)+pow(sigma_x,2))
                    #Calculate beam sizes - these are in mcrions
                    hsiz[nsols] = math.sqrt(pow(sigma_h,2) + pow(sigma_ph*dist,2))
                    vsiz[nsols] = math.sqrt(pow(sigma_v,2) + pow(sigma_pv*dist,2))
        if nsols == 0:
            logging.info("Cannot CALCULATE GAPS")
            return 0
        iharm = [x for x in iharm if x != 0]
        gg_final = [x for x in gg_final if x != 0]
        idx = iharm.index(min(iharm))
        if debug:
            print "Gaps for %s:\n    Calculated:" % name
            for i in iharm:
                print "\t%2.3f on harmonic %d" % (gg_final[iharm.index(i)], i)
            print "    Chosen: %2.3f on harmonic %d" % (gg_final[idx],
                                                        iharm[idx])
        return gg_final[idx]

    def _calc_gaps_lt(self,energy, undulator=None,debug=0, config_file="/users/blissadm/local/beamline_control/configuration/undulators.lut"):
        self.GAPS = {}
        try:
            f = open(config_file) 
            array = []
            for line in f:
                if not line.startswith('#') and line.strip():
                    array.append(map(float,line.split()))
                else:
                    if line.startswith('#'):
                        ll = line.split()
                        try:
                            labels=[]
                            labels.append(ll[1].lower())
                            labels.append(ll[3].lower())
                        except:
                            labels = ll[1].lower()
                            
            f.close()
        except IOError:
            logging.exception("Cannot read undulators file")

        if energy < array[0][0] or energy > array[-1][0]:
            if isinstance(labels, list):
                gaps = [55, 55]
                self.GAPS=dict(zip(labels, gaps))
            else:
                self.GAPS[labels] = 55
            return self.GAPS

        gaps = self._interpol(array, energy, debug)
        try:
            self.GAPS = dict(zip(labels, gaps))
        except:
            self.GAPS[labels] = gaps

        if undulator is not None:
            for i in self.GAPS.keys():
                if i != undulator:
                   self.GAPS[i] = 50 
        return self.GAPS

    def _interpol(self, arr, val, debug=0):
        larr = []
        for i, gaps in enumerate(arr):
            if abs(gaps[0] - val)  < 0.001:
                try:
                    return [gaps[1], gaps[3]]
                except:
                    return gaps[1]
            larr.append(abs(gaps[0] - val))
            
        min_index = larr.index(min(larr))
        x1 = arr[min_index][0]
        y1 = arr[min_index][1]
        try:
            z1 = arr[min_index][3]
        except:
            pass
        if debug:
            print arr[min_index]
        if x1 > val and min_index > 0:
            min_index -= 1
        elif x1 < val and min_index < len(arr)-1:
            min_index += 1
        else:
            return None
        if debug:
            print arr[min_index]
        x2 = arr[min_index][0]
        y2 = arr[min_index][1]
        try:
            z2 = arr[min_index][3]
        except:
            pass        
        bb = (y2-y1)/(x2-x1)
        aa =  y1 - bb*x1
        try:
            dd = (z2-z1)/(x2-x1)
            cc = z1 - dd*x1
            return [aa+bb*val, cc+dd*val]
        except:
            return aa+bb*val
            
if __name__ == '__main__' :

    cg = CalculateGaps()
    #gg = cg._calc_gaps(float(sys.argv[1]), undulator="ppu35c")
    #gg = cg._calc_gaps(float(sys.argv[1]))
    #print gg
    ab = cg._calc_gaps_lt(float(sys.argv[1]), config_file="/users/blissadm/local/beamline_control/configuration/undulators.lut", debug = 0)
    #ab = cg._calc_gaps_lt(float(sys.argv[1]), config_file="/users/blissadm/local/beamline_control/configuration/undulators.lut", undulator="ppu35c", debug = 0)
    print ab

