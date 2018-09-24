import logging
import numpy
import os.path

from XRFSpectrum import *

try:
    from PyMca import ConfigDict
    from PyMca import ClassMcaTheory
    from PyMca import QtMcaAdvancedFitReport
except ImportError:
    from PyMca5.PyMca import ConfigDict
    from PyMca5.PyMca import ClassMcaTheory
    from PyMca5.PyMca import QtMcaAdvancedFitReport


class ID29XRFSpectrum(XRFSpectrum):
    def __init__(self, *args, **kwargs):
        XRFSpectrum.__init__(self, *args, **kwargs)
        self.mca_hwobj = self.getObjectByRole('mca')
        self.ctrl_hwobj = self.getObjectByRole('controller')
        self.fname = "/users/blissadm/local/beamline_configuration/misc/15keV.cfg"
        self.config = ConfigDict.ConfigDict()
        self.mcafit = ClassMcaTheory.McaTheory(self.fname)

    def preset_mca(self, ctime, fname=None):
        self.mca_hwobj.set_roi(2, 15, channel=1)
        self.mca_hwobj.set_presets(erange=1, ctime=ctime, fname=fname)

    def choose_attenuation(self, ctime, fname=None):
        res = True
        if not fname:
            # fname = self.spectrumInfo['filename'].replace('.dat', '.raw')
            fname = self.spectrumInfo['filename']

        self.preset_mca(ctime, fname)

        self.ctrl_hwobj.detcover.set_in()
        try:
            tt = self.ctrl_hwobj.find_max_attenuation(ctime=ctime,
                                                      fname=fname,
                                                      roi=[2., 15.])
            self.spectrumInfo["beamTransmission"] = tt
        except Exception as e:
            logging.getLogger('user_level_log').exception(str(e))
            res = False

        return res
    """
    def _findAttenuation(self, ct):
        return self.choose_attenuation(ct)
    """

    def _findAttenuation(self, ct):
        tf = [0.1, 0.2, 0.3, 0.9, 1.3, 1.9, 2.6, 4.3, 6, 8, 12, 24, 36, 50, 71]
        min_cnt = self.getProperty("min_cnt")
        max_cnt = self.getProperty("max_cnt")
        self.mca_hwobj.set_roi(2, 15, channel=1)
        print self.spectrumInfo["filename"]
        self.mca_hwobj.set_presets(erange=1, ctime=ct, fname=self.spectrumInfo["filename"])

        # put in max attenuation
        self.transmission_hwobj.setTransmission(0)
        
        self.ctrl_hwobj.diffractometer.msopen()
        self.mca_hwobj.start_acq()
        time.sleep(ct)
        ic = sum(self.mca_hwobj.read_roi_data())/ct
        print ic
        if ic > max_cnt:
            self.ctrl_hwobj.diffractometer.msclose()
            logging.getLogger("user_level_log").exception('The detector is saturated, giving up.')
            return False
        for i in tf:
            self.mca_hwobj.clear_spectrum()
            logging.getLogger("user_level_log").info("Setting transmission to %g"% i)
            self.transmission_hwobj.setTransmission(i)
            self.mca_hwobj.start_acq()
            time.sleep(ct)
            ic = sum(self.mca_hwobj.read_roi_data())/ct
            print ic
            if ic >  min_cnt:
                self.ctrl_hwobj.diffractometer.msclose()
                self.spectrumInfo["beamTransmission"] =  self.transmission_hwobj.get_value()
                logging.getLogger("user_level_log").info("Transmission used for spectra: %g"% self.spectrumInfo["beamTransmission"])
                break
        self.ctrl_hwobj.diffractometer.msclose()
        if ic < min_cnt:
            logging.getLogger("user_level_log").exception('Could not find satisfactory attenuation (is the mca properly set up?), giving up.')
            return False
        return True

    """
    Next methods are for fitting the data with pymca
    """
    """
    The configuration is time consuming. It is only executed if the last
    configuration file is not the same.
    """
    def mcafit_configuration(self, config={}):
        change = False
        if 'file' not in config:
            fname = XRFSpectrum._get_cfgfile(self, self.spectrumInfo["energy"])
        else:
            fname = config['file']

        if self.fname != fname:
            self.fname = fname
            change = True
        self.config.read(self.fname)
        if 'concentrations' not in self.config:
            self.config['concentrations'] = {}
            change = True
        if 'attenuators' not in self.config:
            self.config['attenuators'] = {'Matrix':
                                          [1, 'Water', 1.0, 0.01, 45.0, 45.0]}
            change = True
        if 'flux' in config:
            self.config['concentrations']['flux'] = float(config['flux'])
            change = True
        if 'time' in config:
            self.config['concentrations']['time'] = float(config['time'])
            change = True

        if change:
            self.mcafit.configure(self.config)

    def set_data(self, data, calib=None, config={}):
        if config:
            self.mcafit_configuration(config)
        try:
            if data[0].size == 2:
                x = numpy.array(data[:, 0]) * 1.0
                y = numpy.array(data[:, 1])
            else:
                x = data[0] * 1.0
                y = data[1]

            xmin = float(config["min"])
            xmax = float(config["max"])

            # calib = numpy.ravel(calib).tolist()
            self.mcafit.setData(x, y, xmin=xmin, xmax=xmax)

            self.mcafit.estimate()
            # fitresult  = self._fit()

            fitresult = self.mcafit.startfit(digest=1)
            if fitresult:
                fitresult = {'fitresult': fitresult[0], 'result': fitresult[1]}

                # write the csv file to pyarch
                csvname = self.spectrumInfo['fittedDataFileFullPath']
                self._write_csv_file(fitresult, csvname)

                # write html report to pyarch
                fn = os.path.basename(self.spectrumInfo['filename'])
                outfile = fn.split(".")[0]
                outdir = os.path.dirname(
                    self.spectrumInfo['annotatedPymcaXfeSpectrum'])

                kw = {'outdir': outdir, 'outfile': outfile,
                      'fitresult': fitresult, 'plotdict': {'logy': False}}
                report = QtMcaAdvancedFitReport.QtMcaAdvancedFitReport(**kw)
                text = report.getText()
                report.writeReport(text=text)
        except Exception as e:
            logging.getLogger().exception('XRFSpectrum: problem fitting %s'
                                          % str(e))
            raise

    def _write_csv_file(self, fitresult, fname=None):
        if not fname:
            fname = self.spectrumInfo['fittedDataFileFullPath']
        if os.path.exists(fname):
            os.remove(fname)

        # get the significant peaks
        peaks_dict = {}
        pars_len = len(fitresult['result']['parameters'])
        grp_len = len(fitresult['result']['groups'])
        nglobal = pars_len - grp_len
        parameters = fitresult['result']['fittedpar'][:nglobal] + [0.0]*grp_len

        for grp in fitresult['result']['parameters'][nglobal:]:
            idx = fitresult['result']['parameters'].index(grp)
            parameters[idx] = fitresult['result']['fittedpar'][idx]
            xmatrix = fitresult['result']['xdata']
            ymatrix = self.mcafit.mcatheory(parameters, xmatrix)
            ymatrix.shape = [len(ymatrix), 1]
            label = 'y' + grp
            if self.mcafit.STRIP:
                peaks_dict[label] = ymatrix + self.mcafit.zz
            else:
                peaks_dict[label] = ymatrix
            peaks_dict[label].shape = (len(peaks_dict[label]),)
            parameters[idx] = 0.0

        delimiter = ","
        header = '"channel"%s"Energy"%s"counts"%s"fit"%s"continuum"%s"pileup"'\
                 % (delimiter, delimiter, delimiter, delimiter, delimiter)

        # add the peaks labels
        for key in peaks_dict.iterkeys():
            header += delimiter + ('"%s"' % key)

        logging.getLogger('user_level_log').info("Writing %s" % fname)
        with open(fname, 'w') as csv_fd:
            csv_fd.write(header)
            csv_fd.write("\n")
            for i in range(fitresult['result']['xdata'].size):
                csv_fd.write("%.7g%s%.7g%s%.7g%s%.7g%s%.7g%s%.7g" %
                             (fitresult['result']['xdata'][i],
                              delimiter,
                              fitresult['result']['energy'][i],
                              delimiter,
                              fitresult['result']['ydata'][i],
                              delimiter,
                              fitresult['result']['yfit'][i],
                              delimiter,
                              fitresult['result']['continuum'][i],
                              delimiter,
                              fitresult['result']['pileup'][i]))
                for key in peaks_dict.iterkeys():
                    csv_fd.write("%s%.7g" % (delimiter, peaks_dict[key][i]))
                csv_fd.write("\n")
