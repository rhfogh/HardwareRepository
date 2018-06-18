import time
from energy_scan import energy_scan
from EnergyScanMockup import EnergyScanMockup

class PX2EnergyScan(EnergyScanMockup):
    
    def startEnergyScan(self, element, edge, directory, prefix,
                        session_id=None, blsample_id=None, exptime=0.64):
        
        if self._egyscan_task and not self._egyscan_task.ready():
            raise RuntimeError("Scan already started.")

        self.emit('energyScanStarted', ())
        
        self.scan_info = {"sessionId": session_id, "blSampleId": blsample_id,
                         "element": element, "edgeEnergy" : edge}
        self.scan_data = []
        self.scan_directory = directory
        self.scan_prefix = prefix
        self.startup_done = True
        self.scan_info['exposureTime'] = exptime
        self.scan_info['startEnergy'] = 0
        self.scan_info['endEnergy'] = 0
        size_hor, size_ver = 0.01, 0.005
        if self.beam_info_hwobj is not None:
            size_hor, size_ver = self.beam_info_hwobj.get_beam_size()
        self.scan_info['beamSizeHorizontal'] = size_hor
        self.scan_info['beamSizeVertical'] = size_ver    
        self.scan_info['startTime'] = str(time.strftime("%Y-%m-%d %H:%M:%S"))
        
        self.experiment = energy_scan(prefix, directory, element, edge)
        self.scanCommandStarted()
        self.experiment.execute()
        
    def cancelEnergyScan(self):
        self.experiment.stop()
        
    def scanCommandFailed(self, *args):
        with cleanup(self.ready_event.set):
            #error_msg = self.chan_scan_error.getValue()
            #print error_msg
            #logging.getLogger("GUI").error("Energy scan: %s" % error_msg)
            self.scan_info['endTime'] = str(time.strftime("%Y-%m-%d %H:%M:%S"))
            self.scan_info["startEnergy"] = self.experiment.start_energy/1.e3
            self.scan_info["endEnergy"] = self.experiment.end_energy/1.e3
            self.emit('energyScanFailed', ())
            self.emit("progressStop", ())
            self.scanning = False
            self.ready_event.set()    
    
    def get_title(self):
        if self.scan_info["blSampleId"]:
            title = "Sample: %s Element: %s Edge: %s" % \
             (self.scan_info["blSampleId"],
              self.scan_info["element"],
              self.scan_info["edgeEnergy"])
        else:
            title = "Element: %s Edge: %s" % (self.scan_info["element"],
                                              self.scan_info["edgeEnergy"])
        return title
    
    def scanCommandStarted(self, *args):
        title = self.get_title()

        graph_info = {'xlabel': 'energy',
                      'ylabel':  'counts',
                      'scaletype': 'normal',
                      'title': title}
        self.scanning = True
        self.emit('energyScanStarted', graph_info)
        
    def scanCommandAborted(self, *args):
        self.emit('energyScanFailed', ())
        self.emit("progressStop", ())
        if hasattr(self.energy_hwobj, "set_break_bragg"):
            self.energy_hwobj.set_break_bragg()
        self.scanning = False
        self.ready_event.set()
    
    def scanCommandFinished(self, *args):
        with cleanup(self.ready_event.set):
            self.scan_info['endTime'] = time.strftime("%Y-%m-%d %H:%M:%S")
            logging.getLogger("HWR").debug("Energy scan: finished")
            self.scanning = False
            self.scan_info["startEnergy"] = self.experiment.start_energy/1.e3
            self.scan_info["endEnergy"] = self.experiment.end_energy/1.e3
            self.emit('energyScanFinished', (self.scan_info,))
            self.emit("progressStop", ())
               
    def scan_status_changed(self, status):
        self.emit('energyScanStatusChanged', (status,))

    def get_comment(self):
        return ""
    
    def doChooch(self, element, edge, directory, archive_directory, prefix, rm_offset=0.03):
        self.experiment.analyze()
        pk = self.experiment.pk
        fppPeak = self.experiment.fppPeak
        fpPeak = self.experiment.fpPeak
        ip = self.experiment.ip
        fppInfl = self.experiment.fppInfl
        fpInfl = self.experiment.fpInfl
        rm = pk + rm_offset
        chooch_graph_x = self.experiment.efs[:, 0]
        chooch_graph_y1 = self.experiment.efs[:, 1] 
        chooch_graph_y2 = self.experiment.efs[:, 2]
        title = self.get_title()
        comment = self.get_comment()
        self.scan_info["peakEnergy"] = pk
        self.scan_info["inflectionEnergy"] = ip
        self.scan_info["remoteEnergy"] = rm
        self.scan_info["peakFPrime"] = fpPeak
        self.scan_info["peakFDoublePrime"] = fppPeak
        self.scan_info["inflectionFPrime"] = fpInfl
        self.scan_info["inflectionFDoublePrime"] = fppInfl
        self.scan_info["comments"] = self.get_comment()
        self.scan_info["choochFileFullPath"] = self.experiment.get_efs_filename()
        self.scan_info["filename"] = self.experiment.get_raw_filename()
        self.scan_info["workingDirectory"] = archive_directory
        return pk, fppPeak, fpPeak, ip, fppInfl, fpInfl, rm, chooch_graph_x, chooch_graph_y1, chooch_graph_y2, title
             

    def get_scan_data(self):
        """Returns energy scan data.
           List contains tuples of (energy, counts)
        """
        self.scan_data = [tuple(self.experiment.energies), tuple(self.experiment.counts)]
        return self.scan_data
   
    def scan_status_changed(self, status):
        self.emit('energyScanStatusChanged', (status,))
