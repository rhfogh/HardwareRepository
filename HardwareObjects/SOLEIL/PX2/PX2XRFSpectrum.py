import os
import time
import logging

from XRFSpectrumMockup import XRFSpectrumMockup
from fluorescence_spectrum import fluorescence_spectrum


class PX2XRFSpectrum(XRFSpectrumMockup):

    def start_spectrum(self, ct, spectrum_directory, archive_directory, prefix,
            session_id=None, blsample_id=None, adjust_transmission=True):
        """
        Descript. :
        """
        
        if not self.can_spectrum:
            self.spectrum_command_aborted()
            return False
        
        self.spectrum_info = {"sessionId": session_id, "blSampleId": blsample_id}
        if not os.path.isdir(archive_directory):
            logging.getLogger().debug("XRFSpectrum: creating directory %s" % archive_directory)
            try:
                if not os.path.exists(archive_directory):
                    os.makedirs(archive_directory)
                if not os.path.exists(spectrum_directory):
                    os.makedirs(spectrum_directory)
            except OSError, diag:
                logging.getLogger().error(\
                    "XRFSpectrum: error creating directory %s (%s)" % \
                    (archive_directory, str(diag)))
                self.emit('xrfSpectrumStatusChanged', ("Error creating directory", ))
                self.spectrum_command_aborted()
                return False
        archive_file_template = os.path.join(archive_directory, prefix)
        spectrum_file_template = os.path.join(spectrum_directory, prefix)
        if os.path.exists(archive_file_template + ".dat"):
            i = 1
            while os.path.exists(archive_file_template + "%d.dat" %i):
                  i = i + 1
            archive_file_template += "_%d" % i
            spectrum_file_template += "_%d" % i
            prefix += "_%d" % i

        spectrum_file_dat_filename = os.path.extsep.join((spectrum_file_template, "dat"))
        archive_file_dat_filename = os.path.extsep.join((archive_file_template, "dat"))
        archive_file_png_filename = os.path.extsep.join((archive_file_template, "png"))
        archive_file_html_filename = os.path.extsep.join((archive_file_template, "html"))

        self.spectrum_info["filename"] = prefix
        self.spectrum_info["workingDirectory"] = archive_directory
        self.spectrum_info["scanFilePath"] = spectrum_file_dat_filename
        self.spectrum_info["scanFileFullPath"] = archive_file_dat_filename
        self.spectrum_info["jpegScanFileFullPath"] = archive_file_png_filename
        self.spectrum_info["exposureTime"] = ct
        self.spectrum_info["annotatedPymcaXfeSpectrum"] = archive_file_html_filename
        self.spectrum_info["htmldir"] = archive_directory
        self.spectrum_command_started()
        logging.getLogger().debug("XRFSpectrum: spectrum dat file is %s", spectrum_file_dat_filename)
        logging.getLogger().debug("XRFSpectrum: archive file is %s", archive_file_dat_filename)

        self.experiment = fluorescence_spectrum(prefix, archive_directory, integration_time=ct)
        
        self.experiment.execute()
        
        self.spectrum_data = self.experiment.spectrum
        self.mca_calib = self.experiment.detector.get_calibration()
        
        self.specturm_command_finished()
    
    def startXrfSpectrum(self, ct, spectrum_directory, archive_directory, prefix,
            session_id=None, blsample_id=None, adjust_transmission=True):
        
        self.start_spectrum(ct, spectrum_directory, archive_directory, prefix,
            session_id=None, blsample_id=None, adjust_transmission=True)