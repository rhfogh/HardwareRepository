import logging
from HardwareRepository import HardwareRepository
import BeamInfo

class ID30BBeamInfo(BeamInfo.BeamInfo):
    def __init__(self, *args):
        BeamInfo.BeamInfo.__init__(self, *args)

    def init(self):
        self.chan_beam_size_microns = None 
        self.chan_beam_shape_ellipse = None 
        BeamInfo.BeamInfo.init(self)

        self.beam_size_slits = map(float,self.getProperty("beam_size_slits").split()) #[0.1, 0.05]
        self.camera = self.getDeviceByRole("camera")

    def get_beam_position(self):
        return 321,243

    def set_beam_position(self, beam_x, beam_y):
        return

    def evaluate_beam_info(self,*args):
        BeamInfo.BeamInfo.evaluate_beam_info(self,*args)
        self.beam_info_dict["shape"] = "ellipse"
        return self.beam_info_dict
     
