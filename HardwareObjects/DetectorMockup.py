import logging
from AbstractDetector import AbstractDetector
from HardwareRepository.BaseHardwareObjects import HardwareObject

class DetectorMockup(AbstractDetector, HardwareObject): 
    """
    Descript. : Detector class. Contains all information about detector
                the states are 'OK', and 'BAD'
                the status is busy, exposing, ready, etc.
                the physical property is RH for pilatus, P for rayonix
    """
    def __init__(self, name): 
        """
        Descript. :
        """ 
        AbstractDetector.__init__(self)
        HardwareObject.__init__(self, name)
 
    def init(self):
        """
        Descript. :
        """
        # self.distance = 500
        self.temperature = 25
        self.humidity = 60
        self.actual_frame_rate = 40
        self.roi_modes_list = ("9M", "4M")
        self.roi_mode = 0
        self.exposure_time_limits = [1./238, 60000]
        self.status = "ready"
        self.distance_motor_hwobj = self.getObjectByRole("distance_motor")
        logging.getLogger('user_level_log').info('distance_motor_hwobj %s, motor_name %s' % (self.distance_motor_hwobj,self.distance_motor_hwobj.motor_name)) 
        
    def get_distance(self):
        return self.distance_motor_hwobj.get_position()

    def set_distance(self, position, timeout=None):
        self.distance_motor_hwobj.move(position, wait=True)

    def get_distance_limits(self):
        return [100, 1000] 

    def set_roi_mode(self, roi_mode):
        self.roi_mode = roi_mode
        self.emit('detectorModeChanged', (self.roi_mode, ))

    def has_shutterless(self):
        """Returns always True
        """
        return True

    def get_beam_centre(self):
        return 0, 0

    def update_values(self):
        self.emit('detectorModeChanged', (self.roi_mode, ))
        self.emit('temperatureChanged', (self.temperature, True))
        self.emit('humidityChanged', (self.humidity, True))
        self.emit('expTimeLimitsChanged', (self.exposure_time_limits, ))
        self.emit('frameRateChanged', self.actual_frame_rate)
        self.emit('statusChanged', (self.status, "Ready", ))
