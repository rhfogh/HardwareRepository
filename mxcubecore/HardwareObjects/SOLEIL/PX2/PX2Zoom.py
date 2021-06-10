import logging
from camera import camera
import math
import gevent
from mxcubecore.HardwareObjects.mockup.MicrodiffZoomMockup import MicrodiffZoomMockup
from mxcubecore.HardwareObjects.SOLEIL.SOLEILMicrodiffMotor import SOLEILMicrodiffMotor

class PX2Zoom(SOLEILMicrodiffMotor, MicrodiffZoomMockup):
    
    def __init__(self, name):
        SOLEILMicrodiffMotor.__init__(self, name)
        MicrodiffZoomMockup.__init__(self, name)
        self.camera = camera(use_redis=True)
    
    def init(self):
        SOLEILMicrodiffMotor.init(self)
        MicrodiffZoomMockup.init(self)
        
    def get_limits(self):
        return (1, 10)
    
    def _set_value(self, value):
        """Overrriden from AbstractActuator"""
        gevent.spawn(self._set_zoom, value)
        
    def _set_zoom(self, value):
        self._nominal_value = value
        self.camera.set_zoom(value.value)
        
    def get_value(self):
        return int(self.camera.get_zoom())
