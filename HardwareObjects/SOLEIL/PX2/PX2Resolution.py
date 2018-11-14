import logging
from Resolution import Resolution
from resolution import resolution
from beam_center import beam_center

class PX2Resolution(Resolution):
    OFF, READY, MOVING = 1, 2, 4
    TANGO_TO_MOTOR_STATE = {"ALARM": OFF,
                            "STANDBY": READY,
                            "MOVING": MOVING}
    DEFAULT_RESOLUTION = 2.
    
    def __init__(self, *args, **kwargs):
        Resolution.__init__(self, *args, **kwargs)

    
    def init(self):
        
        self.resolution_motor = resolution()
        self.beam_center = beam_center()
        
        self.currentResolution = self.get_resolution() #self.DEFAULT_RESOLUTION
        
        self.energy_channel = self.getChannelObject("energy")
        self.energy_channel.connectSignal("update", self.update_resolution)
        
        self.energy_state_channel = self.getChannelObject("energy_state")
        self.energy_state_channel.connectSignal("update", self.update_energy_state)
        
        self.detector_distance_channel = self.getChannelObject("detector_position")
        #self.energy_channel.connectSignal("valueChanged", self.update_resolution)
        self.detector_distance_channel.connectSignal("update", self.update_resolution)
        
        self.detector_position_state_channel = self.getChannelObject("detector_position_state")
        self.detector_position_state_channel.connectSignal("update", self.update_detector_position_state)
        
        self.energy = self.getObjectByRole("energy")
        self.dtox = self.getObjectByRole("detector_distance")
        
        self.det_radius = self.getProperty("detector_radius")
        self.det_width = self.getProperty("detector_width")
        self.det_height = self.getProperty("detector_height")
        
        self.connect(self.dtox, "stateChanged", self.dtoxStateChanged)
        self.connect(self.dtox, "positionChanged", self.dtoxPositionChanged) 
        self.connect(self.energy, "valueChanged", self.energyChanged)
    
    
    def connectNotify(self, signal):
        if signal == "stateChanged":
            self.dtoxStateChanged(self.get_state())
    
    
    def move(self, resolution):
        self.resolution_motor.set_resolution(resolution)
    
    
    def get_state(self):
        state = self.detector_position_state_channel.getValue()
        if state != None and str(state) in self.TANGO_TO_MOTOR_STATE:
            state = self.TANGO_TO_MOTOR_STATE[state.name]
        else:
            state = self.OFF
        return state
    
    
    def get_beam_centre(self, dtox=None):
        return self.beam_center.get_beam_center()
    
    
    def get_limits(self):
        return self.resolution_motor.get_resolution_limits()
        
    
    def dtoxStateChanged(self, state=None):
        self.update_detector_position_state(state)
        
    
    def update_detector_position_state(self, state=None):
        if state != None and str(state) in self.TANGO_TO_MOTOR_STATE:
            state = self.TANGO_TO_MOTOR_STATE[state.name]
        elif type(state) == int:
            pass
        else:
            state = self.OFF
        self.emit("stateChanged", state)
        
    
    def update_energy_state(self, state=None):
        if state != None and str(state) in self.TANGO_TO_MOTOR_STATE:
            state = self.TANGO_TO_MOTOR_STATE[state.name]
        elif type(state) == int:
            pass
        else:
            state = self.OFF
        self.emit("stateChanged", state)
        
    
    def update_resolution(self, values=None):
        self.currentResolution = self.get_resolution()
        self.emit("positionChanged", self.currentResolution)
        self.emit("valueChanged", self.currentResolution)
        
    
    def stop(self):
        self.resolution_motor.stop()
        
    
    def get_detector_distance(self):
        return self.dtox.get_position()
    
    
    def get_resolution(self):
        resolution = self.resolution_motor.get_resolution()
        return resolution
    
    
