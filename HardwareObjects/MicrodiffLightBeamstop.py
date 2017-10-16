from MicrodiffInOut import MicrodiffInOut

class MicrodiffLightBeamstop(MicrodiffInOut):
    def __init__(self, name):
        MicrodiffInOut.__init__(self, name)

    def init(self):
        
        MicrodiffInOut.init(self)
        self.beamstop = self.getObjectByRole("beamstop")
        try:
            self.safety_position = float(self.getProperty("safety_position"))
        except TypeError:
            self.safety_position = 37

    def actuatorIn(self, wait=True, timeout=None):
        if self.beamstop.getPosition() < self.safety_position:
            self.beamstop.move(self.safety_position, wait=True)
        MicrodiffInOut.actuatorIn(self, wait=True, timeout=None)
