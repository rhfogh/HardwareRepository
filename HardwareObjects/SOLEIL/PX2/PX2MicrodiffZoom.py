from MicrodiffZoom import MicrodiffZoom

import logging
from camera import camera

import gevent

class PX2MicrodiffZoom(MicrodiffZoom):
    
    def __init__(self, name):
        MicrodiffZoom.__init__(self, name)
        self.camera = camera(use_redis=True)
    
    def moveToPosition(self, positionName):
        try:
            position = self.predefinedPositions[positionName]
            gevent.spawn(self.camera.set_zoom, position)
        except:
            logging.getLogger("HWR").exception('Cannot move motor %s: invalid position name.', str(self.userName()))
