import gevent
import atexit
from pymba import *
from PyQt4.QtGui import QImage, QPixmap

from GenericVideoDevice import GenericVideoDevice
import logging
import cv2
import numpy as np

class Qt4_VimbaVideo(GenericVideoDevice):

    def __init__(self, name):
        GenericVideoDevice.__init__(self,name)
  
        self.camera = None
        self.camera_id = str
        self.qimage = None

    def init(self):
        # start Vimba
        self.camera_id = u"%s" % self.getProperty("camera_id")
        self.poll_interval = self.getProperty("poll_interval")
        self.pixel_format = self.getProperty("pixel_format", "RGB8Packed")
        self.image_dimensions = [1360, 1024]
        self.log = logging.getLogger('HWR')
        atexit.register(self.close_camera)
        self.qimage = QImage(np.zeros((self.image_dimensions[1], self.image_dimensions[0], 3)).data, self.image_dimensions[0], 3, QImage.Format_RGB888)
        
        GenericVideoDevice.init(self)
 
    def do_image_polling(self, sleep_time):
        with Vimba() as vimba:
            system = vimba.getSystem()
            vimba.startup() 
            if system.GeVTLIsPresent:
                system.runFeatureCommand("GeVDiscoveryAllOnce")
                gevent.sleep(sleep_time)
            cameraIds = vimba.getCameraIds()
 
            self.camera = vimba.getCamera(self.camera_id)
            self.camera.openCamera()
            self.camera.PixelFormat = self.pixel_format
            self.frame0 = self.camera.getFrame()    # creates a frame
            self.frame0.announceFrame()
            self.log.info('Qt4_VimbaVideo: camera.startCapture()')
            self.camera.startCapture()
 
            self.image_dimensions = (self.frame0.width,
                                     self.frame0.height)
            
            self.camera.runFeatureCommand("AcquisitionStart")
            while True:
                self.frame0.waitFrameCapture(self.poll_interval)
                try:
                    self.frame0.queueFrameCapture()
                except:
                    self.log.info('Qt4_VimbaVideo: frame dropped')
                    continue
                data = self.frame0.getBufferByteData()
                img = np.ndarray(buffer=data, dtype=np.int8, shape=(self.frame0.height, self.frame0.width, self.frame0.pixel_bytes))
                self.qimage = QImage(img,
                                     self.image_dimensions[0],
                                     self.image_dimensions[1],
                                     QImage.Format_RGB888)
                if self.cam_mirror is not None:
                    self.qimage = self.qimage.mirrored(self.cam_mirror[0],
                                                       self.cam_mirror[1])

                self.emit("imageReceived", QPixmap(self.qimage))
                gevent.sleep(sleep_time)

    def get_new_image(self):
        return self.qimage

    def get_video_live(self):
        return True
 
    def close_camera(self):
        with Vimba() as vimba:
            self.camera.flushCaptureQueue()
            self.camera.endCapture()
            self.camera.revokeAllFrames()
            vimba.shutdown()

    def start_camera(self):
        pass

    def change_owner(self):
        pass

def test():
    import numpy as np
    import time
 
    with Vimba() as vimba:
        vimba.startup()
        system = vimba.getSystem()
        system.runFeatureCommand("GeVDiscoveryAllOnce")
        gevent.sleep(2)
        cameraIds = vimba.getCameraIds()
        print 'cameraIds', cameraIds
        camera = vimba.getCamera(cameraIds[0])
        camera.openCamera()
        print 'acquistionMode', camera.AcquisitionMode
             

if __name__ == '__main__':
    test()
