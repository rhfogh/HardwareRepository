import time
from PyQt4.QtGui import QImage, QPixmap
import gevent

from GenericVideoDevice import GenericVideoDevice
import logging
import numpy as np
import traceback
import redis
from scipy.misc import imsave

class Qt4_RedisCamera(GenericVideoDevice):

    def __init__(self, name):
        GenericVideoDevice.__init__(self, name)
  
        self.qimage = None
        self.Contrast = None
        self.Gamma = None
        self.Brightness = None
        
        self.log = logging.getLogger('HWR')
        self.redis = redis.StrictRedis()
        
    def init(self):
        self.image_dimensions = [1360, 1024]
        self.shape = tuple(self.image_dimensions + [3])
        self.poll_interval = self.getProperty("poll_interval", 0.02)
        self.qimage = QImage(np.zeros((self.image_dimensions[1], self.image_dimensions[0], 3)).data, self.image_dimensions[0], 3, QImage.Format_RGB888)
        
        GenericVideoDevice.init(self)
 

    def do_image_polling(self, sleep_time=None):
        
        last_image_id = None

        while True:
            new_image_id = self.get_last_image_id()
            if last_image_id != new_image_id:
                img = self.get_last_image()
                self.qimage = QImage(img,
                                    self.image_dimensions[0],
                                    self.image_dimensions[1],
                                    QImage.Format_RGB888)
                self.emit("imageReceived", QPixmap(self.qimage))
                last_image_id = new_image_id
            gevent.sleep(self.poll_interval)
            

    def get_last_image_id(self):
        return self.redis.get('last_image_id')
    
    
    def get_last_image(self):
        last_image_data = self.redis.get('last_image_data')
        img = np.ndarray(buffer=last_image_data, dtype=np.uint8, shape=self.shape)
        return img
    
    
    def get_new_image(self):
        return self.qimage

    
    def get_video_live(self):
        return True
 
    
    def start_camera(self):
        pass

    
    def change_owner(self):
        pass
    
    
    def get_contrast(self):
        try:
            return self.Contrast
        except:
            self.log.exception(traceback.format_exc())

    
    def set_contrast(self, contrast_value):
        try:
            self.Contrast = contrast_value
        except:
            self.log.exception(traceback.format_exc())

    
    def get_brightness(self):
        try:
            return self.Brightness
        except:
            self.log.exception(traceback.format_exc())

    def set_brightness(self, brightness_value):
        try:
            self.Brightness = brightness_value
        except:
            self.log.exception(traceback.format_exc())
  
    def get_gain(self):
        gain = float(self.redis.get('camera_gain'))
        return gain
        
   
    def set_gain(self, gain_value):
        try:
            if gain_value != None:
                self.redis.set('camera_gain', gain_value)
        except:
            self.log.exception(traceback.format_exc())


    def get_gamma(self):
        try:
            return self.Gamma
        except:
            self.log.exception(traceback.format_exc())


    def set_gamma(self, gamma_value):
        try:
            self.Gamma = gamma_value
        except:
            self.log.exception(traceback.format_exc())


    def get_exposure_time(self):
        try:
            exposure_time = float(self.redis.get('camera_exposure_time'))
            return exposure_time
        except:
            self.log.exception(traceback.format_exc())
        

    def set_exposure_time(self, exposure_time_value):
        # time unit used by API is microsecond
        # while we typically operate in seconds
        self.log.info('exposure_time_value %.3f' % exposure_time_value)
        exposure_time_value = (exposure_time_value < 5  and exposure_time_value > 0.001) and exposure_time_value or exposure_time_value/1.e3
        self.log.info('after adjustment exposure_time_value %.3f' % exposure_time_value)
        try:
            self.redis.set('camera_exposure_time', exposure_time_value)
        except:
            self.log.exception(traceback.format_exc())
        
    
    def save_snapshot(self, filename, image_type):
        image = self.get_last_image()
        if image_type.lower() == filename[-len(image_type):].lower():
            imsave(filename, image)
        else:
            imsave('%s.%s' % (filename, image_type.lower()))
            
