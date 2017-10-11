from HardwareRepository.BaseHardwareObjects import HardwareObject
from HardwareRepository import HardwareRepository
import gevent
import sys

CRYO_STATUS = ["OFF", "SATURATED", "READY", "WARNING", "FROZEN" , "UNKNOWN"]

class Oxford700(HardwareObject):
    def __init__(self, name):
        HardwareObject.__init__(self, name)

        self.n2level = None
        self.temp = None
        self.temp_error = None

    def _do_polling(self):
        while True: 
            try:
                self.valueChanged()
            except:
                sys.excepthook(*sys.exc_info())
            gevent.sleep(self.getProperty("interval") or 10)

    def init(self):
        controller = HardwareRepository.HardwareRepository().getHardwareObject(self.getProperty("controller"))
        cryostat = self.getProperty("cryostat")
        self.ctrl = getattr(controller,cryostat)
        if self.ctrl is not None:
            gevent.spawn(self._do_polling)

    def valueChanged(self):
        self.emit("temperatureChanged", (self.getTemperature(),))

    def valueChanged_old(self, deviceName, values):
        n2level     = values[0]
        temp        = float(values[1])
        temp_error  = values[2]
        cryo_status = int(values[3])
        temp_evap   = values[4]
        gas_heater  = values[5]
        dry_status  = int(values[6])
        sdry_status = int(values[7])
        minlevel    = values[8]
        maxlevel    = values[9]
        version     = values[10]

        if n2level != self.n2level:
            self.n2level = n2level
            self.emit("levelChanged", (n2level, ))
        if temp != self.temp or temp_error != self.temp_error:
            self.temp = temp
            self.temp_error = temp_error
            self.emit("temperatureChanged", (temp, temp_error, ))
        if cryo_status != self.cryo_status:
            self.cryo_status = cryo_status
            self.emit("cryoStatusChanged", (CRYO_STATUS[cryo_status], ))

    def setN2Level(self, newLevel):
        pass

    def getTemperature(self):
        self.temp = self.ctrl.read()
        return self.temp
