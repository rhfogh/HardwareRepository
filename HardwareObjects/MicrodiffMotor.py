from gevent import Timeout, sleep, spawn
from AbstractMotor import AbstractMotor

class MD2TimeoutError(Exception):
    pass

"""
Example xml file:
<device class="MicrodiffMotor">
  <username>phiy</username>
  <exporter_address>wid30bmd2s:9001</exporter_address>
  <motor_name>AlignmentY</motor_name>
  <GUIstep>1.0</GUIstep>
  <unit>-1e-3</unit>
  <resolution>1e-2</resolution>
</device>
"""

class MicrodiffMotor(AbstractMotor):
    
    def __init__(self, name):
        AbstractMotor.__init__(self, name) 
        self.motor_pos_attr_suffix = "Position"
        self.motor_state_attr_suffix = "State"
        
        self.EXPORTER_TO_MOTOR_STATE = { "Invalid": self.motor_states.NOTINITIALIZED,
                                         "Fault": self.motor_states.FAULT,
                                         "Ready": self.motor_states.READY,
                                         "Moving": self.motor_states.MOVING,
                                         "Created": self.motor_states.NOTINITIALIZED,
                                         "Initializing": self.motor_states.NOTINITIALIZED,
                                         "Unknown": self.motor_states.UNKNOWN,
                                         "Offline": self.motor_states.OFF,
                                         "LowLim": self.motor_states.LOWLIMIT,
                                         "HighLim": self.motor_states.HIGHLIMIT}

        self.TANGO_TO_MOTOR_STATE = {"STANDBY": self.motor_states.READY,
                                     "MOVING": self.motor_states.MOVING}

        self.old_state = None
        self.state_emits = 0
        
    def init(self):
        self.position = None
        self.motorState = None
        #assign value to motor_name
        self.motor_name = self.getProperty("motor_name")
 
        self.GUIstep = self.getProperty("GUIstep")
        
        self.motor_resolution = self.getProperty("resolution")
        if self.motor_resolution is None:
           self.motor_resolution = 0.0001

        # this is ugly : I added it to make the centring procedure happy
        self.specName = self.motor_name

        self.motorState = self.motor_states.NOTINITIALIZED
        
        self.position_attr = self.getChannelObject("%s%s" % (self.motor_name, self.motor_pos_attr_suffix))

        if not self.position_attr:
            self.position_attr = self.addChannel({"type": "exporter",
                                                  "name": "%sPosition" %self.motor_name},
                                                  self.motor_name + self.motor_pos_attr_suffix)
        
        if self.position_attr is not None:
            self.state_attr = self.getChannelObject("%s%s" % (self.motor_name, self.motor_state_attr_suffix))
            if not self.state_attr:
                self.state_attr = self.addChannel({"type": "exporter",
                                                   "name": "%sState" %self.motor_name},
                                                   self.motor_name + self.motor_state_attr_suffix)
                
            self.position_attr.connectSignal("update", self.motorPositionChanged)
            self.state_attr.connectSignal("update", self.motorStateChanged)
            
            self.motors_state_attr = self.getChannelObject("motor_states")
            if not self.motors_state_attr:
                self.motors_state_attr = self.addChannel({"type": "exporter",
                                                          "name": "motor_states"},
                                                          "MotorStates")
            #self.motors_state_attr.connectSignal("update", self.updateMotorState)
            
            self._motor_abort = self.getCommandObject("abort")
            if not self._motor_abort:
                self._motor_abort = self.addCommand({"type": "exporter",
                                                     "name": "abort" },
                                                     "abort")
            
            self.get_dynamic_limits_cmd = self.getCommandObject("get%sDynamicLimits" % self.motor_name)
            if not self.get_dynamic_limits_cmd:
                self.get_dynamic_limits_cmd = self.addCommand({"type": "exporter",
                                                               "name": "get%sDynamicLimits" % self.motor_name},
                                                               "getMotorDynamicLimits")
            
            self.get_limits_cmd = self.getCommandObject("getMotorLimits")
            if not self.get_limits_cmd:
                self.get_limits_cmd = self.addCommand({"type": "exporter",
                                                       "name": "get_limits"},
                                                       "getMotorLimits")
                
            self.get_max_speed_cmd = self.getCommandObject("getMotorMaxSpeed")
            if not self.get_max_speed_cmd:
                self.get_max_speed_cmd = self.addCommand({"type": "exporter",
                                                          "name": "get_max_speed"},
                                                          "getMotorMaxSpeed")
            
            self.home_cmd = self.getCommandObject("homing")
            if not self.home_cmd:
                self.home_cmd = self.addCommand({"type": "exporter",
                                                 "name": "homing" },
                                                 "startHomingMotor")
            
        self.emit('stateChanged', (self.get_state(), ))
        self.emit('positionChanged', (self.get_position(), ))
        self.emit('limitsChanged', (self.get_limits(), ))

    def connectNotify(self, signal):
        if signal == 'positionChanged':
            self.motorPositionChanged()
        elif signal == 'stateChanged':
            self.motorStateChanged()
        elif signal == 'limitsChanged':
            self.motorLimitsChanged()  
 
    def updateState(self):
        state = self.get_state()
        self.setIsReady(self.is_ready())

    def is_ready(self):
        return self.get_state() in [self.motor_states.READY, 
                                    self.motor_states.STANDBY, 
                                    self.motor_states.LOWLIMIT,
                                    self.motor_states.HIGHLIMIT,
                                    self.motor_states.MOVING,
                                    self.motor_states.BUSY] 
    
    def setIsReady(self, value):
        if value == True:
            self.set_ready()
    
    def set_ready(self, task=None):
        """Sets motor state to ready"""
        self.set_state(self.motor_states.READY)
        
    def set_state(self, state):
        """Sets motor state

        Keyword Args:
            state (str): motor state
        """
        self.__state = state
        self.emit('stateChanged', (state, ))
        
    def updateMotorState(self, motor_states):
        d = dict([x.split("=") for x in motor_states])
        #Some are like motors but have no state
        # we set them to ready
        _motor_state = d.get(self.motor_name)
        if _motor_state is None:
            new_motor_state = self.motor_states.READY   
        else:
            if _motor_state in self.EXPORTER_TO_MOTOR_STATE:
                new_motor_state = self.EXPORTER_TO_MOTOR_STATE[_motor_state]
            else:
                new_motor_state = self.TANGO_TO_MOTOR_STATE[_motor_state]
        if self.motorState == new_motor_state:
          return
        self.motorState = new_motor_state
        self.motorStateChanged(self.motorState)
    

    def motorStateChanged(self, state=None):
        self.updateState()
        if state == None:
            state = self.state_attr.getValue()
        if type(state) is not int:
            state_int = self.translate_state(state)
        else:
            state_int = state
        if self.old_state != None and self.old_state == state_int:
            return
        self.emit('stateChanged', (state_int, ))
        self.old_state = state_int
        
    def translate_state(self, state_value):
        if state_value in self.EXPORTER_TO_MOTOR_STATE:
            state = self.EXPORTER_TO_MOTOR_STATE[state_value]
        else:
            state = self.TANGO_TO_MOTOR_STATE[state_value.name]
        return state
    
    def get_state(self):
        state_value = self._get_state()
        state = self.translate_state(state_value)
        return state
    
    def _get_state(self):
        state_value = self.state_attr.getValue()
        return state_value
        
    def motorLimitsChanged(self):
        self.emit('limitsChanged', (self.get_limits(), ))
                     
    def get_limits(self):
        dynamic_limits = self.getDynamicLimits()
        if dynamic_limits != (-1E4, 1E4):
            return dynamic_limits
        else: 
            try:
              low_lim,hi_lim = map(float, self.get_limits_cmd(self.motor_name))
              if low_lim==float(1E999) or hi_lim==float(1E999):
                  raise ValueError
              return low_lim, hi_lim
            except:
              return (-1E4, 1E4)

    def getDynamicLimits(self):
        try:
          low_lim,hi_lim = map(float, self.get_dynamic_limits_cmd(self.motor_name))
          if low_lim==float(1E999) or hi_lim==float(1E999):
            raise ValueError
          return low_lim, hi_lim
        except:
          return (-1E4, 1E4)

    def getMaxSpeed(self):
        return self.get_max_speed_cmd(self.motor_name)

    def motorPositionChanged(self, absolute_position=None, private={}):
        if absolute_position == None:
            absolute_position = self.position_attr.getValue()
        if not None in (absolute_position, self.position):
            if abs(absolute_position - self.position) <= self.motor_resolution:
                return
        self.position = absolute_position
        self.emit('positionChanged', (self.position, ))

    def get_position(self):
        if self.position_attr is not None:   
            try:
                self.position = self.position_attr.getValue()
            except:
                pass
        return self.position
 
    def getDialPosition(self):
        return self.get_position()

    def move(self, absolutePosition, wait=True, timeout=None):
        if abs(self.get_position() - absolutePosition) >= self.motor_resolution:
            spawn(self.position_attr.setValue(absolutePosition))

    def moveRelative(self, relativePosition):
        self.move(self.get_position() + relativePosition)

    def syncMoveRelative(self, relative_position, timeout=None):
        return self.syncMove(self.get_position() + relative_position)

    def waitEndOfMove(self, timeout=None):
        with Timeout(timeout):
           sleep(0.1)
           while self.get_state() == self.motor_states.MOVING:
              sleep(0.1) 

    def syncMove(self, position, timeout=None):
        self.move(position)

    def motorIsMoving(self):
        return self.isReady() and self.get_state() == self.motor_states.MOVING 
 
    def getMotorMnemonic(self):
        return self.motor_name

    def stop(self):
        self.demand_move = False
        if self.get_state() != self.motor_states.NOTINITIALIZED and self.get_limits()[-1] != 1e4:
          self._motor_abort()

    def homeMotor(self, timeout=None):
        self.home_cmd(self.motor_name)
        try:
            self.waitEndOfMove(timeout)
        except:
            raise MD2TimeoutError

    def update_values(self):
        return
        #self.emit('stateChanged', (self.get_state(), ))
        #self.emit('positionChanged', (self.get_position(), ))
        #self.emit('limitsChanged', (self.get_limits(), ))
        
    def move_to_high_limit(self):
        self.demand_move = True
        last_move_time = 0
        if self.get_limits()[-1] != 1e4:
            self.move(self.get_limits()[-1])
        else:
            self.move(self.get_position() + 90.)
        
    def move_to_low_limit(self):
        self.demand_move = True
        last_move_time = 0
        if self.get_limits()[0] != -1e4:
            self.move(self.get_limits()[0])
        else:
            self.move(self.get_position() - 90.)
        
