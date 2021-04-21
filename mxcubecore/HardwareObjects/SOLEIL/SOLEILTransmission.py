#
#  Project: MXCuBE
#  https://github.com/mxcube
#
#  This file is part of MXCuBE software.
#
#  MXCuBE is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  MXCuBE is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with MXCuBE. If not, see <http://www.gnu.org/licenses/>.


import gevent
from mxcubecore.HardwareObjects.abstract.AbstractTransmission import (
    AbstractTransmission,
)

from transmission import transmission

class SOLEILTransmission(AbstractTransmission):
    
    def __init__(self, name):
        AbstractTransmission.__init__(self, name)

        self.chan_att_value = None
        self.chan_att_state = None
        self.chan_att_limits = None
        self.transmission = transmission()
        
    def init(self):
        
        self.h_gap_channel = self.get_channel_object("h_gap")
        self.h_state_channel = self.get_channel_object("h_state")
        self.v_gap_channel = self.get_channel_object("v_gap")
        self.v_state_channel = self.get_channel_object("v_state")
        
        self.h_gap_channel.connect_signal('update', self.value_changed)
        self.h_state_channel.connect_signal('update', self.state_changed)  
        self.v_gap_channel.connect_signal('update', self.value_changed)
        self.v_state_channel.connect_signal('update', self.state_changed)

        self.re_emit_values()
    
    def state_changed(self, state):
        if str(state) == 'STANDBY':
            self._state = self.STATES.READY
        else:
            self._state = self.STATES.BUSY
        self.emit("stateChanged", self._state)

    def value_changed(self, value):
        self._value = self.get_value()
        self.emit("valueChanged", self._value)

    def limits_changed(self, value):
        self._limits = value
        self.emit("limitsChanged", (self._limits,))

    def _set_value(self, value):
        self.transmission.set_transmission(value)

    def get_value(self):
        t = self.transmission.get_transmission()
        return t
    
    def is_ready(self):
        return self.get_state() in [self.STATES.STANDBY, self.STATES.READY]
        
