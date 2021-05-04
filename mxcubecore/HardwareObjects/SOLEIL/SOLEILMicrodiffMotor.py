from MicrodiffMotor import MicrodiffMotor
import logging
from gevent import spawn
from goniometer import goniometer

class SOLEILMicrodiffMotor(MicrodiffMotor):
    
    def __init__(self, name):
        MicrodiffMotor.__init__(self, name)
        self.goniometer = goniometer()
        
    def init(self):
        self.position = None
        # assign value to actuator_name
        self.actuator_name = self.get_property("actuator_name", "")

        self.GUIstep = self.get_property("GUIstep")

        self.motor_resolution = self.get_property("resolution")
        if self.motor_resolution is None:
            self.motor_resolution = 0.0001

        self.motorState = MicrodiffMotor.NOTINITIALIZED

        self.position_attr = self.get_channel_object(
            "%s%s" % (self.actuator_name, self.motor_pos_attr_suffix)
        )
        if not self.position_attr:
            self.position_attr = self.add_channel(
                {"type": "exporter", "name": "%sPosition" % self.actuator_name},
                self.actuator_name + self.motor_pos_attr_suffix,
            )

        if self.position_attr is not None:
            self.state_attr = self.get_channel_object(
                "%s%s" % (self.actuator_name, self.motor_state_attr_suffix)
            )
            if not self.state_attr:
                self.state_attr = self.add_channel(
                    {"type": "exporter", "name": "%sState" % self.actuator_name},
                    self.actuator_name + self.motor_state_attr_suffix,
                )

            self.position_attr.connect_signal("update", self.motor_positions_changed)
            self.state_attr.connect_signal("update", self.motorStateChanged)

            self._motor_abort = self.get_command_object("abort")
            if not self._motor_abort:
                self._motor_abort = self.add_command(
                    {"type": "exporter", "name": "abort"}, "abort"
                )

            self.get_dynamic_limits_cmd = self.get_command_object(
                "get%sDynamicLimits" % self.actuator_name
            )
            if not self.get_dynamic_limits_cmd:
                self.get_dynamic_limits_cmd = self.add_command(
                    {
                        "type": "exporter",
                        "name": "get%sDynamicLimits" % self.actuator_name,
                    },
                    "getMotorDynamicLimits",
                )

            self.get_limits_cmd = self.get_command_object("getMotorLimits")
            if not self.get_limits_cmd:
                self.get_limits_cmd = self.add_command(
                    {"type": "exporter", "name": "get_limits"}, "getMotorLimits"
                )

            self.get_max_speed_cmd = self.get_command_object("getMotorMaxSpeed")
            if not self.get_max_speed_cmd:
                self.get_max_speed_cmd = self.add_command(
                    {"type": "exporter", "name": "get_max_speed"}, "getMotorMaxSpeed"
                )

            self.home_cmd = self.get_command_object("homing")
            if not self.home_cmd:
                self.home_cmd = self.add_command(
                    {"type": "exporter", "name": "homing"}, "startHomingMotor"
                )

        self.motor_positions_changed(self.position_attr.get_value())
        
    def move(self, position, wait=True, timeout=None):
        if abs(self.get_position() - position) >= self.motor_resolution:
            if hasattr(self.goniometer, 'set_%s_position' % self.motor_name.lower()):
                spawn(getattr(self.goniometer, 'set_%s_position' % self.motor_name.lower()), position)
            else:
                spawn(self.goniometer.set_position, {self.motor_name: position})

