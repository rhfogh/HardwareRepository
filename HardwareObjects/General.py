#! /usr/bin/env python
# encoding: utf-8#
#  Project: MXCuBE
#  https://github.com/mxcube.
#
#  This file is part of MXCuBE software.

"""General data and functions, that can be shared between different HArdwareObjects
"""

from __future__ import division, absolute_import
from __future__ import print_function, unicode_literals
__author__ = "rhfogh"
__date__ = "19/06/17"


import enum

@enum.unique
class States(enum.Enum):
    """Standard device states, based on TangoShutter states.
    SardanaMotor.state_map, and DiffractometerState,
    for general use across HardwareObjects.

    Limited to a common set of states.

    Grammar of tags corrected ('CLOSE->CLOSED, DISABLE->DISABLED) relative
    to Tango states, so you can echo them to the UI without misunderstanding"""

    CLOSED = 0
    OPEN = 1  # Also used for workflow 'Expecting input'
    ON = 2  # Could be used to mean 'Connected'.
    OFF = 3  # Could be used to mean 'Disconnected'
    INSERT = 4
    EXTRACT = 5
    MOVING = 6
    STANDBY = 7  # Could be used to mean 'Ready'
    FAULT = 8
    INIT = 9
    RUNNING = 10
    ALARM = 11
    DISABLED = 12
    UNKNOWN = 13
