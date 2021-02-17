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

from scipy.interpolate import interp1d

import api

from HardwareRepository.BaseHardwareObjects import HardwareObject

__credits__ = ["MXCuBE collaboration"]
__category__ = "General"


class AbstractFlux(HardwareObject):

    # Dose rate for a standard composition crystal, in Gy/s
    # As a function of energy in keV
    dose_rate_per_photon_per_mmsq = interp1d(
        [4.0, 6.6, 9.2, 11.8, 14.4, 17.0, 19.6, 22.2, 24.8, 27.4, 30.0],
        [
            4590.0e-12,
            1620.0e-12,
            790.0e-12,
            457.0e-12,
            293.0e-12,
            202.0e-12,
            146.0e-12,
            111.0e-12,
            86.1e-12,
            68.7e-12,
            55.2e-12,
        ],
    )

    def __init__(self, name):
        HardwareObject.__init__(self, name)

        self._value = None
        self._status = None

    def set_flux(self, value):
        self._value = value
        self.emit("fluxValueChanged", self._value)

    def get_flux(self):
        return self._value

    def update_values(self):
        self.emit("fluxValueChanged", self._value)

    def get_total_absorbed_dose(self):
        # FIXME calculate correct value, or refactor away function
        # return self.get_flux()
        return 0.0

    def get_average_flux_density(self, transmission=None):
        """Get average flux density over the beam area in photons / mm^2
        for a given transmisison setting

        Args:
            transmission (float): # Target transmission in % (defaults to current value)

        Returns (photons / mm^2) : average flux density over beam area

        """

        beam_size = api.beam_info.get_beam_size()
        flux = api.flux.get_flux()
        result = None
        if flux and all(beam_size):
            result = flux / (beam_size[0] * beam_size[1])
            if transmission  is not None:
                result = result * transmission / api.transmission.get_value()
        #
        return result

