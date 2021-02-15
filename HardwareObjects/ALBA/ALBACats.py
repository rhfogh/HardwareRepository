#
#  Project: MXCuBE
#  https://github.com/mxcube.
#
#  This file is part of MXCuBE software.
#
#  MXCuBE is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  MXCuBE is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with MXCuBE.  If not, see <http://www.gnu.org/licenses/>.

"""
[Name] ALBACats

[Description]
HwObj used to control the CATS sample changer via Tango.

[Signals]
- powerStateChanged
- runningStateChanged
"""

from __future__ import print_function

import logging
import time

from sample_changer.Cats90 import Cats90, SampleChangerState, TOOL_SPINE

__credits__ = ["ALBA Synchrotron"]
__version__ = "2.3"
__category__ = "General"

TIMEOUT = 3


class ALBACats(Cats90):
    """
    Main class used @ ALBA to integrate the CATS-IRELEC sample changer.
    """

    def __init__(self, *args):
        Cats90.__init__(self, *args)
        self.logger = logging.getLogger("HWR.ALBACats")
        self.detdist_saved = None

        self.shifts_channel = None
        self.phase_channel = None
        self.super_state_channel = None
        self.detdist_position_channel = None
        self._chnPathSafe = None

        self.go_transfer_cmd = None
        self.go_sampleview_cmd = None
        self.super_abort_cmd = None

        self._cmdLoadHT = None
        self._cmdChainedLoadHT = None
        self._cmdUnloadHT = None

        self.auto_prepare_diff = None

    def init(self):
        self.logger.debug("Initializing {0}".format(self.__class__.__name__))
        Cats90.init(self)
        # TODO: Migrate to taurus channels instead of tango channels
        self.shifts_channel = self.getChannelObject("shifts")
        self.phase_channel = self.getChannelObject("phase")
        self.super_state_channel = self.getChannelObject("super_state")
        self.detdist_position_channel = self.getChannelObject("detdist_position")
        self._chnPathSafe = self.getChannelObject("_chnPathSafe")

        self.go_transfer_cmd = self.getCommandObject("go_transfer")
        self.go_sampleview_cmd = self.getCommandObject("go_sampleview")
        self.super_abort_cmd = self.getCommandObject("super_abort")

        self._cmdLoadHT = self.getCommandObject("_cmdLoadHT")
        self._cmdChainedLoadHT = self.getCommandObject("_cmdChainedLoadHT")
        self._cmdUnloadHT = self.getCommandObject("_cmdUnloadHT")

        self.auto_prepare_diff = self.getProperty("auto_prepare_diff")

        if self._chnPathRunning is not None:
            self._chnPathRunning.connectSignal("update", self._update_running_state)

        if self._chnPowered is not None:
            self._chnPowered.connectSignal("update", self._update_powered_state)

    def isReady(self):
        """
        Returns a boolean value indicating is the sample changer is ready for operation.

        @return: boolean
        """
        return self.state == SampleChangerState.Ready or \
            self.state == SampleChangerState.Loaded or \
            self.state == SampleChangerState.Charging or \
            self.state == SampleChangerState.StandBy or \
            self.state == SampleChangerState.Disabled

    def diff_send_transfer(self):
        """
        Checks if beamline supervisor is in TRANSFER phase (i.e. sample changer in
        TRANSFER phase too). If is not the case, It sends the sample changer to TRANSFER
        phase. Returns a boolean value indication if the sample changer is in TRANSFER
        phase.

        @return: boolean
        """
        if self.read_super_phase().upper() == "TRANSFER":
            self.logger.error("Supervisor is already in transfer phase")
            return True

        t0 = time.time()
        while True:
            state = str(self.super_state_channel.getValue())
            if str(state) == "ON":
                break

            if (time.time() - t0) > TIMEOUT:
                self.logger.error("Supervisor timeout waiting for ON state. Returning")
                return False

            time.sleep(0.1)

        self.go_transfer_cmd()
        ret = self._wait_phase_done('TRANSFER')
        return ret

    def diff_send_sampleview(self):
        """
        Checks if beamline supervisor is in SAMPLE phase (i.e. sample changer in SAMPLE
        phase too). If is not the case, It sends the sample changer to SAMPLE phase.
        Returns a boolean value indication if the sample changer is in SAMPLE phase.

        @return: boolean
        """
        if self.read_super_phase().upper() == "SAMPLE":
            self.logger.error("Supervisor is already in sample view phase")
            return True

        t0 = time.time()
        while True:
            state = str(self.super_state_channel.getValue())
            if str(state) == "ON":
                break

            if (time.time() - t0) > TIMEOUT:
                self.logger.error("Supervisor timeout waiting for ON state. Returning")
                return False

            time.sleep(0.1)

        self.go_sampleview_cmd()
        ret = self._wait_phase_done('SAMPLE')
        return ret

    def _wait_super_ready(self):
        while True:
            state = str(self.super_state_channel.getValue())
            if state == "ON":
                self.logger.error("Supervisor is in ON state. Returning")
                break

    def _wait_phase_done(self, final_phase):
        """
        Method to wait a phase change. When supervisor reaches the final phase, the
        diffractometer returns True.

        @final_phase: target phase
        @return: boolean
        """
        time.sleep(1.5)
        while True:
            state = str(self.super_state_channel.getValue())
            phase = self.read_super_phase().upper()
            if state == "ON":
                self.logger.error(
                    "Supervisor state {0}, phase {1} Returning".format(state, phase))
                break
            elif str(state) != "MOVING":
                self.logger.error("Supervisor is in a funny state %s" % str(state))
                return False

            self.logger.debug("Supervisor waiting to finish phase change")
            time.sleep(0.2)

        time.sleep(0.1)

        if self.read_super_phase().upper() != final_phase:
            self.logger.error("Supervisor is not yet in %s phase. Aborting load" %
                              final_phase)
            return False
        else:
            self.logger.info(
                "Supervisor is in %s phase. Beamline ready to start sample loading..." %
                final_phase)
            return True

    def save_detdist_position(self):
        self.detdist_saved = self.detdist_position_channel.getValue()
        self.logger.error("Saving current det.distance (%s)" % self.detdist_saved)

    def restore_detdist_position(self):
        if abs(self.detdist_saved - self.detdist_position_channel.getValue()) >= 0.1:
            self.logger.error(
                "Restoring det.distance to %s" % self.detdist_saved)
            self.detdist_position_channel.setValue(self.detdist_saved)
            time.sleep(0.4)
            self._wait_super_ready()

    def read_super_phase(self):
        """
        Returns supervisor phase (CurrentPhase attribute from Beamline Supervisor
        TangoDS)

        @return: str
        """
        return self.phase_channel.getValue()

    def load(self, sample=None, wait=False, wash=False):
        """
        Loads a sample. Overides to include ht basket.

        @sample: sample to load.
        @wait:
        @wash: wash dring the load opearation.
        @return:
        """

        self.logger.debug(
            "Loading sample %s / type(%s)" %
            (sample, type(sample)))

        ret, msg = self._check_coherence()
        if not ret:
            raise Exception(msg)

        sample_ht = self.is_ht_sample(sample)

        if not sample_ht:
            sample = self._resolveComponent(sample)
            self.assertNotCharging()
            use_ht = False
        else:
            sample = sample_ht
            use_ht = True

        if self.hasLoadedSample():
            if (wash is False) and self.getLoadedSample() == sample:
                raise Exception(
                    "The sample %s is already loaded" % sample.getAddress())
            else:
                # Unload first / do a chained load
                pass

        return self._executeTask(SampleChangerState.Loading,
                                 wait, self._doLoad, sample, None, use_ht)

    def unload(self, sample_slot=None, wait=False):
        """
        Unload the sample. If sample_slot=None, unloads to the same slot the sample was
        loaded from.

        @sample_slot:
        @wait:
        @return:
        """
        sample_slot = self._resolveComponent(sample_slot)

        self.assertNotCharging()

        # In case we have manually mounted we can command an unmount
        if not self.hasLoadedSample():
            raise Exception("No sample is loaded")

        return self._executeTask(SampleChangerState.Unloading,
                                 wait, self._doUnload, sample_slot)

    # TODO: this overides identical method from Cats90
    def isPowered(self):
        return self._chnPowered.getValue()

    # TODO: this overides identical method from Cats90
    def isPathRunning(self):
        return self._chnPathRunning.getValue()

    # TODO: this overides method from GenericSampleChanger
    # def hasLoadedSample(self):  # not used.  to use it remove _
     #   return self._chnSampleIsDetected.getValue()

    def _update_running_state(self, value):
        """
        Emits signal with new Running State

        @value: New running state
        """
        self.emit('runningStateChanged', (value, ))

    def _update_powered_state(self, value):
        """
        Emits signal with new Powered State

        @value: New powered state
        """
        self.emit('powerStateChanged', (value, ))

    def _doLoad(self, sample=None, shifts=None, use_ht=False, waitsafe=True):
        """
        Loads a sample on the diffractometer. Performs a simple put operation if the
        diffractometer is empty, and a sample exchange (unmount of old + mount of new
        sample) if a sample is already mounted on the diffractometer.
        Overides Cats90 method.

        @sample: sample to load.
        @shifts: mounting point offsets.
        @use_ht: mount a sample from hot tool.
        """
        if not self._chnPowered.getValue():
            # TODO: implement a wait with timeout method.
            self.logger.debug("CATS power is OFF. Trying to switch the power ON...")
            self._cmdPowerOn()  # try switching power on
            time.sleep(1)

        current_tool = self.get_current_tool()

        self.save_detdist_position()
        ret = self.diff_send_transfer()

        if ret is False:
            self.logger.error(
                "Supervisor cmd transfer phase returned an error.")
            self._updateState()
            raise Exception(
                "CATS cannot get to transfer phase. Aborting sample changer operation.")

        if not self._chnPowered.getValue():
            raise Exception(
                "CATS power is not enabled. Please switch on arm power before "
                "transferring samples.")

        # obtain mounting offsets from diffr
        shifts = self._get_shifts()

        if shifts is None:
            xshift, yshift, zshift = ["0", "0", "0"]
        else:
            xshift, yshift, zshift = map(str, shifts)

        # get sample selection
        selected = self.getSelectedSample()

        self.logger.debug("Selected sample is %s (prev %s)" %
                          (str(selected), str(sample)))

        if not use_ht:
            if sample is not None:
                if sample != selected:
                    self._doSelect(sample)
                    selected = self.getSelectedSample()
            else:
                if selected is not None:
                    sample = selected
                else:
                    raise Exception("No sample selected")
        else:
            selected = None

        # some cancel cases
        if not use_ht and self.hasLoadedSample() and selected == self.getLoadedSample():
            self._updateState()
            raise Exception("The sample " +
                            str(self.getLoadedSample().getAddress()) +
                            " is already loaded")

        if not self.hasLoadedSample() and self.cats_sample_on_diffr() == 1:
            self.logger.warning(
                "Sample on diffractometer, loading aborted!")
            self._updateState()
            raise Exception("The sample " +
                            str(self.getLoadedSample().getAddress()) +
                            " is already loaded")

        if self.cats_sample_on_diffr() == -1:
            self._updateState()
            raise Exception(
                "Conflicting info between diffractometer and on-magnet detection."
                "Consider 'Clear'")

        # end some cancel cases

        # if load_ht
        loaded_ht = self.is_loaded_ht()

        #
        # Loading HT sample
        #
        if use_ht:  # loading HT sample

            if loaded_ht == -1:  # has loaded but it is not HT
                # first unmount (non HT)
                self.logger.error("Mixing load/unload dewar vs HT, NOT IMPLEMENTED YET")
                return

            tool = self.tool_for_basket(100)  # basketno)

            if tool != current_tool:
                self.logger.warning("Changing tool from %s to %s" %
                                    (current_tool, tool))
                changing_tool = True
            else:
                changing_tool = False

            argin = ["2", str(sample), "0", "0", xshift, yshift, zshift]
            self.logger.warning("Loading HT sample, %s" % str(argin))
            if loaded_ht == 1:  # has ht loaded
                cmd_ok = self._executeServerTask(self._cmdChainedLoadHT,
                                                 argin, waitsafe=True)
            else:
                cmd_ok = self._executeServerTask(self._cmdLoadHT, argin, waitsafe=False)

        #
        # Loading non HT sample
        #
        else:
            if loaded_ht == 1:  # has an HT sample mounted
                # first unmount HT
                self.logger.warning(
                    "Mixing load/unload dewar vs HT, NOT IMPLEMENTED YET")
                return

            basketno = selected.getBasketNo()
            sampleno = selected.getVialNo()

            lid, sample = self.basketsample_to_lidsample(basketno, sampleno)
            tool = self.tool_for_basket(basketno)
            stype = self.get_cassette_type(basketno)

            if tool != current_tool:
                self.logger.warning("Changing tool from %s to %s" %
                                    (current_tool, tool))
                changing_tool = True
            else:
                changing_tool = False

            # we should now check basket type on diffr to see if tool is different...
            # then decide what to do

            if shifts is None:
                xshift, yshift, zshift = ["0", "0", "0"]
            else:
                xshift, yshift, zshift = map(str, shifts)

            # prepare argin values
            argin = [
                str(tool),
                str(lid),
                str(sample),
                str(stype),
                "0",
                xshift,
                yshift,
                zshift]

            if tool == 2:
                read_barcode = self.read_datamatrix and \
                               self._cmdChainedLoadBarcode is not None
            else:
                if self.read_datamatrix:
                    self.logger.error("Reading barcode only possible with spine pucks")
                read_barcode = False

            if loaded_ht == -1:  # has a loaded but it is not an HT

                if changing_tool:
                    raise Exception(
                        "This operation requires a tool change. You should unload"
                        "sample first")

                if read_barcode:
                    self.logger.warning(
                        "Chained load sample (barcode), sending to cats: %s" % argin)
                    cmd_ok = self._executeServerTask(
                        self._cmdChainedLoadBarcode, argin, waitsafe=True)
                else:
                    self.logger.warning("Chained load sample, sending to cats: %s"
                        % argin)
                    cmd_ok = self._executeServerTask(
                        self._cmdChainedLoad, argin, waitsafe=True)
            elif loaded_ht == 0:
                if read_barcode:
                    self.logger.warning("Load sample (barcode), sending to cats: %s"
                        % argin)
                    cmd_ok = self._executeServerTask(
                        self._cmdLoadBarcode, argin, waitsafe=True)
                else:
                    self.logger.warning("Load sample, sending to cats:  %s" % argin)
                    cmd_ok = self._executeServerTask(
                        self._cmdLoad, argin, waitsafe=True)

        if not cmd_ok:
            self.logger.info("Load Command failed on device server")
        elif self.auto_prepare_diff and not changing_tool:
            self.logger.info(
                "AUTO_PREPARE_DIFF (On) sample changer is in safe state... "
                "preparing diff now")
            #ret = self.diff_send_sampleview()
            self._wait_super_ready()
            self.go_sampleview_cmd()
            self.logger.info("Restoring detector distance")
            self.restore_detdist_position()
            self._wait_phase_done('SAMPLE')
        else:
            self.logger.info(
                "AUTO_PREPARE_DIFF (Off) sample loading done / or changing tool (%s)" %
                changing_tool)

        # load commands are executed until path is safe. Then we have to wait for
        # path to be finished
        self._waitDeviceSafe()
        # self._waitDeviceReady()

    def _doUnload(self, sample_slot=None, shifts=None):
        """
        Unloads a sample from the diffractometer.
        Overides Cats90 method.

        @sample_slot:
        @shifts: mounting position
        """
        if not self._chnPowered.getValue():
            self._cmdPowerOn()  # try switching power on

        ret = self.diff_send_transfer()

        if ret is False:
            self.logger.error(
                "Supervisor cmd transfer phase returned an error.")
            return

        shifts = self._get_shifts()

        if sample_slot is not None:
            self._doSelect(sample_slot)

        loaded_ht = self.is_loaded_ht()

        if shifts is None:
            xshift, yshift, zshift = ["0", "0", "0"]
        else:
            xshift, yshift, zshift = map(str, shifts)

        loaded_lid = self._chnLidLoadedSample.getValue()
        loaded_num = self._chnNumLoadedSample.getValue()

        if loaded_lid == -1:
            self.logger.warning("Unload sample, no sample mounted detected")
            return

        loaded_basket, loaded_sample = self.lidsample_to_basketsample(
            loaded_lid, loaded_num)

        tool = self.tool_for_basket(loaded_basket)

        argin = [str(tool), "0", xshift, yshift, zshift]

        self.logger.warning("Unload sample, sending to cats:  %s" %
                            argin)
        if loaded_ht == 1:
            cmd_ret = self._executeServerTask(self._cmdUnloadHT, argin)
        else:
            cmd_ret = self._executeServerTask(self._cmdUnload, argin)

    def _doAbort(self):
        """
        Aborts a running trajectory on the sample changer.

        :returns: None
        :rtype: None
        """
        if self.super_abort_cmd is not None:
            self.super_abort_cmd()  # stops super
        self._cmdAbort()
        self._updateState()  # remove software flags like Loading.. reflects current hardware state

    def _check_coherence(self):
        detected = self._chnSampleIsDetected.getValue()
        loaded_lid = self._chnLidLoadedSample.getValue()
        loaded_num = self._chnNumLoadedSample.getValue()

        if -1 in [loaded_lid, loaded_num] and detected:
            return False, "Sample detected on Diffract. but there is no info about it"

        return True, ""

    def _get_shifts(self):
        """
        Get the mounting position from the Diffractometer DS.

        @return: 3-tuple
        """
        if self.shifts_channel is not None:
            shifts = self.shifts_channel.getValue()
        else:
            shifts = None
        return shifts

    # TODO: fix return type
    def is_ht_sample(self, address):
        """
        Returns is sample address belongs to hot tool basket.

        @address: sample address
        @return: int or boolean
        """
        basket, sample = address.split(":")
        try:
            if int(basket) >= 100:
                return int(sample)
            else:
                return False
        except Exception as e:
            self.logger.debug("Cannot identify sample in hot tool")
            return False

    def tool_for_basket(self, basketno):
        """
        Returns the tool corresponding to the basket.

        @basketno: basket number
        @return: int
        """
        if basketno == 100:
            return TOOL_SPINE

        return Cats90.tool_for_basket(self, basketno)

    def is_loaded_ht(self):
        """
           1 : has loaded ht
           0 : nothing loaded
          -1 : loaded but not ht
        """
        sample_lid = self._chnLidLoadedSample.getValue()

        if self.hasLoadedSample():
            if sample_lid == 100:
                return 1
            else:
                return -1
        else:
            return 0


def test_hwo(hwo):
    hwo._updateCatsContents()
    print("Is path running? ", hwo.isPathRunning())
    print("Loading shifts:  ", hwo._get_shifts())
    print("Sample on diffr :  ", hwo.cats_sample_on_diffr())
    print("Baskets :  ", hwo.basket_presence)
    print("Baskets :  ", hwo.getBasketList())
    if hwo.hasLoadedSample():
        print("Loaded is: ", hwo.getLoadedSample().getCoords())
    print("Is mounted sample: ", hwo.is_mounted_sample((1, 1)))
