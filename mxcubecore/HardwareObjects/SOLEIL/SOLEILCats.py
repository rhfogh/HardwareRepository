"""
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
"""

import time
import logging

from mxcubecore.HardwareObjects.Cats90 import *

from cats import cats

class SoleilPuck(Basket):
    def __init__(self, container, number, samples_num=16, name="UniPuck", parent=None):
        super(Basket, self).__init__(
            self.__TYPE__, container, Basket.get_basket_address(number), True
        )

        self._name = name
        self.samples_num = samples_num
        self.parent = parent
        for i in range(samples_num):
            slot = Pin(self, number, i + 1)
            self._add_component(slot)
            if self.parent is not None:
                self.parent.component_by_adddress[slot.get_address()] = slot
        
class SOLEILCats(Cats90):
    """
    Actual implementation of the CATS Sample Changer,
       BESSY BL14.1 installation with 3 lids and 90 samples

    """

    __TYPE__ = "CATS"

    default_no_lids = 3
    baskets_per_lid = 3

    default_basket_type = BASKET_UNIPUCK
    DETECT_PUCKS = True
    
    def init(self):

        self.cats_api = cats()
        self._selected_sample = None
        self._selected_basket = None
        self._scIsCharging = None

        self.read_datamatrix = False
        self.unipuck_tool = TOOL_UNIPUCK

        self.former_loaded = None
        self.cats_device = None
        self.component_by_adddress = {}
        
        self.cats_datamatrix = ""
        self.cats_loaded_lid = None
        self.cats_loaded_num = None

        # Default values
        self.cats_powered = False
        self.cats_status = ""
        self.cats_running = False
        self.cats_state = 'Unknown'
        self.cats_lids_closed = False

        self.basket_types = None
        #self.no_of_baskets = None

        # add support for CATS dewars with variable number of lids

        # Create channels from XML
        self.tangoname = self.get_property("tangoname")
        print('tangoname', self.tangoname)
        self.polling = self.get_property("polling")
        print('polling', self.polling)
        self.no_of_lids = self.get_property("no_of_lids", self.default_no_lids)
        print('no_of_lids', self.no_of_lids)
        self.no_of_baskets = self.get_property("no_of_baskets", 10)
        print('no_of_baskets', self.no_of_baskets)
        self.samples_per_basket = self.get_property("samples_per_basket", 16)
        print('samples_per_basket', self.samples_per_basket)
        self.do_detect_pucks = self.get_property('detect_pucks', SOLEILCats.DETECT_PUCKS)
        print('do_detect_pucks', self.do_detect_pucks)
        self.use_update_timer = self.get_property('update_timer', True)
        print('use_update_timer', self.use_update_timer)
        
        # find number of baskets and number of samples per basket
        self.basket_types = [None] * self.no_of_baskets

        # declare channels to detect basket presence changes
        self.basket_channels = []
        
        # Create channels
        channel_attributes = \
            (
                "State", 
                "Status", 
                "Powered", 
                "PathRunning", 
                "PathSafe", 
                ("NumLoadedSample", "NumSampleOnDiff"), 
                ("LidLoadedSample", "LidSampleOnDiff"), 
                ("SampleBarcode", "Barcode"), 
                ("AllLidsClosed", "di_AllLidsClosed"), 
                "Message", 
                ("LN2RegulationDewar1", "LN2Regulating")
            )
        channel_attributes += tuple((("di_Lid%dState" % k, "di_Lid%dOpen" % k) for k in range(1, self.no_of_lids+1))) 
        channel_attributes += tuple((("Basket%dState" % k, "di_Cassette%dPresence" % k) for k in range(1, self.no_of_baskets+1)))
    
        for channel_attribute in channel_attributes:
            if type(channel_attribute) == tuple:
                channel_name = channel_attribute[1]
                _channel_name = "_chn%s" % channel_attribute[0]
            else:
                channel_name = channel_attribute
                _channel_name = "_chn%s" % channel_name
            channel = self.add_channel(
                {
                    "type": "tango",
                    "name": _channel_name,
                    "tangoname": self.tangoname,
                    "polling": self.polling,
                },
                channel_name,
            )
            print('adding channel %s %s' % (_channel_name, str(channel)))
            setattr(self, _channel_name, channel)
            if "Basket" in _channel_name:
                self.basket_channels.append(channel)
            
            
        self._chnSampleIsDetected = self.get_channel_object("_chnSampleIsDetected")
        
        # Create commands
        command_attributes = \
            (
                ("Load", "put"),
                ("Unload", "get"),
                ("ChainedLoad", "getput"),
                "Abort",
                ("ScanSample", "barcode"),
                "PowerOn",
                "PowerOff",
                "RegulOn",
                "RegulOff",
                "Reset",
                "Back",
                "Safe",
                "Home",
                "Dry",
                ("DrySoak", "dry_soak"),
                "Soak",
                ("ResetParameters", "reset_parameters"),
                ("ClearMemory", "clear_memory"),
                ("AckSampleMemory", "ack_sample_memory"),
                "OpenTool",
                "ToolCal",
                "OpenLid1",
                ("OpenLid2", "home_openlid2"),
                "OpenLid3",
                "CloseLid1",
                "CloseLid2",
                "CloseLid3"
            )
        
        for command_attribute in command_attributes:
            if type(channel_attribute) == tuple:
                command_name = command_attribute[1]
            else:
                command_name = command_attribute
            _command_name = "_cmd%s" % command_name
            command = self.add_command(
                {
                    "type": "tango",
                    "name": _command_name,
                    "tangoname": self.tangoname,
                },
                command_name,
            )
            setattr(self, _command_name, command)
           
        self.cats_model = "CATS"
        self.basket_presence = [True] * self.no_of_baskets
        self._init_sc_contents()

        #
        # connect channel signals to update info
        #

        self.use_update_timer = False  # do not use update_timer for Cats

        self._chnState.connect_signal("update", self.cats_state_changed)
        self._chnPathRunning.connect_signal("update", self.cats_pathrunning_changed)
        self._chnPowered.connect_signal("update", self.cats_powered_changed)
        self._chnPathSafe.connect_signal("update", self.cats_pathsafe_changed)
        self._chnAllLidsClosed.connect_signal("update", self.cats_lids_closed_changed)
        self._chnNumLoadedSample.connect_signal("update", self.cats_loaded_num_changed)

        # connect presence channels
        if self.do_detect_pucks:
            if self.basket_channels is not None:  # old device server
                for basket_index in range(self.no_of_baskets):
                    channel = self.basket_channels[basket_index]
                    channel.connect_signal("update", self.cats_basket_presence_changed)
            else:  # new device server with global CassettePresence attribute
                self._chnBasketPresence.connect_signal("update", self.cats_baskets_changed)

        # Read other XML properties
        read_datamatrix = self.get_property("read_datamatrix")
        if read_datamatrix:
            self.set_read_barcode(True)

        unipuck_tool = self.get_property("unipuck_tool")
        try:
            unipuck_tool = int(unipuck_tool)
            if unipuck_tool:
                self.set_unipuck_tool(unipuck_tool)
        except Exception:
            pass

    def _get_by_address(self, address):
        try:
            component = self.component_by_adddress[address]
        except KeyError:
            component = self.get_component_by_address(address)
            self.component_by_adddress[address] = component
        return component 
    
    def _init_sc_contents(self):
        """
        Initializes the sample changer content with default values.

        :returns: None
        :rtype: None
        """
        _start = time.time()
        logging.getLogger("HWR").warning("SOLEILCats: initializing contents self %s" % self)

        for i in range(self.no_of_baskets):
            if self.basket_types[i] == BASKET_SPINE:
                basket = SpineBasket(self, i + 1)
            elif self.basket_types[i] == BASKET_UNIPUCK:
                basket = UnipuckBasket(self, i + 1)
            else:
                basket = SoleilPuck(self, i + 1, samples_num=self.samples_per_basket, parent=self)

            self._add_component(basket)
            self.component_by_adddress[basket.get_address()] = basket
            
        # write the default basket information into permanent Basket objects
        for basket_index in range(self.no_of_baskets):
            basket = self.get_components()[basket_index]
            datamatrix = None
            present = scanned = False
            basket._set_info(present, datamatrix, scanned)

        # create temporary list with default sample information and indices
        sample_list = []
        for basket_index in range(self.no_of_baskets):
            basket = self.get_components()[basket_index]
            for sample_index in range(basket.get_number_of_samples()):
                sample_list.append(
                    ("", basket_index + 1, sample_index + 1, 1, Pin.STD_HOLDERLENGTH)
                )

        # write the default sample information into permanent Pin objects
        for spl in sample_list:
            address = "%d:%02d" % (spl[1], spl[2])
            sample = self._get_by_address(address)
            datamatrix = None
            present = scanned = loaded = _has_been_loaded = False
            sample._set_info(present, datamatrix, scanned)
            sample._set_loaded(loaded, _has_been_loaded)
            sample._set_holder_length(spl[4])

        logging.getLogger("HWR").warning("SOLEILCats: initializing contents took %.6f" % (time.time()-_start))
        
    def _do_update_cats_contents(self):
        """
        Updates the sample changer content. The state of the puck positions are
        read from the respective channels in the CATS Tango DS.
        The CATS sample sample does not have an detection of each individual sample, so all
        samples are flagged as 'Present' if the respective puck is mounted.

        :returns: None
        :rtype: None
        """

        for basket_index in range(self.no_of_baskets):
            # get presence information from the device server
            if self.do_detect_pucks:
                channel = self.basket_channels[basket_index]
                is_present = channel.get_value()
            else:
                is_present = True
            self.basket_presence[basket_index] = is_present

        self._update_cats_contents()
    
    def _update_cats_contents(self):
        _start = time.time()
        logging.getLogger("HWR").warning(
            "Updating contents %s" % str(self.basket_presence)
        )
        for basket_index in range(self.no_of_baskets):
            # get saved presence information from object's internal bookkeeping
            basket = self.get_components()[basket_index]
            is_present = self.basket_presence[basket_index]

            if is_present is None:
                continue

            # check if the basket presence has changed
            if is_present ^ basket.is_present():
                # a mounting action was detected ...
                if is_present:
                    # basket was mounted
                    present = True
                    scanned = False
                    datamatrix = None
                    basket._set_info(present, datamatrix, scanned)
                else:
                    # basket was removed
                    present = False
                    scanned = False
                    datamatrix = None
                    basket._set_info(present, datamatrix, scanned)

                # set the information for all dependent samples
                for sample_index in range(basket.get_number_of_samples()):
                    address = Pin.get_sample_address((basket_index + 1), (sample_index + 1))
                    sample = self._get_by_address(address)
                        
                    present = sample.get_container().is_present()
                    if present:
                        datamatrix = "          "
                    else:
                        datamatrix = None
                    scanned = False
                    sample._set_info(present, datamatrix, scanned)

                    # forget about any loaded state in newly mounted or removed basket)
                    loaded = _has_been_loaded = False
                    sample._set_loaded(loaded, _has_been_loaded)

        self._trigger_contents_updated_event()
        self._update_loaded_sample()
        print('_update_cats_contents took %.6f' % (time.time() - _start))
        
    def check_power_on(self):
        if not self._chnPowered.get_value():
            logging.getLogger().info("CATS power is not enabled. Switching on the arm power ...")
            try:
                self.cats_api.on()
            except:
                logging.getLogger('HWR').info('SOLEILCats in powerOn exception %s' % traceback.format_exc())

    def load(self, sample=None, wait=True):
        """
        Load a sample.
            overwrite original load() from AbstractSampleChanger to allow finer decision
            on command to use (with or without barcode / or allow for wash in some cases)
            Implement that logic in _do_load()
            Add initial verification about the Powered:
            (NOTE) In fact should be already as the power is considered in the state handling
        """

        self._update_state()  # remove software flags like Loading.
        logging.getLogger().info('SOLEILCats in load')
        self.assert_not_charging()
        self.check_power_on()
        location = sample
        logging.getLogger('HWR').info('SOLEILCats load, location %s' % str(location))
        try:
            sample = self.component_by_adddress[location]
        except:
            sample = self._resolve_component(location)
        self.assert_not_charging()

        if type(location) == str:
            puck, sample = map(int, location.split(':'))
        else:
            puck, sample = location
            
        lid = (puck - 1) / self.no_of_lids + 1
        sample = ((puck - 1) % self.no_of_lids) * self.no_of_samples_in_basket + sample
        lid = int(lid)
        sample = int(sample)
        logging.getLogger('HWR').info('SOLEILCats load, lid %s sample %s' % (lid, sample))
        
        self.cats_api.getput(lid, sample, wait=True)
        self._trigger_info_changed_event()

    def unload(self, sample_slot=None, wait=True):
        logging.getLogger().info('Cats90 in unload')
        self.assert_not_charging()
        self.check_power_on()
        self.cats_api.get(wait=True)
 
    def _update_loaded_sample(self, sample_num=None, lid=None):
        _start = time.time()
        if None in [sample_num, lid]:
            loadedSampleNum = self._chnNumLoadedSample.get_value()
            loadedSampleLid = self._chnLidLoadedSample.get_value()
        else:
            loadedSampleNum = sample_num
            loadedSampleLid = lid

        self.cats_loaded_lid = loadedSampleLid
        self.cats_loaded_num = loadedSampleNum

        logging.getLogger("HWR").info(
            "Updating loaded sample %d:%02d" % (loadedSampleLid, loadedSampleNum)
        )

        if -1 not in [loadedSampleLid, loadedSampleNum]:
            basket, sample = self.lidsample_to_basketsample(
                loadedSampleLid, loadedSampleNum
            )
            address = "%d:%02d" % (basket, sample)
            new_sample = self._get_by_address(address)
        else:
            basket, sample = None, None
            new_sample = None

        old_sample = self.get_loaded_sample()

        logging.getLogger("HWR").debug(
            "----- Cats90 -----.  Sample has changed. Dealing with it - new_sample = %s / old_sample = %s"
            % (new_sample, old_sample)
        )

        if old_sample != new_sample:
            # remove 'loaded' flag from old sample but keep all other information

            if old_sample is not None:
                # there was a sample on the gonio
                loaded = False
                has_been_loaded = True
                old_sample._set_loaded(loaded, has_been_loaded)

            if new_sample is not None:
                loaded = True
                has_been_loaded = True
                new_sample._set_loaded(loaded, has_been_loaded)

            if (
                (old_sample is None)
                or (new_sample is None)
                or (old_sample.get_address() != new_loaded.get_address())
            ):
                self._trigger_loaded_sample_changed_event(new_sample)
                self._trigger_info_changed_event()
        print('_update_loaded_sample took %.4f' % (time.time() - _start))
