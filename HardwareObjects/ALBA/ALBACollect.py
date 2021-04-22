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
[Name] ALBACollect

[Description]
Specific implementation of the collection methods for ALBA synchrotron.
Basic Flow:
    do_collect in AbstractCollect: TODO this opens shutters. Is the beamstop always in by then???
      data_collection_hook in ALBACollect
        prepare_acquisition in ALBACollect
          detector_hwobj.prepare_acquisition
        prepare_collection in ALBACollect                       <-- Repeated for each test image in case of characterization
          detector_hwobj.prepare_collection                     <-- Repeated for each test image in case of characterization
        collect_images                                               
           wait_collection_done
        collection_finished 
    data_collection_cleanup in ALBACollect (supersedes AbstractCollect method with same name)
    
There are currently three routines used for when a collection fails
    data_collection_failed in ALBACollect 
        calls stop_collect (is this explicit call necessary?)
    collection_failed in AbstractCollect is called from do_collect in case of failure 

In case the user aborts data collection, stopCollect is called, then stop_collect then data_collection_cleanup
    In that case, is post processing prevented?
    
[Signals]
- progressInit
- collectConnected
- collectStarted
- collectReady
- progressStop
- collectOscillationFailed

Implementation of Sardana collects uses Macros, as defined in ./HardwareRepository/Command/Sardana.py
We need to investigate how to stop Macros
"""

# RB 2020102: data collection sweeps to be done through a Sardana Macro

from __future__ import print_function

import os
import sys
import time
import gevent
import logging
import math

from HardwareRepository.TaskUtils import task
from AbstractCollect import AbstractCollect
from taurus.core.tango.enums import DevState
from xaloc.resolution import get_dettaby, get_resolution

__credits__ = ["ALBA Synchrotron"]
__version__ = "2.3"
__category__ = "General"


class ALBACollect(AbstractCollect):
    """Main data collection class. Inherited from AbstractMulticollect class
       Collection is done by setting collection parameters and
       executing collect command
    """

    def __init__(self, name):
        AbstractCollect.__init__(self, name)
        self.logger = logging.getLogger("HWR.ALBACollect")
        self.supervisor_hwobj = None
        self.fastshut_hwobj = None
        self.slowshut_hwobj = None
        self.photonshut_hwobj = None
        self.frontend_hwobj = None
        self.diffractometer_hwobj = None
        self.omega_hwobj = None
        self.lims_client_hwobj = None
        self.machine_info_hwobj = None
        self.energy_hwobj = None
        self.resolution_hwobj = None
        self.transmission_hwobj = None
        self.detector_hwobj = None
        self.beam_info_hwobj = None
        self.graphics_manager_hwobj = None
        self.autoprocessing_hwobj = None
        self.flux_hwobj = None
        self.aborted_by_user = None

        #
        #
        # START of 20210218: Lines only necessary for ni660 collects, remove when switching to pure meshct/ascanct scans
        self.cmd_ni_conf = None
        self.cmd_ni_unconf = None
        # END of lines for ni660 scans
        #
        #

        #self.cmd_ni_conf = None
        #self.cmd_ni_unconf = None
        self.set_pilatus_saving_pattern = None
        self.ascanct = None
        self.meshct = None
        self.senv = None
        self.mxcube_sardanascan_running = None

        self.chan_kappa_pos = None
        self.chan_phi_pos = None

        self.chan_undulator_gap = None

        self.scan_motors_hwobj = {}
        self.xaloc_motor_names_dict = {}

        self._error_msg = ""
        self.owner = None
        self.osc_id = None
        self._collecting = None

        self.omega_hwobj = None
        self.graphics_manager_hwobj = None

        self.mesh_scan_line_motor_name = None
        self.mesh_scan_discrete_motor_name = None
        self.scan_start_positions = {}
        self.scan_end_positions = {}
        self.scan_velocities = {}
        self.scan_init_velocities = {}
        self.scan_init_positions = {}
        self.scan_motors_hwobj = {}
        self.scan_move_motor_names = []
        self.scan_all_motor_names = []

        self.mesh_mxcube_fast_motor_name = None
        self.mesh_mxcube_slow_motor_name = None
        self.mesh_fast_index = None
        self.mesh_slow_index = None
        self.mesh_sshaped_bool = None # True: up and down scans, False: only up scans

        #self.helical_positions = None
        #self.saved_omega_velocity = None

        self.omega_init_pos = None
        self.omega_init_vel = None

        self.bypass_shutters = False

        
    def init(self):
        self.logger.debug("Initializing {0}".format(self.__class__.__name__))
        self.ready_event = gevent.event.Event()

        self.supervisor_hwobj = self.getObjectByRole("supervisor")
        self.fastshut_hwobj = self.getObjectByRole("fast_shutter")
        self.slowshut_hwobj = self.getObjectByRole("slow_shutter")
        self.photonshut_hwobj = self.getObjectByRole("photon_shutter")
        self.frontend_hwobj = self.getObjectByRole("frontend")
        self.diffractometer_hwobj = self.getObjectByRole("diffractometer")
        self.omega_hwobj = self.getObjectByRole("omega")
        self.lims_client_hwobj = self.getObjectByRole("lims_client")
        self.machine_info_hwobj = self.getObjectByRole("machine_info")
        self.energy_hwobj = self.getObjectByRole("energy")
        self.resolution_hwobj = self.getObjectByRole("resolution")
        self.transmission_hwobj = self.getObjectByRole("transmission")
        self.detector_hwobj = self.getObjectByRole("detector")
        self.beam_info_hwobj = self.getObjectByRole("beam_info")
        self.graphics_manager_hwobj = self.getObjectByRole("graphics_manager")
        self.autoprocessing_hwobj = self.getObjectByRole("auto_processing")
        self.flux_hwobj = self.getObjectByRole("flux")
        self.aborted_by_user = False

        #
        #
        # START of 20210218: Lines only necessary for ni660 collects, remove when switching to pure meshct/ascanct scans
        self.cmd_ni_conf = self.getCommandObject("ni_configure")
        self.cmd_ni_unconf = self.getCommandObject("ni_unconfigure")
        # END of lines for ni660 scans
        #
        #

        self.set_pilatus_saving_pattern = self.getCommandObject("set_pilatus_saving_pattern")
        self.ascanct = self.getCommandObject("ascanct")
        self.meshct = self.getCommandObject("meshct")
        self.senv = self.getCommandObject("senv")
        self.mxcube_sardanascan_running = False # Is set equal to the macro running whem mecessary


        self.chan_kappa_pos = self.getChannelObject("kappapos")
        self.chan_phi_pos = self.getChannelObject("phipos")

        self.xaloc_motor_names_dict = {'phi': 'omega',
                                       'phiy' : 'omegax',
                                       'phiz': 'omegaz',
                                       'kappa': 'kappa',
                                       'kappaphi': 'phi',
                                       'sampx': 'centx',
                                       'sampy': 'centy'}

        #TODO 20200921 kappa_phi is broken
        #self.scan_all_motor_names = ['phiy', 'phiz', 'sampx', 'sampy', 'kappa', 'kappa_phi']
        self.scan_all_motor_names = ['phiy', 'phiz', 'sampx', 'sampy']
        #TODO get rid of hardcoded max and minvelocity numbers
        self.scan_motors_min_velocity = {'phiy': 6E-6, 'phiz': 1.3E-4, 'sampx': 7E-6, 'sampy': 7E-6, 'kappa': 4, 'kappaphi': 7} # see XALOC elog 925
        self.scan_motors_max_velocity = {'phiy': 1.0, 'phiz': 0.22, 'sampx': 0.15, 'sampy': 0.15, 'kappa': 17, 'kappaphi': 70}
        for scan_motor in self.scan_all_motor_names:
            self.scan_motors_hwobj[scan_motor] = self.getObjectByRole(scan_motor)

        self.mesh_mxcube_fast_motor_name = 'phiy' # omegax
        self.mesh_mxcube_slow_motor_name = 'phiz' # omegaz
        self.mesh_fast_index = 0
        self.mesh_slow_index = 1

        #self.mesh_sshaped_bool = True # True: up and down scans
        self.mesh_sshaped_bool = False # False: only up scans

        #self.chan_undulator_gap = self.getChannelObject("chanUndulatorGap")

        self.scan_motors_hwobj = {}
        self.mesh_scan_line_motor_name = 'phiz'
        self.mesh_scan_discrete_motor_name = 'phiy' 
        #TODO 20200921 kappa_phi is broken
        #self.scan_all_motor_names = ['phiy', 'phiz', 'sampx', 'sampy', 'kappa', 'kappa_phi']
        self.scan_all_motor_names = ['phiy', 'phiz', 'sampx', 'sampy']
        #TODO get rid of hardcoded max and minvelocity numbers
        self.scan_motors_min_velocity = {'phiy': 6E-6, 'phiz': 1.3E-4, 'sampx': 7E-6, 'sampy': 7E-6, 'kappa': 4, 'kappaphi': 7} # see XALOC elog 925
        self.scan_motors_max_velocity = {'phiy': 1.0, 'phiz': 0.22, 'sampx': 0.15, 'sampy': 0.15, 'kappa': 17, 'kappaphi': 70}
        for scan_motor in self.scan_all_motor_names:
            self.scan_motors_hwobj[scan_motor] = self.getObjectByRole(scan_motor)
        
        undulators = []
        try:
            for undulator in self["undulators"]:
                undulators.append(undulator)
        except BaseException:
            pass

        self.exp_type_dict = {'Mesh': 'raster',
                              'Helical': 'Helical'}

        det_px, det_py = self.detector_hwobj.get_pixel_size()

        self.set_beamline_configuration(
            synchrotron_name="ALBA",
            directory_prefix=self.getProperty("directory_prefix"),
            default_exposure_time=self.detector_hwobj.get_default_exposure_time(),
            minimum_exposure_time=self.detector_hwobj.get_minimum_exposure_time(),
            detector_fileext=self.detector_hwobj.get_file_suffix(),
            detector_type=self.detector_hwobj.get_detector_type(),
            detector_manufacturer=self.detector_hwobj.get_manufacturer(),
            detector_model=self.detector_hwobj.get_model(),
            detector_px=det_px,
            detector_py=det_py,
            undulators=undulators,
            focusing_optic=self.getProperty('focusing_optic'),
            monochromator_type=self.getProperty('monochromator'),
            beam_divergence_vertical=self.beam_info_hwobj.get_beam_divergence_hor(),
            beam_divergence_horizontal=self.beam_info_hwobj.get_beam_divergence_ver(),
            polarisation=self.getProperty('polarisation'),
            input_files_server=self.getProperty("input_files_server"))

        self.emit("collectConnected", (True,))
        self.emit("collectReady", (True, ))

        #self.logger.debug('*** bypass shutters: %s', type(os.environ.get('MXCUBE_BYPASS_SHUTTERS')))
        self.bypass_shutters = os.environ.get('MXCUBE_BYPASS_SHUTTERS')
        #self.logger.debug('*** bypass shutters: %s', self.bypass_shutters)
        if self.bypass_shutters and self.bypass_shutters.lower() == 'true':
            self.logger.warning("Simulation mode: BYPASSING the SHUTTERS")
            self.bypass_shutters = True

    def data_collection_hook(self):
        """Main collection hook, called from do_collect in AbstractCollect
        """

        self.logger.info("Running ALBA data collection hook")
        self.logger.info("Waiting for resolution ready...")
        self.resolution_hwobj.wait_end_of_move()
        self.logger.info("Waiting for detector distance ready...")
        self.detector_hwobj.wait_move_distance_done()
        self.logger.info("Waiting for energy ready...")
        self.energy_hwobj.wait_move_energy_done()

        # First, save current parameters to revert back when done/fails
        for motorname in self.scan_all_motor_names: # TODO: check if this is the right place to add these values. Ideally this should be done right after the collect click
            self.logger.info('Inital motor velocity of motor %s = %.4f' % ( motorname, self.scan_motors_hwobj[motorname].get_velocity() ) )
            self.scan_init_velocities[motorname] = self.scan_motors_hwobj[motorname].get_velocity()
            self.scan_init_positions[motorname] = self.scan_motors_hwobj[motorname].getPosition()
        self.omega_init_pos = self.omega_hwobj.getPosition()
        self.omega_init_vel = 60 # self.omega_hwobj.get_velocity()

        #self.logger.info('Inital motor velocities dict %s' % str(self.scan_init_velocities) )
        #self.logger.info('Inital omega velocity value %.1f' % self.omega_init_vel )

        # prepare input files for autoprocessing

        # pass wavelength needed in auto processing input files
        osc_seq = self.current_dc_parameters['oscillation_sequence'][0]
        osc_seq['wavelength'] = self.get_wavelength()

        self.current_dc_parameters['detector_mode'] = ['EXTERNAL_TRIGGER']

        first_image_no = osc_seq['start_image_number']
        exp_time = osc_seq['exposure_time']
        img_range = osc_seq['range']
        omega_speed = 60 # initial the speed value, this will be recalculated later on in prepare_acquisition
        omega_pos = osc_seq['start']
        exp_time = osc_seq['exposure_time']
        nb_images = osc_seq['number_of_images']
        sweep_nb_images = nb_images
        total_collection_time = exp_time * nb_images

        self.logger.info('%s' % osc_seq)

        if self.aborted_by_user:
            self.emit_collection_failed("Aborted by user")
            self.aborted_by_user = False
            return

        ### EDNA_REF, OSC, MESH, HELICAL

        exp_type = self.current_dc_parameters['experiment_type']
        self.logger.debug("Collection method selected is %s" % exp_type)

        if exp_type == "Characterization":
            self.logger.debug("Running a collect (CHARACTERIZATION)")
        elif exp_type == "Helical":
            self.scan_move_motor_names = []
            self.logger.debug("Running a helical collection")
            self.logger.debug(
                "\thelical start positions are: %s" % str(
                    self.scan_start_positions))
            self.logger.debug(
                "\thelical end positions are: %s" % str(
                    self.scan_end_positions))
            self.scan_move_motor_names = []
            self.set_scan_move_motors(self.scan_start_positions, self.scan_end_positions)
            self.scan_velocities = self.calculate_scan_velocities(
                                      self.scan_start_positions, self.scan_end_positions, total_collection_time
                                                                )
            self.logger.info('Preliminary helical setup completed')
        elif exp_type == "Mesh":
            fast_motor_nr_images, slow_motor_nr_images = self.setMeshScanParameters(
                                            osc_seq,
                                            self.mesh_mxcube_fast_motor_name,
                                            self.mesh_mxcube_slow_motor_name,
                                            self.mesh_center,
                                            self.mesh_range
                                            )
        else:
            self.logger.debug("Running a collect (STANDARD)")

        if self.check_scan_velocities( self.scan_velocities ): # There are motors that cant work at required speed
            msg = 'Cant reach the required velocities'
            self.data_collection_failed( msg )
            raise Exception( msg )

        try:
            ready = self.prepare_acquisition()
            init_pos, final_pos, total_dist, omega_speed = self.calc_omega_scan_values(
                                                                omega_pos,
                                                                sweep_nb_images
                                                           )

            # RB init_pos and final_pos include the ramp up and run out range for omega
        except Exception as e:
            self.logger.error("Prepare_acquisition failed")
            self.logger.error('error %s' % str(e) )
            logging.getLogger('user_level_log').error('error %s' % repr( e ) )
            self.data_collection_failed( "Prepare_acquisition failed" )
            raise Exception( e )
        self.logger.debug('  Sweep parameters omega: init %s start %s total dist %s speed %s' %
                          (init_pos, omega_pos, total_dist, omega_speed )
                          )

        self._collecting = True
        # for progressBar brick
        self.emit("progressInit", "Collection", osc_seq['number_of_images'])

        self.emit("collectStarted", (self.owner, 1))

        self.autoprocessing_hwobj.create_input_files(self.current_dc_parameters)

        if not ready:
            msg = "prepare_acquisition not ready"
            self.data_collection_failed( msg )
            self.stop_collect() # This is the only place this function is called, defined in AbstractCollect, is it do anything
            raise Exception( msg )

        if exp_type == 'OSC' or (exp_type == 'Characterization' and nb_images == 1) or exp_type == 'Helical':
            # Sardana collect: run ascanct
            self.collect_prepare_omega( init_pos, omega_speed )
            final_pos = self.prepare_collection(
                start_angle=omega_pos,
                nb_images=nb_images,
                img_range=img_range,
                first_image_no=first_image_no,
                omega_speed = omega_speed
            )
            # omega_speed, start_pos, final_pos, nb_images, first_image_no
            self.collect_images(
                    omega_speed, omega_pos, final_pos, nb_images,first_image_no
                )
        elif exp_type == 'Characterization' and nb_images > 1:   # image one by one
            for imgno in range(nb_images):
                # Sardana collect, run ascanct
                self.collect_prepare_omega( init_pos, omega_speed )
                final_pos = self.prepare_collection(
                    start_angle=omega_pos,
                    nb_images=1,
                    img_range=img_range,
                    first_image_no=first_image_no,
                    omega_speed = omega_speed
                )
                self.collect_images(
                    omega_speed, omega_pos, final_pos, 1, first_image_no
                )
                first_image_no += 1
                omega_pos += 90

                #
                #
                # START of 20210218: Lines only necessary for ni660 collects, remove when switching to pure meshct/ascanct scans

                init_pos, final_pos, total_dist, omega_speed = self.calc_omega_scan_values( omega_pos, nb_images )
                # END of lines for ni660 scans
                #
                #

        elif exp_type == 'Mesh':   # combine all collections
            self.logger.debug("Running a raster collection")
            self.write_image_headers(omega_pos)
            self.collect_mesh(
                      'test_pilatus_omegax_scan',
                      first_image_no,
                      self.mesh_mxcube_fast_motor_name,
                      fast_motor_nr_images,
                      self.mesh_mxcube_slow_motor_name,
                      slow_motor_nr_images,
                      self.mesh_range,
                      exp_time
                    )
            #self.finalize_mesh_scan()

        self.collection_finished()


    # Collect images using direct configuration of the ni660 card
    def collect_images(self, omega_speed, start_pos, final_pos, nb_images, first_image_no):
        """
           Run a single wedge. 
           Start position is the omega position where images should be collected. 
           It is assumed omega is already at the initial position, which comes before the start position
               and allows the mechanics to get up to speed
        """
        self.logger.info("collect_images: Collecting images, by moving omega to %s" % final_pos)
        total_time = (final_pos - self.omega_hwobj.getPosition() ) / omega_speed # assumes omega is already at start position for collection
        self.logger.info("    Total collection time = %s" % total_time)

        # Now collect the data
        self.detector_hwobj.start_collection()

        if omega_speed != 0:
            try: 
                self.logger.info("    Moving omega to final position = %.4f" % final_pos )
                self.omega_hwobj.move( final_pos )
            except Exception as e:
                self.data_collection_failed('Cant open safety shutter for omega speed %.6f' % omega_speed)
                raise Exception(e)
        else:
            try:
                self.open_fast_shutter_for_internal_trigger()
            except Exception as e:
                self.data_collection_failed('MXCuBE is not prepared to collect still images')
                raise Exception(e)

        if self.current_dc_parameters['experiment_type'] == 'Helical':
            self.wait_start_helical_motors()

        self.wait_collection_done(first_image_no, nb_images + first_image_no - 1, total_time)

    def collect_mesh(
                       self,
                       measurement_group,
                       first_image_no,
                       mesh_mxcube_fast_motor_name,
                       mesh_num_frames_per_line,
                       mesh_mxcube_slow_motor_name,
                       mesh_num_lines,
                       mesh_range,
                       time_interval
                    ):
        """
           mesh scan using Sardana ascanct. It is assumed that the fast motor and slow motor move in a positive direction
           The mesh scan does an S shape when sshape is true.
        """

        # Calculate motor steps
        mov_fast_step = mesh_range[self.mesh_fast_index] / float( mesh_num_frames_per_line )
        mov_slow_step = 0
        if mesh_num_lines > 1: mov_slow_step = mesh_range[self.mesh_slow_index] / float( mesh_num_lines - 1 )

        local_slow_start_pos = self.scan_start_positions[ mesh_mxcube_slow_motor_name ]
        local_first_image_no = first_image_no
        mesh_xaloc_fast_motor_name = self.xaloc_motor_names_dict[ mesh_mxcube_fast_motor_name ]
        self.logger.debug("Running a raster collection")
        # final_pos is end of omega range (not including safedelta)
        detdeadtime = self.detector_hwobj.get_latency_time()
        total_time = mesh_num_frames_per_line * mesh_num_lines * time_interval

        #TODO: fix image headers
        #self.write_image_headers( self.omega_hwobj.getPosition() )

        local_fast_start_pos = self.scan_start_positions[ mesh_mxcube_fast_motor_name ]
        local_fast_end_pos = self.scan_end_positions[ mesh_mxcube_fast_motor_name ]
        self.scan_motors_hwobj[ self.mesh_mxcube_slow_motor_name ].syncMove( local_slow_start_pos )

        #TODO fix the numbering of the files
        self.prepare_sardana_env( measurement_group )
        sshape = True

        for lineno in range( mesh_num_lines ):
            if self.aborted_by_user:
                self.logger.info("User interruption of data collection during mesh scan detected, aborting mesh scan" )
                break # cleanup will be handled in stop_collect
            else:
                self.logger.debug("\t line %s out of %s" % ( lineno + 1, mesh_num_lines ) )

                #TODO move omegax/phiy to starting position of collection (OR is this done in the MxCube sequence somewhere???
                # Sardana will move the motor back to the inital position after the scan. Two consecuences:
                #    the fast motor will always go back the same position after each scan, so better move it to the start position of the scan to prevent excessive movements


                self.logger.debug("mesh_xaloc_fast_motor_name = %s\nlocal_fast_start_pos = %.4f\nlocal_fast_end_pos = %.4f\nmov_fast_step = %.4f\ntime_interval - detdeadtime = %.4f\ndetdeadtime = %.4f\nfirst_image_no = %d\nmesh_num_frames_per_line = %d" % (
                                          mesh_xaloc_fast_motor_name,
                                          local_fast_start_pos,
                                          local_fast_end_pos,
                                          mov_fast_step,
                                          time_interval - detdeadtime,
                                          detdeadtime,
                                          local_first_image_no,
                                          mesh_num_frames_per_line
                                   )
                                 )
                # TODO: that should not be here: prepare mesh or sardana instead
                self.write_image_headers(0)
                time.sleep(1)
                self.run_ascanct(
                        mesh_xaloc_fast_motor_name,
                        local_fast_start_pos,
                        local_fast_end_pos,
                        mov_fast_step,
                        time_interval,
                        detdeadtime,
                        local_first_image_no,
                        mesh_num_frames_per_line
                    )
                local_first_image_no += mesh_num_frames_per_line
                self.scan_motors_hwobj[ self.mesh_mxcube_slow_motor_name ].syncMoveRelative(
                                             mov_slow_step
                                        )
                #TODO sscans are not possible yet, becuase Sardana moves the motors back to the starting position
                #self.logger.debug('  scan_start_positions before swap= %s' % self.scan_start_positions )
                #self.logger.debug('  scan_end_positions before swap= %s' % self.scan_end_positions )
                ## Invert fast start and end positions so the motor will move the other way for the next sweep
                if sshape:
                    dummy = local_fast_start_pos
                    local_fast_start_pos = local_fast_end_pos
                    local_fast_end_pos = dummy

                #self.logger.debug('  scan_start_positions after swap= %s' % self.scan_start_positions )
                #self.logger.debug('  scan_end_positions after swap= %s' % self.scan_end_positions )

        if self.diffractometer_hwobj.getProperty("omegaReference"):
                self.logger.debug("\t Propertry %s" % str( self.diffractometer_hwobj.getProperty("omegaReference") ) )
                omegaz_reference = eval( self.diffractometer_hwobj.getProperty("omegaReference") )
                self.logger.debug("\t Moving motor %s to %.4f" % ( self.mesh_mxcube_slow_motor_name, omegaz_reference['position'] ) )
                self.scan_motors_hwobj[ self.mesh_mxcube_slow_motor_name ].syncMove( omegaz_reference['position']  )
        self.wait_collection_done(first_image_no, first_image_no + ( mesh_num_frames_per_line * mesh_num_lines ) - 1, total_time + 5)
        self.data_collection_end()
        self.collection_finished()


    # omega_speed and det_trigger are not necessary for sardanized collections
    def prepare_collection(self, start_angle, nb_images, img_range, first_image_no, omega_speed ):
        osc_seq = self.current_dc_parameters['oscillation_sequence'][0]

        total_dist = float ( nb_images * img_range )
        final_pos = start_angle + total_dist

        self.logger.info("nb_images: %s / total_distance: %s " %
                                      ( nb_images, total_dist )
                        )

        self.write_image_headers(start_angle)

        for scanmovemotorname in self.scan_move_motor_names:
            self.logger.info("Setting %s velocity to %.4f" % (scanmovemotorname, self.scan_velocities[scanmovemotorname]) )
            try:
                #self._motor_persistently_set_velocity(self.scan_motors_hwobj[scanmovemotorname], self.scan_velocities[scanmovemotorname])
                self.scan_motors_hwobj[scanmovemotorname].set_velocity( self.scan_velocities[scanmovemotorname] )
            except Exception as e:
                self.logger.info("Cant set the scan velocity of motor %s" % scanmovemotorname )
                self.data_collection_failed("Cant set the scan velocity of motor %s" % scanmovemotorname)
                raise Exception( e )

        #
        #
        # START of 20210218: Lines only necessary for ni660 collects, remove when switching to pure meshct/ascanct scans
        try:
            self.detector_hwobj.prepare_collection( nb_images, first_image_no )
        except Exception as e :
            self.logger.error(e)
            logging.getLogger('user_level_log').error("Cannot prepare the detector, does the image exist? If not, check the detector state" )
            raise(e)

        if self.current_dc_parameters['experiment_type'] != 'Mesh':
            self.detector_hwobj.set_detector_mode( self.current_dc_parameters['detector_mode'][0] )
            if omega_speed != 0 :
               self.configure_ni(start_angle, total_dist)

        # END of lines for ni660 scans
        #
        #

        #TODO it doesnt make sense that prepare_collection returns some of these numbers, reorganize!
        return final_pos

    def prepare_sardana_env( self, measurement_group ):
                # This is repeated code: occurs in write_image_headers and wait_save_image
        fileinfo = self.current_dc_parameters['fileinfo']
        basedir = fileinfo['directory']
        template = fileinfo['template'] # prefix_1_%04d.cbf
        sardtemplate = template.split('%')[0] + \
            "{index:%02d}" % int(template.split('%')[-1].split('d')[0]) + \
                template.split('%')[-1].split('d')[1]
        savingpattern = 'file://' + os.path.join( basedir , sardtemplate )
        self.logger.info("savingpattern = %s" % savingpattern)

        # Set the appropriate environment variables
        # TODO: the time.sleeps are necessary to wait for the door to recover. Make a while loop to check for doors ON. see ./HardwareRepository/Command/Sardana.py
        # save the images to the write place
        self.logger.info("setting ActiveMntGrp")
        self.senv('ActiveMntGrp', measurement_group, wait=True)
        time.sleep(0.1)
        self.logger.info("setting set_pilatus_saving_pattern")
        self.set_pilatus_saving_pattern( measurement_group, savingpattern, wait=True)
        self.logger.info("senv.door_state = %s" % self.set_pilatus_saving_pattern.door_state )

        # save the collection details
        self.logger.info("setting ScanDir")
        self.senv( 'ScanDir', basedir, wait=True )
        time.sleep(0.1)
        self.logger.info("setting ScanFile")
        self.senv( 'ScanFile ' + str( template.split('_%')[0] + '.dat'), wait=True )
        time.sleep(0.1)

    #def run_meshct(
                      #self,
                      #first_image_no,
                      #mesh_mxcube_fast_motor_name,
                      #fast_motor_nr_images,
                      #mesh_mxcube_slow_motor_name,
                      #slow_motor_nr_images,
                      #time_interval,
                      #sshaped_bool,
                      #deadtime
                    #):
        ##meshct
        ##Parameters:
            ##motor1 : (Moveable) First motor to move (generates triggers, fast motor)
            ##m1_start_pos : (Float) Scan start position for first motor
            ##m1_final_pos : (Float) Scan final position for first motor
            ##m1_nr_interv : (Integer) Number of scan intervals
            ##motor2 : (Moveable) Second motor to move
            ##m2_start_pos : (Float) Scan start position for second motor
            ##m2_final_pos : (Float) Scan final position for second motor
            ##m2_nr_interv : (Integer) Number of scan intervals
            ##integ_time : (Float) Integration time
            ##bidirectional : (Boolean) Save time by scanning s-shaped
            ##latency_time : (Float) Latency time
        #self.logger.debug("meshct parameters:")
        #self.logger.debug("    fast_motor_name: %s" % mesh_mxcube_fast_motor_name)
        #self.logger.debug("    xaloc fast_motor_name: %s" % self.xaloc_motor_names_dict[mesh_mxcube_fast_motor_name])
        #self.logger.debug("    fast_motor_start_pos: %s" % self.scan_start_positions[mesh_mxcube_fast_motor_name])
        #self.logger.debug("    fast_motor_end_pos: %s" % self.scan_end_positions[mesh_mxcube_fast_motor_name])
        #self.logger.debug("    fast_motor_nr_images: %s" % fast_motor_nr_images)
        #self.logger.debug("    slow_motor_name: %s" % mesh_xaloc_slow_motor_name)
        #self.logger.debug("    xaloc slow_motor_name: %s" % self.xaloc_motor_names_dict[mesh_mxcube_slow_motor_name])
        #self.logger.debug("    slow_motor_start_pos: %s" % self.scan_start_positions[mesh_mxcube_slow_motor_name])
        #self.logger.debug("    slow_motor_end_pos: %s" % self.scan_end_positions[mesh_mxcube_slow_motor_name])
        #self.logger.debug("    slow_motor_nr_images: %s" % slow_motor_nr_images)
        #self.logger.debug("    time_interval: %.4f" % time_interval)
        #self.logger.debug("    sshaped_bool: %s" % sshaped_bool)
        #self.logger.debug("    deadtime: %.4f" % deadtime)

        # TODO: include the first image as a parameter
        #self.meshct(
                      #self.xaloc_motor_names_dict[mesh_mxcube_fast_motor_name],
                      #self.scan_start_positions[mesh_mxcube_fast_motor_name],
                      #self.scan_end_positions[mesh_mxcube_fast_motor_name], #start pos of fast motor for last data point
                      #fast_motor_nr_images - 1,
                      #self.xaloc_motor_names_dict[mesh_mxcube_slow_motor_name],
                      #self.scan_start_positions[mesh_mxcube_slow_motor_name],
                      #self.scan_end_positions[mesh_mxcube_slow_motor_name], #start pos of slow motor for last data point
                      #slow_motor_nr_images - 1,
                      #time_interval - deadtime,
                      #sshaped_bool, # True: up and down scans, False: only up scans
                      #deadtime
                   #)
        #total_time = fast_motor_nr_images * fast_motor_nr_images * ( time_interval + deadtime )

        #self.wait_collection_done(first_image_no-1, fast_motor_nr_images * slow_motor_nr_images - 1, total_time + 5) # TODO: allow for start at higher image numbers
        #self.data_collection_end()
        #self.collection_finished()


    def run_ascanct(self, moveable, start_pos, final_pos, deg_interval, time_interval, deadtime, first_image_no, nb_images):
        if self.aborted_by_user:
                self.logger.info("User interruption of data collection during mesh scan detected, aborting ascanct" )
                return # cleanup will be handled in stop_collect

        self.logger.info( "Collecting images using the ascanct macro" )
        total_time = time_interval * (final_pos - start_pos) / deg_interval

        if final_pos < start_pos: deg_interval = - deg_interval

        self.logger.debug("moveable                  %s" % moveable )
        self.logger.debug("start_pos                 %.4f" % start_pos )
        self.logger.debug("final_pos - deg_interval  %.4f " % (final_pos - deg_interval) )
        self.logger.debug("nb_images - 1             %d" % ( nb_images - 1 ) )
        self.logger.debug("time_interval - deadtime  %.4f" % ( time_interval - deadtime ) )
        self.logger.debug("deadtime                  %.4f" % ( deadtime ) )

        #TODO: Set the first image number here
        self.mxcube_sardanascan_running = True
        self.ascanct(moveable,
                         start_pos,
                         final_pos - deg_interval,
                         nb_images - 1,
                         time_interval - deadtime,
                         deadtime,
                         wait = True
                     )
        self.mxcube_sardanascan_running = False


    def calc_omega_scan_values( self, omega_start_pos, nb_images ):
        """
           Calculates the values at which the omega should start and end so that
           during collection it has a constant speed
        """
        osc_seq = self.current_dc_parameters['oscillation_sequence'][0]

        img_range = osc_seq['range']
        exp_time = osc_seq['exposure_time']

        total_dist = float ( nb_images * img_range )
        total_time = float ( nb_images * exp_time )
        omega_acceltime = self.omega_hwobj.get_acceleration()
        omega_speed = float( total_dist / total_time )

        # TODO: for mesh scans, this range is way to big
        safe_delta = 1
        if self.current_dc_parameters['experiment_type'] == 'Mesh':
            safe_delta = 0.5
        else:
            safe_delta = 9.0 * omega_speed * omega_acceltime

        init_pos = omega_start_pos - safe_delta
        final_pos = omega_start_pos + total_dist + safe_delta #TODO adjust the margins to the minimum necessary

        return ( init_pos, final_pos, total_dist, omega_speed )


    def collect_prepare_omega(self, omega_pos, omega_speed):
        '''
            Prepares omega for sweep.
            - Sets the velocity to the fast speed
            - Moves omega to the initial position ( required by ni card)
            - Sets the velocity to the collect speed
        '''

        try:
            self.omega_hwobj.set_velocity( 60 )
        except Exception as e :
            self.logger.error("Error setting omega velocity, state is %s" % str(self.omega_hwobj.getState()))
            self.logger.info("Omega velocity set to its nominal value")
            self.data_collection_failed('Omega position could not be set')
            raise Exception( e )

        self.logger.info("Moving omega to initial position %s" % omega_pos)
        try:
            if math.fabs(self.omega_hwobj.getPosition() - omega_pos) > 0.0001:
                #self._motor_persistently_syncmove(self.omega_hwobj, omega_pos)
                self.omega_hwobj.syncMove( omega_pos )
        except Exception as e :
            self.logger.info("Omega state is %s" % str(self.omega_hwobj.getState()))
            self.data_collection_failed('Omega position could not be set')
            raise Exception( e )

        self.logger.info("Setting omega velocity to %s and is moving %s" % (omega_speed, self.omega_hwobj.is_moving() ) )

        try:
            self.omega_hwobj.set_velocity( omega_speed )
        except Exception as e:
            self.data_collection_failed("Cannot set the omega velocity to %s" % omega_speed)
            raise Exception( e )

        return
        # END of lines for ni660 scans
        #
        #

    def wait_start_helical_motors( self):
        timestep = 1 # secs
        timeout = 10 # secs
        self.detector_hwobj.wait_running( timestep, timeout ) # once the detector is running (collection started), start the helical motors
        for scanmovemotorname in self.scan_move_motor_names:
            try: 
                # It might be faster (but more dangerous)  
                self.scan_motors_hwobj[scanmovemotorname].position_channel.setValue( self.scan_end_positions[scanmovemotorname] )
                # the safer option:
                #self.scan_motors_hwobj[scanmovemotorname].move( self.scan_end_positions[scanmovemotorname] )
                
                #self.logger.info('Moving motor %s from %.4f to final position %.4f at %.6f velocity' % 
                #                  (scanmovemotorname, self.scan_start_positions[scanmovemotorname],
                #                     self.scan_end_positions[scanmovemotorname], 
                #                       self.scan_velocities[scanmovemotorname] ) )
            except Exception as e:
                self.data_collection_failed('Cannot move the helical motor %s to its end position' % scanmovemotorname)
                raise Exception(e)



    def data_collection_end(self):
        self.omega_hwobj.set_velocity(60)
        self.unconfigure_ni()

    def data_collection_failed(self, failed_msg="ALBACollect data_collection_failed"):
        self.logger.info("ALBACollect data_collection_failed")
        logging.getLogger('user_level_log').error(failed_msg)

        self.logger.debug("  Initiating recovery sequence")
        self.stopCollect() # is it necessary to call this, or is it called through events? If it is not, this method is not necessary

        #AbstractCollect.data_collection_failed() # there is no data_collection_failed in AbstractCollect

        #AbstractCollect.data_collection_failed() # there is no data_collection_failed in AbstractCollect
        
    def prepare_acquisition(self):
        """
          checks shutters, moves supervisor to collect phase, prepares detector and calculates omega start values
          omega_start_pos should be the value at which the collection starts
        """

        fileinfo = self.current_dc_parameters['fileinfo']
        basedir = fileinfo['directory']
        self.check_directory(basedir)

        full_path = self.get_image_file_name( 1 )
        if os.path.exists( full_path ):
            msg = "Filename already exists, TODO: let mxcube handle this!!"
            logging.getLogger('user_level_log').error(msg)
            raise( Exception( msg ) )

        # Save omega velocity
        # self.saved_omega_velocity = self.omega_hwobj.get_velocity()
        # Better use it nominal velocity (to be properly defined in Sardana motor)
        # Ensure omega has its nominal velocity to go to the initial position
        # We have to ensure omega is not moving when setting the velocity

        # create directories if needed

        # check fast shutter closed. others opened

        if self.bypass_shutters:
            logging.getLogger('user_level_log').warning("Shutters BYPASSED")
        else:
            _ok, failed = self.check_shutters()
            if not _ok:
                msg = "Shutter(s) {} NOT READY".format(failed)
                logging.getLogger('user_level_log').error(msg)
                return _ok, msg
            else:
                logging.getLogger('user_level_log').info("Shutters READY")

        gevent.sleep(1)
        self.logger.info(
            "Waiting diffractometer ready (is %s)" % str(self.diffractometer_hwobj.current_state))
        self.diffractometer_hwobj.wait_device_ready(timeout=10) # Is an exception generated upon timeout???
        self.logger.info("Diffractometer is now ready.")

        # go to collect phase
        if not self.is_collect_phase():
            self.logger.info("Supervisor not in collect phase, asking to go...")
            success = self.go_to_collect()
            if not success:
                msg = "Supervisor cannot set COLLECT phase. Issue an Init in the diff and supervisor devices. Omegax should be between -1 and +1 mm"
                logging.getLogger('user_level_log').error(msg)
                self.logger.error(msg)
                return False, msg

        detok = self.detector_hwobj.get_cam_state() == 'STANDBY'
        self.logger.info( 'Detector ok %s' % detok )

        if not detok:
            msg = "Cannot prepare the detector for acquisition, check the Pilatus state. "
            if self.detector_hwobj.get_cam_state() != 'SETTING_ENERGY':
                msg += "Issuing a reset command in bl13/eh/pilatuslima, try again"
                self.detector_hwobj.cmd_reset()
            else: msg += "The Pilatus is setting the energy, please be patient!!!"
            self.logger.info( msg )
            self.data_collection_failed( msg )
            self.stop_collect()
            raise Exception( msg )

        #
        #
        # START of 20210218: Lines only necessary for ni660 collects, remove when switching to pure meshct/ascanct scans

        detok = False
        try:
            detok = self.detector_hwobj.prepare_acquisition(self.current_dc_parameters)
            self.logger.info("Prepared detector for acquistion, detok = %s" % str( detok ) )
        except Exception as e:
            self.logger.info( "Cannot prepare the detector for acquisition" )
            self.data_collection_failed("Cannot prepare the detector for acquisition")
            self.stop_collect()
            raise Exception( e )

        if not detok:
            msg = "Cannot prepare the detector for acquisition" 
            self.logger.info( msg )
            self.data_collection_failed( msg )
            self.stop_collect()
            raise Exception( msg )

        # END of lines for ni660 scans
        #
        #

        return ( detok )

    def write_image_headers(self, start_angle):
        # maintain for sardana scans?
        fileinfo = self.current_dc_parameters['fileinfo']
        basedir = fileinfo['directory']

        exp_type = self.current_dc_parameters['experiment_type']
        osc_seq = self.current_dc_parameters['oscillation_sequence'][0]

        nb_images = osc_seq['number_of_images']
        # start_angle = osc_seq['start']

        try: img_range = osc_seq['range']
        except: img_range = 0

        if exp_type == "Characterization":
            angle_spacing = 90
        else:
            angle_spacing = img_range

        exp_time = osc_seq['exposure_time']

        # PROGRAM Image Headers
        #latency_time = 0.003
        latency_time = self.detector_hwobj.get_latency_time()
        limaexpt = exp_time - latency_time

        self.image_headers = {}

        angle_info = [start_angle, img_range, angle_spacing]

        self.image_headers['nb_images'] = nb_images
        self.image_headers['Exposure_time'] = "%.4f" % limaexpt
        self.image_headers['Exposure_period'] = "%.4f" % exp_time
        self.image_headers['Start_angle'] = "%f deg." % start_angle
        self.image_headers['Angle_increment'] = "%f deg." % img_range
        self.image_headers['Wavelength'] = self.energy_hwobj.get_wavelength()

        self.image_headers["Detector_distance"] = "%.5f m" % (
            self.detector_hwobj.get_distance() / 1000.0)
        self.image_headers["Detector_Voffset"] = '0 m'

        beamx, beamy = self.detector_hwobj.get_beam_centre()
        self.image_headers["Beam_xy"] = "(%.2f, %.2f) pixels" % (beamx, beamy)

        self.image_headers["Filter_transmission"] = "%.4f" % (
            self.transmission_hwobj.getAttFactor() / 100.0)
        self.image_headers["Flux"] = "%.4g" % self.flux_hwobj.get_flux()
        self.image_headers["Detector_2theta"] = "0.0000"
        self.image_headers["Polarization"] = "0.99"
        self.image_headers["Alpha"] = '0 deg.'

        # TODO add XALOC phi (MXCuBE kappa_phi) to image headers
        self.image_headers["Kappa"] = "%.4f deg." % self.chan_kappa_pos.getValue()
        self.image_headers["Phi"] = "%.4f deg." % self.chan_phi_pos.getValue()

        self.image_headers["Chi"] = "0 deg."
        self.image_headers["Oscillation_axis"] = "omega (X, CW)"
        self.image_headers["N_oscillations"] = '1'
        self.image_headers["Detector_2theta"] = "0.0000 deg"

        self.image_headers["Image_path"] = ': %s' % basedir

        self.image_headers["Threshold_setting"] = '%0f eV' %\
                                                  self.detector_hwobj.get_threshold()
        self.image_headers["Gain_setting"] = '%s (vtr)' % str(
            self.detector_hwobj.get_gain())

        self.image_headers["Tau"] = '%s s' % str(199.1e-09)
        self.image_headers["Count_cutoff"] = '%s counts' % str(370913)
        self.image_headers["N_excluded_pixels"] = '= %s' % str(1178)
        self.image_headers["Excluded_pixels"] = ': %s' % str("badpix_mask.tif")
        self.image_headers["Trim_file"] = ': %s' % str(
            "p6m0108_E12661_T6330_vrf_m0p20.bin") #TODO: pick up the true trim file!!!!!

        self.detector_hwobj.set_image_headers(self.image_headers, angle_info)

    def wait_collection_done(self, first_image_no, last_image_no, total_time):

        self.logger.info("  Total acquisition time = %.2f s" % total_time )
        self.logger.info("  last_image_no          = %d" % last_image_no )

        self.wait_save_image(first_image_no)
        self.omega_hwobj.wait_end_of_move( timeout= total_time + 5 )
        self.wait_save_image( last_image_no ) # TODO: the minus one to fix the fact that first image is 0
        self.logger.info("  wait_collection_done last image found" )
        for motorname in self.scan_move_motor_names:
            self.logger.info("     Motor %s position = %.2f" % ( motorname, self.scan_motors_hwobj[motorname].getPosition() ) )

        # Wait for omega to stop moving, it continues further than necessary
        self.omega_hwobj.wait_end_of_move(timeout=40)
        # Wait for any other motors to stop moving
        for motorname in self.scan_move_motor_names:
            self.scan_motors_hwobj[motorname].wait_end_of_move(timeout=40)
        # Make sure the detector is ready (in stand by and not moving)
        self.detector_hwobj.wait_ready()

    def wait_save_image(self, frame_number, timeout=25):

        full_path = self.get_image_file_name( frame_number )

        start_wait = time.time()

        self.logger.debug("   waiting for image on disk: %s" % full_path)

        while not os.path.exists(full_path) and not self.aborted_by_user:
            # TODO: review next line for NFTS related issues.
            if (time.time() - start_wait) > timeout:
                self.logger.debug("   giving up waiting for image")
                cam_state = self.detector_hwobj.chan_cam_state.getValue()
                acq_status = self.detector_hwobj.chan_acq_status.getValue()
                fault_error = self.detector_hwobj.chan_acq_status_fault_error.getValue()
                self.detector_hwobj.get_saving_statistics()
                msg = "cam_state = {}, acq_status = {}, fault_error = {}".format(
                    cam_state, acq_status, fault_error)
                logging.getLogger('user_level_log').error("Incomplete data collection")
                logging.getLogger('user_level_log').error(msg)
                raise RuntimeError(msg)
                #return False
            #logging.getLogger('user_level_log').error("self._collecting %s" % str(self._collecting) )
            time.sleep(0.2)

        self.detector_hwobj.get_saving_statistics()

        # self.last_saved_image = fullpath

        # generate thumbnails
        fileinfo = self.current_dc_parameters['fileinfo']
        template = fileinfo['template']
        filename = template % frame_number
        archive_dir = fileinfo['archive_directory']
        self.check_directory(archive_dir)

        jpeg_filename = os.path.splitext(filename)[0] + ".jpeg"
        thumb_filename = os.path.splitext(filename)[0] + ".thumb.jpeg"

        thumb_fullpath = os.path.join(archive_dir, thumb_filename)
        jpeg_fullpath = os.path.join(archive_dir, jpeg_filename)

        self.logger.debug(
            "   creating thumbnails for  %s in: %s and %s" %
            (full_path, jpeg_fullpath, thumb_fullpath))
        cmd = "adxv_thumb 0.4 %s %s" % (full_path, jpeg_fullpath)
        os.system(cmd)
        cmd = "adxv_thumb 0.1 %s %s" % (full_path, thumb_fullpath)
        os.system(cmd)

        self.logger.debug("   writing thumbnails info in LIMS")
        self.store_image_in_lims(frame_number)

        self.logger.debug("   Found image on disk: %s" % full_path)

        return True

    def get_image_file_name( self, frame_number ):
        fileinfo = self.current_dc_parameters['fileinfo']
        basedir = fileinfo['directory']
        template = fileinfo['template']

        filename = template % frame_number
        full_path = os.path.join(basedir, filename)

        return full_path

    def check_shutters(self):

        # Shutters ready: 1, 1, 1, 1
        
        # fast shutter closed: State = 1
        # slow shutter is close: State = 0
        # photon shutter is close: State = 0
        # front end is close: State = 0
        fast_shutter = self.fastshut_hwobj.getState()
        slow_shutter = self.slowshut_hwobj.getState()
        photon_shutter = self.photonshut_hwobj.getState()
        front_end = self.frontend_hwobj.getState()

        shutters = ['fast', 'slow', 'photon', 'front-end']
        states = [fast_shutter, slow_shutter, photon_shutter, front_end]

        failed = [s for s, state in zip(shutters, states) if not state]

        self.logger.debug("fast shutter state is: %s" % fast_shutter) 
        self.logger.debug("slow shutter state is: %s" % slow_shutter) 
        self.logger.debug("photon shutter state is: %s" % photon_shutter) 
        self.logger.debug("front_end state is: %s" % front_end) 

        return all([fast_shutter, slow_shutter, photon_shutter, front_end]), failed

    def get_image_headers(self):
        headers = []
        return headers

    def data_collection_cleanup(self):
        # this is called from AbstractCollect.do_collect, so it is always called and will be the last step in the data collection
        self.logger.info('Initial velocity %s' % self.scan_init_velocities)
        try:
            if self.omega_init_vel != None: # Sometimes, data collections fails before the initial velocities are set, eg when supervisor or diff DS are in alarm
                self.logger.info('  Setting velocity of motor omega')
                self.logger.info('     to initial velocity %.6f' % self.omega_init_vel )
                self.omega_hwobj.set_velocity( self.omega_init_vel )
        except:
            self.logger.info('  Setting velocity of motor omega to %.1f failed' % self.omega_init_vel)

        for motorname in self.scan_move_motor_names:
            self.logger.info('  Setting velocity of motor %s' % motorname )
            self.logger.info('     to initial velocity %.6f' % self.scan_init_velocities[motorname] ) 
            #self._motor_persistently_set_velocity( self.scan_motors_hwobj[motorname], self.scan_init_velocities[motorname] )
            self.scan_motors_hwobj[motorname].set_velocity( self.scan_init_velocities[motorname] )

        self.logger.debug("Cleanup: moving omega to initial position %s" % self.omega_init_pos)
        try:
            # RB: isnt it better that the detetor keeps collecting to not loose images of a collection.Or increase wating time?
            # In fact, this is done in stopCollect when a user specifically asks for an Abort
            if self.detector_hwobj.get_cam_state() == 'ERROR': self.detector_hwobj.stop_collection()
            if self.omega_init_pos != None: # Sometiemes collection fails before omega_init_pos is set, no need to move in those cases
                self.omega_hwobj.move( self.omega_init_pos )
        except:
            self.logger.debug("Omega needs to be stopped before restoring initial position, will try this now")
            self.omega_hwobj.stop()
            self.omega_hwobj.set_velocity( self.omega_init_vel )
            self.omega_hwobj.move(self.omega_init_pos)

        self.scan_delete_motor_data()
        self.fastshut_hwobj.close() # RB: not sure if it closes when unconfiguring it, just in case
        self.aborted_by_user = False

        self.logger.debug("ALBA data_collection_cleanup finished")

    def check_directory(self, basedir):
        if not os.path.exists(basedir):
            try:
                os.makedirs(basedir)
            except OSError as e:
                import errno
                if e.errno != errno.EEXIST:
                    logging.getLogger('user_level_log').error('Directories cannot be made, are the lockdown settings correct?')
                    self.logger.debug("user_level_log').error('Directories cannot be made, are the lockdown settings correct?")
                    raise

    def collect_finished(self, green):
        logging.getLogger('user_level_log').info("Data collection finished")

    def go_to_collect(self, timeout=180):
        self.wait_supervisor_ready()
        self.logger.debug("Sending supervisor to collect phase")
        self.supervisor_hwobj.go_collect()

        gevent.sleep(0.5)

        t0 = time.time()
        while True:

# TODO: This call return None !!!!
            super_state = self.supervisor_hwobj.get_state()
            super_state2 = self.supervisor_hwobj.current_state

            #self.logger.debug("Supervisor get_state() is %s" % super_state)
            #self.logger.debug("Supervisor current current_state is %s" % super_state2)
            #TODO: review, sometimes get_current_phase returns None 
            try:
                cphase = self.supervisor_hwobj.get_current_phase().upper()
                #self.logger.debug("Supervisor current phase is %s" % cphase)
            except:
                cphase = None
            
            if super_state == DevState.ON and cphase == "COLLECT":
                break
            if time.time() - t0 > timeout or self.aborted_by_user:
                msg = "Timeout sending supervisor to collect phase"
                self.logger.debug(msg)
                raise RuntimeError(msg)
            gevent.sleep(0.5)

        self.logger.debug("New supervisor phase is %s (Collect phase was requested)" % cphase)
        self._collecting = True

        return self.is_collect_phase()

    def is_collect_phase(self):
        self.logger.debug("In is_collect_phase method")
        try:
            return (self.supervisor_hwobj.get_current_phase().upper() == "COLLECT" and self._collecting == True ) # RB added the self._collecting, check if it works
            #return self.supervisor_hwobj.get_current_phase().upper() == "COLLECT" 
        except Exception as e:
            msg = "Cannot return current phase from supervisor. Please, restart MXCuBE."
            logging.getLogger('user_level_log').error(msg)
            raise Exception(msg)

    def go_to_sampleview(self, timeout=180):
        self.wait_supervisor_ready()
        self.logger.debug("Sending supervisor to sample view phase")
        self.close_fast_shutter()
        #if not self.supervisor_hwobj.is_fast_shutter_in_collect_position():
        self.close_safety_shutter()
            
        self.supervisor_hwobj.go_sample_view()

        gevent.sleep(0.5)

        t0 = time.time()
        while True:
            #TODO: review, some calls return None for get_current_phase()
            try:
                super_state = self.supervisor_hwobj.get_state()
                cphase = self.supervisor_hwobj.get_current_phase().upper()
            except:
                super_state = cphase = None
            if super_state != DevState.MOVING and cphase == "SAMPLE":
                break
            if time.time() - t0 > timeout:
                self.logger.debug("Timeout sending supervisor to sample view phase")
                break
            gevent.sleep(0.5)

        self.logger.debug("New supervisor phase is %s" % cphase)

        return self.is_sampleview_phase()

    def is_sampleview_phase(self):
        return self.supervisor_hwobj.get_current_phase().upper() == "SAMPLE"

    def wait_supervisor_ready(self, timeout=30):
        self.logger.debug("Waiting to supervisor ready")

        gevent.sleep(0.5)

        t0 = time.time()
        while True:
            super_state = self.supervisor_hwobj.get_state()
            if super_state == DevState.ON:
                break
            if time.time() - t0 > timeout:
                self.logger.debug("Timeout waiting for supervisor ready")
                raise RuntimeError("Supervisor cannot be operated (state %s)" % super_state)
                break
            self.logger.debug("Supervisor state is %s" % super_state)
            gevent.sleep(0.5)

        #
        #
        # START of 20210218: Lines only necessary for ni660 collects, remove when switching to pure meshct/ascanct scans
    def configure_ni(self, startang, total_dist):
        self.logger.debug(
            "Configuring NI660 with pars 0, %s, %s, 0, 1" %
            (startang, total_dist))
        self.cmd_ni_conf(0.0, startang, total_dist, 0, 1)

    def unconfigure_ni(self):
        self.cmd_ni_unconf()
        # END of lines for ni660 scans
        #
        #


    def open_safety_shutter(self):
        """ implements prepare_shutters in collect macro """

        # prepare ALL shutters

        if self.fastshut_hwobj.getState() != 0:
            self.fastshut_hwobj.close()

        if self.slowshut_hwobj.getState() != 1:
            self.slowshut_hwobj.open()

        if self.photonshut_hwobj.getState() != 1:
            self.photonshut_hwobj.open()

        if self.frontend_hwobj.getState() != 0:
            self.frontend_hwobj.open()

    def open_detector_cover(self):
        self.supervisor_hwobj.open_detector_cover()

    def open_fast_shutter_for_interal_trigger(self):
        # self.fastshut_hwobj.open()
        # this function is empty for ALBA. we are not opening the fast shutter.
        # on the contrary open_safety_shutter (equivalent to prepare_shutters in
        # original collect macro will first close the fast shutter and open the
        # other three
        if self.is_collect_phase():
            self.fastshut_hwobj.open()

    def close_fast_shutter(self):
        self.fastshut_hwobj.cmdOut()

    def close_safety_shutter(self):
        #  we will not close safety shutter during collections
        pass

    def close_detector_cover(self):
        #  we will not close detector cover during collections
        #  self.supervisor.close_detector_cover()
        pass

    def scan_delete_motor_data(self):
        self.scan_move_motor_names = []
        self.scan_velocities = {}
        self.scan_end_positions = {}
        self.scan_start_positions = {}
        self.scan_init_positions = {}

    def scan_delete_motor_data(self):
        self.scan_move_motor_names = []
        self.scan_velocities = {}
        self.scan_end_positions = {}
        self.scan_start_positions = {}
        self.scan_init_positions = {}

    def set_helical_pos(self, arg):
        """
        Descript. : 8 floats describe
        p1AlignmY, p1AlignmZ, p1CentrX, p1CentrY
        p2AlignmY, p2AlignmZ, p2CentrX, p2CentrY
        At XALOC, the phiz motor is not moved
        """
        self.logger.info('Setting helical motor positions')
        self.logger.info('Helical motor positions %s' % str(arg))
        try:
            for motorname in self.scan_all_motor_names: # phiy (omegax), phiz (omegaz), sampx (centx), sampy (centy)
                if arg["1"][motorname] != None: self.scan_start_positions[motorname] = arg["1"][motorname]
                else: self.logger.info('Helical motor %s not added, position is None' % arg["1"][motorname])
                if arg["2"][motorname] != None: self.scan_end_positions[motorname] = arg["2"][motorname]
                else: self.logger.info('Helical motor %s not added, position is None' % arg["2"][motorname])
        except Exception as e :
             self.logger.error('FAILED TO SET HELICAL MOTOR POSITIONS')
             self.data_collection_failed("Cannot interpret helical motor start/end positions") #TODO: collect_failed?? or self.data_collection_failed?
             raise Exception( e )

    def set_scan_move_motors(self, scan_start_pos, scan_end_pos, cutoff=0.005): #cutoff in microns
        self.logger.info('  Checking which motors should move for the helical data collection')
        for motorname in self.scan_all_motor_names:
            self.logger.info('    Motor    : %s' % motorname)
            self.logger.info('    Start pos: %.4f' % scan_start_pos[motorname])
            self.logger.info('    End pos  : %.4f' % scan_end_pos[motorname])
            self.logger.info('    Diff pos : %.4f' % math.fabs(scan_start_pos[motorname] - scan_end_pos[motorname]))
            if math.fabs(scan_start_pos[motorname] - scan_end_pos[motorname]) > cutoff and scan_start_pos[motorname] != None and scan_end_pos[motorname] != None:
                self.logger.info('    Add helical motor %s' % motorname)
                self.scan_move_motor_names.append(motorname)

    def calculate_scan_velocities(self, scan_start_pos, scan_end_pos, total_collection_time):
        self.logger.info('  Calculating the velocities for the helical data collection')
        self.logger.info('    Calculating the required velocities for the helical motors')
        scan_velocities = {}
        for motorname in self.scan_move_motor_names:
            self.logger.info('    Motor    : %s' % motorname)
            self.logger.info('    Start pos: %.4f' % scan_start_pos[motorname])
            self.logger.info('    End pos  : %.4f' % scan_end_pos[motorname])
            scan_velocities[motorname] = math.fabs ( scan_start_pos[motorname] - scan_end_pos[motorname] ) / total_collection_time
            self.logger.info('    Velocity : %.7f' % scan_velocities[motorname])

        return scan_velocities

    def check_scan_velocities(self, scan_velocities):
        self.logger.info('  Checking the velocities for the helical data collection')
        scan_motor_velo_keys = scan_velocities.keys()
        self.logger.info('    Helical motor list: %s' % scan_motor_velo_keys )
        scan_motor_too_fast = [] # list of motors that cannot go fast enough for the proposed scan
        scan_motor_too_slow = [] # list of motors that cannot go slow enough for the proposed scan
        for motorname in scan_motor_velo_keys:
            self.logger.info('    %7s velocity : %.4f' % (motorname, scan_velocities[motorname])  )
            if scan_velocities[motorname] < self.scan_motors_min_velocity[motorname]:
                scan_motor_too_slow.append(motorname)
            elif scan_velocities[motorname] > self.scan_motors_max_velocity[motorname]:
                scan_motor_too_fast.append(motorname)

        for motorname in scan_motor_too_fast:
            self.logger.error('Helical collection error: Stop data collection cause %s cant go fast enough. TIP reduce the range or increase the total data collection time' % motorname)
            logging.getLogger('user_level_log').error('Helical collection error: Stop data collection cause %s cant go fast enough.' % motorname)
            logging.getLogger('user_level_log').error('    TIP reduce the distance between starting and ending points, or increase the total data collection time' )
        for motorname in scan_motor_too_slow:
            self.logger.error('stop data collection cause %s cant go slow enough. TIP increase the range or reduce the total data collection time' % motorname)
            logging.getLogger('user_level_log').error('stop data collection cause %s cant go slow enough.' % motorname)
            logging.getLogger('user_level_log').error('    TIP increase the distance between starting and ending points, or reduce the total data collection time')

        return len(scan_motor_too_fast)+len(scan_motor_too_slow)

    def set_mesh_scan_parameters(self, num_lines, total_nb_frames, mesh_center_param, mesh_range_param):
        """
        sets the mesh scan parameters :
         - vertcal range
         - horizontal range
         - nb lines
         - nb frames per line
         - invert direction (boolean)  # NOT YET DONE
         """


        # num_lines are the number of vertical lines
        self.mesh_num_lines = num_lines
        self.mesh_total_nb_frames = total_nb_frames
        # mesh_range is a list of two values in fast en slow direction: the range is actually the total interval!
        self.mesh_range = mesh_range_param
        # mesh_center is a dictionary holding centered positions of motors
        self.mesh_center = mesh_center_param
        self.logger.debug( "mesh_num_lines       : %s" % self.mesh_num_lines )
        self.logger.debug( "mesh_total_nb_frames : %s" % self.mesh_total_nb_frames )
        self.logger.debug( "mesh_range           : %s" % self.mesh_range )
        self.logger.debug( "mesh_center          : %s" % self.mesh_center )


    def setMeshScanParameters(self, osc_seq, mesh_mxcube_fast_motor_name, mesh_mxcube_slow_motor_name, mesh_center, mesh_range):
        """
        Calculates velocity, starting and end position of the phiy (omegax) motor for each horizontal line scan
                   the step size for the vertical direction

            
                      mesh_scan_dict['fast_motor_start_pos'],
                      mesh_scan_dict['fast_motor_final_pos'],
                      mesh_scan_dict['fast_motor_nr_images'],
                      mesh_xaloc_slow_motor_name,
                      mesh_scan_dict['slow_motor_start_pos'],
                      mesh_scan_dict['slow_motor_final_pos'],
                      mesh_scan_dict['slow_motor_nr_images'],
./hardware_objects.xml/mini-diff.xml:  <gridDirection>{"fast": (1, 0), "slow": (0, -1)}</gridDirection>


        """

        #TODO: CHECK IF THE SPEED OF THE FAST MOTOR IS NOT TOO FAST!

        # mesh_center is a Centering_Point, which fails to work as a dictionary, following lines needed
        if not type(mesh_center) is dict:
            mesh_center = mesh_center.as_dict()

        mesh_vertical_discrete_step_size = 0
        self.logger.debug('setMeshScanParameters')

        beam_size = self.get_beam_size() # TODO get beam size
        self.logger.debug('\t beam size: %s' % str(beam_size))

        self.logger.debug('\t mesh_center: %s' % str(mesh_center))
        self.logger.debug('\t mesh_range: %s' % str(mesh_range))
        self.logger.debug('\t mesh_mxcube_fast_motor_name %s' % ( mesh_mxcube_fast_motor_name) )
        self.logger.debug('\t mesh_mxcube_slow_motor_name %s' % ( mesh_mxcube_slow_motor_name) )
        #self.logger.debug('\t mesh_center[%s]: %s' %
                #( mesh_mxcube_fast_motor_name, str(mesh_center[mesh_mxcube_fast_motor_name]) )
            #)
        #self.logger.debug('\t mesh_center[%s]: %s' %
                #( mesh_mxcube_slow_motor_name, str(mesh_center[mesh_mxcube_slow_motor_name]) )
            #)
        self.logger.debug('\t ( mesh_range[1] / 2 ) %s' % ( mesh_range[1] / 2 ) )
        #TODO: index 0 is fast??

        self.scan_start_positions[mesh_mxcube_fast_motor_name] = \
            mesh_center [ mesh_mxcube_fast_motor_name ] - ( mesh_range[0] / 2.0 ) - ( beam_size[0] / 2.0 )
        self.scan_start_positions[mesh_mxcube_slow_motor_name] = \
            mesh_center [ mesh_mxcube_slow_motor_name ] - ( mesh_range[1] / 2.0 )

        self.scan_end_positions[mesh_mxcube_fast_motor_name] = \
            self.scan_start_positions[mesh_mxcube_fast_motor_name] + mesh_range[0] + beam_size[0]
        # Add a beamsize to the slow motor
        self.scan_end_positions[mesh_mxcube_slow_motor_name] = \
            self.scan_start_positions[mesh_mxcube_slow_motor_name] + mesh_range[1]
        self.logger.debug('\t scan_start_positions: %s' % str(self.scan_start_positions))
        self.logger.debug('\t scan_end_positions: %s' % str(self.scan_end_positions))

        slow_motor_nr_images = self.mesh_num_lines
        fast_motor_nr_images = self.mesh_total_nb_frames / self.mesh_num_lines
        self.logger.debug('\t fast_motor_nr_images: %s' % str( fast_motor_nr_images ) )
        self.logger.debug('\t slow_motor_nr_images: %s' % str( slow_motor_nr_images ) )

        return fast_motor_nr_images, slow_motor_nr_images

    @task
    def _take_crystal_snapshot(self, filename):
        """
        Descript. :
        """
        if not self.is_sampleview_phase():
            self.go_to_sampleview()

        self.graphics_manager_hwobj.save_scene_snapshot(filename)
        self.logger.debug("Crystal snapshot saved (%s)" % filename)

    def set_energy(self, value):
        """
        Descript. : This is Synchronous to be able to calculate the resolution @ ALBA
        """
        #   program energy
        #   prepare detector for diffraction
        self.energy_hwobj.move_energy(value)
        logging.getLogger('user_level_log').warning("Setting beamline energy it can take a while, please be patient")
        self.energy_hwobj.wait_move_energy_done()

    def set_wavelength(self, value):
        """
        Descript. :
        """
        #   program energy
        #   prepare detector for diffraction
        self.energy_hwobj.move_wavelength(value)
        self.energy_hwobj.wait_move_wavelength_done()

    def get_energy(self):
        return self.energy_hwobj.get_energy()

    def set_transmission(self, value):
        """
        Descript. :
        """
        self.transmission_hwobj.set_value(value)

    def set_resolution(self, value,  energy=None):
        """
        Descript. : resolution is a motor in out system
        """
        # Current resolution non valid since depends on energy and detector distance!!
        #current_resolution = self.resolution_hwobj.getPosition()
        #self.logger.debug("Current resolution is %s, moving to %s" % (current_resolution, value))
        self.logger.debug("Moving resolution to %s" % value)

        if energy:
            # calculate the detector position to achieve the desired resolution
            _det_pos = get_dettaby(value, energy=energy)
            # calulate the corresponding resolution
            value = get_resolution(_det_pos, energy=energy)

        self.resolution_hwobj.move(value)

    def move_detector(self, value):
        self.detector_hwobj.move_distance(value)

    def stopCollect(self):
        """
           Apparently this method is called when the user aborts data collection
           overrides AbstractCollect.stopCollect

           TODO: stop the supervisor from changing state
        """
        self.logger.debug("ALBACollect stopCollect")
        self.aborted_by_user = True
        self.stop_collect()

    def stop_collect(self):
        """
        Stops data collection, either interrupted by user, or due to failure
        overrides AbstractCollect.stop_collect

        """
        start_wait = time.time()
        timeout = 100 #TODO set a more educated guess for the timeout, depending on the time needed for the scan
        while self.mxcube_sardanascan_running == True and (time.time() - start_wait) < timeout:
            time.sleep(0.01)
        if (time.time() - start_wait) > timeout:
            logging.getLogger('user_level_log').error("Timeout waiting for scan to stop. Is the Macroserver ok?")

        self.logger.debug("ALBACollect stop_collect")
        self.logger.info("  Stopping all motors")
        self.detector_hwobj.stop_collection()
        self.omega_hwobj.stop()

        self.logger.info("  Closing fast shutter")
        self.close_fast_shutter()

        for helmovemotorname in self.scan_move_motor_names:
            self.scan_motors_hwobj[helmovemotorname].stop()

        self._collecting = False

        #if self.data_collect_task is not None:
        #    self.data_collect_task.kill(block = False)


    @task
    def move_motors(self, motor_position_dict):
        """
        Descript. :
        """
        self.diffractometer_hwobj.move_motors(motor_position_dict)

    def create_file_directories(self):
        """
        Method create directories for raw files and processing files.
        Directories for xds, mosflm, ednaproc and autoproc
        """
        self.create_directories(
            self.current_dc_parameters['fileinfo']['directory'],
            self.current_dc_parameters['fileinfo']['process_directory']
            )

        # create processing directories for each post process
        for proc in ['xds', 'mosflm', 'ednaproc', 'autoproc']:
            self._create_proc_files_directory(proc)

    # The following methods are copied to improve error logging, the functionality is the same
    def create_directories(self, *args):
        """
        Descript. :
        """
        for directory in args:
            self.logger.debug('Creating directory: %s' % str(directory) )
            try:
                os.makedirs(directory)
            except OSError as e:
                import errno
                if e.errno != errno.EEXIST:
                    logging.getLogger('user_level_log').error('Error in making the directories, has the permission lockdown been setup properly?' )
                    raise

    def _create_proc_files_directory(self, proc_name):

        i = 1

        while True:
            _dirname = "%s_%s_%s_%d" % (
                proc_name,
                self.current_dc_parameters['fileinfo']['prefix'],
                self.current_dc_parameters['fileinfo']['run_number'],
                i)
            _directory = os.path.join(
                self.current_dc_parameters['fileinfo']['process_directory'],
                _dirname)
            if not os.path.exists(_directory):
                break
            i += 1

        try:
            self.logger.debug('Creating proc directory: %s' % _directory)
            self.create_directories(_directory)
            os.system("chmod -R 777 %s" % _directory)
        except Exception as e:
            msg = "Could not create directory %s, are the file permissions setup correctly?\n%s" % (_directory, str(e))
            self.logger.exception(msg)
            return

        # save directory names in current_dc_parameters. They will later be used
        #  by autoprocessing.
        key = "%s_dir" % proc_name
        self.current_dc_parameters[key] = _directory
        self.logger.debug("dc_pars[%s] = %s" % (key, _directory))
        return _directory

    def get_wavelength(self):
        """
        Descript. :
            Called to save wavelength in lims
        """
        if self.energy_hwobj is not None:
            return self.energy_hwobj.get_wavelength()

    def get_detector_distance(self):
        """
        Descript. :
            Called to save detector_distance in lims
        """
        if self.detector_hwobj is not None:
            return self.detector_hwobj.get_distance()

    def get_resolution(self):
        """
        Descript. :
            Called to save resolution in lims
        """
        if self.resolution_hwobj is not None:
            return self.resolution_hwobj.getPosition()

    def get_transmission(self):
        """
        Descript. :
            Called to save transmission in lims
        """
        if self.transmission_hwobj is not None:
            return self.transmission_hwobj.getAttFactor()

    def get_undulators_gaps(self):
        """
        Descript. : return triplet with gaps. In our case we have one gap,
                    others are 0
        """
        # TODO
        try:
            if self.chan_undulator_gap:
                und_gaps = self.chan_undulator_gap.getValue()
                if type(und_gaps) in (list, tuple):
                    return und_gaps
                else:
                    return (und_gaps)
        except BaseException as e:
            self.logger.debug("Get undulator gaps error\n%s" % str(e))
            pass
        return {}

    def get_beam_size(self):
        """
        Descript. :
        """
        if self.beam_info_hwobj is not None:
            return self.beam_info_hwobj.get_beam_size()

    def get_slit_gaps(self):
        """
        Descript. :
        """
        if self.beam_info_hwobj is not None:
            return self.beam_info_hwobj.get_slits_gap()
        return None, None

    def get_beam_shape(self):
        """
        Descript. :
        """
        if self.beam_info_hwobj is not None:
            return self.beam_info_hwobj.get_beam_shape()

    def get_measured_intensity(self):
        """
        Descript. :
        """
        if self.flux_hwobj is not None:
            return self.flux_hwobj.get_flux()

    def get_machine_current(self):
        """
        Descript. :
        """
        if self.machine_info_hwobj:
            return self.machine_info_hwobj.get_current()
        else:
            return 0

    def get_machine_message(self):
        """
        Descript. :
        """
        if self.machine_info_hwobj:
            return self.machine_info_hwobj.get_message()
        else:
            return ''
    # TODO: implement fill mode
    def get_machine_fill_mode(self):
        """
        Descript. :
        """
        if self.machine_info_hwobj:
            return "FillMode not/impl"
        else:
            return ''

    def get_flux(self):
        """
        Descript. :
        """
        return self.get_measured_intensity()

    def trigger_auto_processing(self, event, frame):
        if event == "after":
            dc_pars = self.current_dc_parameters
            self.autoprocessing_hwobj.trigger_auto_processing(dc_pars)

    # TODO: Copied from EMBL, need to be evaluated
    def update_lims_with_workflow(self, workflow_id, grid_snapshot_filename):
        """Updates collection with information about workflow

        :param workflow_id: workflow id
        :type workflow_id: int
        :param grid_snapshot_filename: grid snapshot file path
        :type grid_snapshot_filename: string
        """
        if self.lims_client_hwobj is not None:
            try:
                self.current_dc_parameters["workflow_id"] = workflow_id
                if grid_snapshot_filename:
                    self.current_dc_parameters["xtalSnapshotFullPath3"] =\
                        grid_snapshot_filename
                self.lims_client_hwobj.update_data_collection(
                    self.current_dc_parameters)
            except Exception as e:
                logging.getLogger("HWR").exception(
                    "Could not store data collection into ISPyB\n%s" % e)


def test_hwo(hwo):
    print("Energy: ", hwo.get_energy())
    print("Transmission: ", hwo.get_transmission())
    print("Resolution: ", hwo.get_resolution())
    print("Shutters (ready for collect): ", hwo.check_shutters())
    print("Supervisor(collect phase): ", hwo.is_collect_phase())

    print("Flux ", hwo.get_flux())
    print("Kappa ", hwo.kappapos_chan.getValue())
    print("Phi ", hwo.phipos_chan.getValue())
