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

        self.cmd_ni_conf = None
        self.cmd_ni_unconf = None

        self.chan_kappa_pos = None
        self.chan_phi_pos = None

        self.chan_undulator_gap = None

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

        self.mesh_range = []
        self.mesh_center = {}
        self.mesh_num_lines = None
        self.total_nb_frames = None

#        self.saved_omega_velocity = None

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

        self.cmd_ni_conf = self.getCommandObject("ni_configure")
        self.cmd_ni_unconf = self.getCommandObject("ni_unconfigure")

        self.chan_kappa_pos = self.getChannelObject("kappapos")
        self.chan_phi_pos = self.getChannelObject("phipos")

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
        self.omega_init_vel = self.omega_hwobj.get_velocity()
        self.logger.info('Inital motor velocities dict %s' % str(self.scan_init_velocities) )
        self.logger.info('Inital omega velocity value %.1f' % self.omega_init_vel )

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
        # Save omega initial position to be recovered after collection (cleanup).
        self.omega_init_pos = omega_pos
        exp_time = osc_seq['exposure_time']
        nb_images = osc_seq['number_of_images']
        sweep_nb_images = nb_images
        total_collection_time = exp_time * nb_images

        self.logger.info('%s' % osc_seq)
        
        self.autoprocessing_hwobj.create_input_files(self.current_dc_parameters)

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
            
            self.set_scan_move_motors(self.scan_start_positions, self.scan_end_positions)
            self.scan_velocities = self.calculate_scan_velocities(
                                      self.scan_start_positions, self.scan_end_positions, total_collection_time
                                                                 )
            self.logger.info('Preliminary helical setup completed')
        elif exp_type == "Mesh": 
            # Mesh scans are fast in columns (phiz/omegaz), slow in rows (phiy/omegax). 
            # First column start from min omegaz to max omegaz. Rows are collected from min omegax to max omegax.
            # mesh_range is a list with two values, the first value represents the horizontal range, 
            #                                       the second value represents the vertical line scan range
            # number_of_lines represents the number of line scans, ie the number of steps in the horizontal direction
            # number_of_images represents the number of images to be taken for each line scan
            self.scan_move_motor_names = [self.mesh_scan_line_motor_name]
            self.mesh_num_lines = osc_seq['number_of_lines']
            self.mesh_range = osc_seq['mesh_range']
            if not osc_seq['number_of_images'] % self.mesh_num_lines:
                mesh_nb_frames_per_line = osc_seq['number_of_images'] / self.mesh_num_lines 
                sweep_nb_images = mesh_nb_frames_per_line
                mesh_line_exposure_time = mesh_nb_frames_per_line * exp_time
            else: 
                msg = 'Inconsistent total number of images and number of lines in Mesh scan'
                self.data_collection_failed(msg)
                raise Exception( msg )
            
            self.logger.debug("Running a raster collection ()")
            self.logger.debug(
                "\tnumber of lines are: %s" %
                self.mesh_num_lines)
            self.logger.debug(
                "\ttotal nb of frames: %s" % 
                                 (mesh_nb_frames_per_line * self.mesh_num_lines)
                             )
            self.logger.debug("\tmesh range[0] : %s" % self.mesh_range[0])
            mesh_vertical_discrete_step_size = self.setMeshScanParameters(
                                                 self.mesh_num_lines, mesh_nb_frames_per_line, 
                                                 self.mesh_range, mesh_line_exposure_time
                                               ) 
            # For mesh scans, the start parameter represents the center of the mesh
            omega_pos = omega_pos - ( mesh_nb_frames_per_line * img_range / 2.0 )
        else:
            self.logger.debug( "Running a collect (STANDARD)" )

        if self.check_scan_velocities( self.scan_velocities ): # There are motors that cant work at required speed
            msg = 'Cant reach the required velocities'
            self.data_collection_failed( msg )
            raise Exception( msg )
            
        try:
            ready = self.prepare_acquisition()
            # RB init_pos and final_pos include the ramp up and run out range for omega
            init_pos, final_pos, total_dist, omega_speed = self.calc_omega_scan_values(
                                                                omega_pos, sweep_nb_images 
                                                           )
        except Exception as e:
            self.logger.error("Prepare_acquisition failed")
            self.logger.error('error %s' % str(e) )
            logging.getLogger('user_level_log').error('error %s' % repr( e ) )
            self.data_collection_failed( "Prepare_acquisition failed" )
            raise Exception( e )


        self.logger.debug('  Sweep parameters omega: init %s start %s total dist %s speed %s' % (init_pos, omega_pos, total_dist, omega_speed ) )
            
        if not ready:
            self.data_collection_failed( msg )
            self.stop_collect() # This is the only place this function is called, defined in AbstractCollect, is it do anything
            raise Exception( msg )
            
        # for progressBar brick
        self.emit("progressInit", "Collection", osc_seq['number_of_images'])

        self.emit("collectStarted", (self.owner, 1))


        if exp_type == 'OSC' or (exp_type == 'Characterization' and nb_images == 1) or exp_type == 'Helical':
            self.collect_prepare_omega(init_pos)
            self.prepare_collection(
                                      start_angle=omega_pos,
                                      nb_images=nb_images,
                                      first_image_no=first_image_no,
                                      img_range = img_range,
                                      exp_time = exp_time,
                                      omega_speed = omega_speed,
                                      det_trigger = self.current_dc_parameters['detector_mode'][0]
                                   )
            self.detector_hwobj.start_collection()
            #TODO: set a try except to capture aborted data collections, if necessary
            self.collect_images(omega_speed, omega_pos, final_pos, nb_images, first_image_no)
        elif exp_type == 'Characterization' and nb_images > 1:   # image one by one
            for imgno in range(nb_images):
                self.collect_prepare_omega(init_pos) # init_pos is calculated in prepare_acquisition but needs to be updated 
                self.prepare_collection(
                                          start_angle=omega_pos, nb_images=1, first_image_no=first_image_no,
                                          img_range = img_range,
                                          exp_time = exp_time,
                                          omega_speed = omega_speed,
                                          det_trigger = self.current_dc_parameters['detector_mode'][0]
                                       )
                self.detector_hwobj.start_collection()
                self.collect_images(omega_speed, omega_pos, final_pos, 1, first_image_no)
                first_image_no += 1
                omega_pos += 90
                init_pos, final_pos, total_dist, omega_speed = self.calc_omega_scan_values( omega_pos, nb_images )
                
        elif exp_type == 'Mesh':   # combine all collections
            self.logger.debug("Running a raster collection")
            self.init_mesh_scan()
            # final_pos is end of omega range (not including safedelta)
            if omega_speed == 0: self.current_dc_parameters['detector_mode'] = ['INTERNAL_TRIGGER']
            for lineno in range(self.mesh_num_lines):
                self.collect_prepare_omega(init_pos)
                self.logger.debug("\t line %s out of %s" % ( lineno+1, self.mesh_num_lines ) )
                #TODO move omegax/phiy to starting position of collection (OR is this done in the MxCube sequence somewhere???
                #TODO fix the naming of the files
                self.logger.debug("\t omega_pos %s mesh_nb_frames %s first image %s" % 
                       ( omega_pos, mesh_nb_frames_per_line, first_image_no ) )
                self.prepare_collection(
                                           start_angle=omega_pos, nb_images=sweep_nb_images, first_image_no=first_image_no,
                                           img_range = img_range,
                                           exp_time = exp_time,
                                           omega_speed = omega_speed,
                                           det_trigger = self.current_dc_parameters['detector_mode'][0]
                                       )
                self.detector_hwobj.start_collection()
                self.collect_images(omega_speed, omega_pos, final_pos, 1, first_image_no)
                first_image_no += mesh_nb_frames_per_line
                #TODO move omegaz/phiz by the mesh_vertical_discrete_step_size
                #self._motor_persistently_move( self.scan_motors_hwobj[self.mesh_scan_discrete_motor_name], mesh_vertical_discrete_step_size, 'REL' )
                self.scan_motors_hwobj[self.mesh_scan_discrete_motor_name].moveRelative( mesh_vertical_discrete_step_size )
                #TODO invert the direction of the collection
                self.logger.debug('  scan_start_positions before swap= %s' % self.scan_start_positions )
                self.logger.debug('  scan_end_positions before swap= %s' % self.scan_end_positions )
                # setup the end positions so the motor will move the other way for the next sweep
                dummy = self.scan_end_positions[self.mesh_scan_line_motor_name]
                self.scan_end_positions[self.mesh_scan_line_motor_name] = self.scan_start_positions[self.mesh_scan_line_motor_name]
                self.scan_start_positions[self.mesh_scan_line_motor_name] = dummy
                self.logger.debug('  scan_start_positions after swap= %s' % self.scan_start_positions )
                self.logger.debug('  scan_end_positions after swap= %s' % self.scan_end_positions )
                
            self.finalize_mesh_scan()

        #TODO: collection_finished should only be called in case of success, check if this is the case
        self.collection_finished()

                
    def collect_images(self, omega_speed, start_pos, final_pos, nb_images, first_image_no):
        """
           Run a single wedge. 
           Start position is the omega position where images should be collected. 
           It is assumed omega is already at the initial position, which comes before the start position
               and allows the mechanics to get up to speed
        """
        self.logger.info("collect_images: Collecting images, by moving omega to %s" % final_pos)
        total_time = (final_pos - self.omega_hwobj.getPosition() ) / omega_speed 
        omega_ramp_time = math.fabs ( ( self.omega_hwobj.getPosition() - start_pos ) / omega_speed )
        self.logger.info("    Total collection time = %s" % total_time)
        if omega_speed != 0:
            try: 
                #self._motor_persistently_move(self.omega_hwobj, final_pos)
                self.omega_hwobj.move( final_pos )
            except Exception as e:
                self.data_collection_failed('Initial omega position could not be reached')
                raise Exception(e)
        else:
            try:
                self.open_fast_shutter_for_interal_trigger()
            except Exception as e:
                self.data_collection_failed('Cant open safety shutter for omega speed %.6f' % omega_speed)
                raise Exception(e)

        time.sleep(omega_ramp_time)
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
                self.logger.error('Cannot move the helical motor %s to its end position' % scanmovemotorname)
                self.data_collection_failed('Cannot move the helical motor %s to its end position' % scanmovemotorname)
                raise Exception(e)
        self.wait_collection_done(nb_images, first_image_no, total_time)

    def data_collection_failed(self, failed_msg):
        self.logger.info("ALBACollect data_collection_failed")
        logging.getLogger('user_level_log').error(failed_msg)
        
        self.logger.debug("  Initiating recovery sequence")
        self.stopCollect() # is it necessary to call this, or is it called through events? If it is not, this method is not necessary

        #AbstractCollect.data_collection_failed() # there is no data_collection_failed in AbstractCollect
        
    def prepare_acquisition(self):
        """
          checks shutters, moves supervisor to collect phase, prepares detector and calculates omega start values
          omega_start_pos should be the value at which the collection starts
        """

        fileinfo = self.current_dc_parameters['fileinfo']

        basedir = fileinfo['directory']

        # Save omega velocity
        # self.saved_omega_velocity = self.omega_hwobj.get_velocity()
        # Better use it nominal velocity (to be properly defined in Sardana motor)
        # Ensure omega has its nominal velocity to go to the initial position
        # We have to ensure omega is not moving when setting the velocity

        # create directories if needed
        self.check_directory(basedir)

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
        self.diffractometer_hwobj.wait_device_ready(timeout=10)
        self.logger.info("Diffractometer is now ready.")

        # go to collect phase
        if not self.is_collect_phase():
            self.logger.info("Supervisor not in collect phase, asking to go...")
            success = self.go_to_collect()
            if not success:
                msg = "Supervisor cannot set COLLECT phase"
                self.logger.error(msg)
                return False, msg

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

        return ( detok )

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

    def collect_prepare_omega(self, omega_pos):
        '''
            Prepares omega for sweep. Calculates the margins at start and end position
            Moves omega to the initial position
        '''

        try:
            #self._motor_persistently_set_velocity(self.omega_hwobj, 60) # make sure omega is max speed before moving to start pos
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

        return 
 
    def prepare_collection(self, start_angle, nb_images, first_image_no, img_range, exp_time, omega_speed, det_trigger):
        """
          sets up data collection. 
          the start_angle is the position of omega when the first image starts to be collected
        """

        total_dist = nb_images * img_range
        
        self.write_image_headers(start_angle)
        
        self.logger.info("nb_images: %s / img_range: %s / exp_time: %s /"
                                      " total_distance: %s / speed: %s" %
                                      (nb_images, img_range, exp_time, total_dist,
                                       omega_speed))

        self.detector_hwobj.prepare_collection(nb_images, first_image_no)

        # TODO: Increase timeout: 
        self.omega_hwobj.wait_end_of_move(timeout=40)

        self.logger.info(
            "Omega now at %s" %
            self.omega_hwobj.getPosition())

        # program omega speed depending on exposure time

        self.logger.info("Setting omega velocity to %s and is moving %s" % (omega_speed, self.omega_hwobj.is_moving() ) )
        self.logger.debug('  Omega motor state = %s' % self.omega_hwobj.getState() )
        #while self.omega_hwobj.getState() == 3:
        #            time.sleep(0.1)
        self.logger.debug('  Omega motor state = %s' % self.omega_hwobj.getState() )
        try: 
            #self._motor_persistently_set_velocity(self.omega_hwobj, omega_speed)
            self.omega_hwobj.set_velocity( omega_speed )
        except Exception as e: 
            self.data_collection_failed("Cannot set the omega velocity to %s" % omega_speed) 
            raise Exception( e )
        
        for scanmovemotorname in self.scan_move_motor_names:
            self.logger.info("Setting %s velocity to %.4f" % (scanmovemotorname, self.scan_velocities[scanmovemotorname]) )
            try:
                #self._motor_persistently_set_velocity(self.scan_motors_hwobj[scanmovemotorname], self.scan_velocities[scanmovemotorname])
                self.scan_motors_hwobj[scanmovemotorname].set_velocity( self.scan_velocities[scanmovemotorname] )
            except Exception as e:
                self.logger.info("Cant set the scan velocity of motor %s" % scanmovemotorname )
                self.data_collection_failed("Cant set the scan velocity of motor %s" % scanmovemotorname) 
                raise Exception( e )
                
        self.detector_hwobj.set_detector_mode(det_trigger)
        if omega_speed != 0:
            self.configure_ni(start_angle, total_dist)
        
    def write_image_headers(self, start_angle):
        fileinfo = self.current_dc_parameters['fileinfo']
        basedir = fileinfo['directory']

        exp_type = self.current_dc_parameters['experiment_type']
        osc_seq = self.current_dc_parameters['oscillation_sequence'][0]

        nb_images = osc_seq['number_of_images']
        # start_angle = osc_seq['start']

        img_range = osc_seq['range']

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

    def wait_collection_done(self, nb_images, first_image_no, total_time):

        # Deprecated
        # osc_seq = self.current_dc_parameters['oscillation_sequence'][0]
        # first_image_no = osc_seq['start_image_number']
        # nb_images = osc_seq['number_of_images']
        last_image_no = first_image_no + nb_images - 1
        self.logger.info("  Total acquisition time = %.2f s" % total_time )
        self.logger.info("  last_image_no          = %d" % last_image_no )

        if nb_images > 1:
            self.wait_save_image(first_image_no)
        self.omega_hwobj.wait_end_of_move(timeout=720)
        self.wait_save_image(last_image_no)
        self.logger.info("  wait_collection_done last image found" )
        for motorname in self.scan_move_motor_names:
            self.logger.info("     Motor %s position = %.2f" % ( motorname, self.scan_motors_hwobj[motorname].getPosition() ) )

        # Wait for omega to stop moving, it continues further than necessary
        self.omega_hwobj.wait_end_of_move(timeout=40)
        # Wait for any other motors to stop moving
        for motorname in self.scan_move_motor_names:
            self.scan_motors_hwobj[motorname].wait_end_of_move(timeout=40)

        # Make sure the detector is readyddddd (in stand by and not moving)
        self.detector_hwobj.wait_ready()

    def wait_save_image(self, frame_number, timeout=25):

        fileinfo = self.current_dc_parameters['fileinfo']
        basedir = fileinfo['directory']
        template = fileinfo['template']

        filename = template % frame_number
        full_path = os.path.join(basedir, filename)

        start_wait = time.time()

        self.logger.debug("   waiting for image on disk: %s" % full_path)

        while not os.path.exists(full_path) and self.aborted_by_user: 
            # TODO: review next line for NFTS related issues.
            dirlist = os.listdir(basedir)  # forces directory flush ?
            if (time.time() - start_wait) > timeout:
                self.logger.debug("   giving up waiting for image")
                cam_state = self.detector_hwobj.chan_cam_state.getValue()
                acq_status = self.detector_hwobj.chan_acq_status.getValue()
                fault_error = self.detector_hwobj.chan_acq_status_fault_error.getValue()
                self.detector_hwobj.get_saving_statistics()
                msg = "cam_state = {}, acq_status = {}, fault_error = {}".format(
                    cam_state, acq_status, fault_error)
                logging.getLogger('user_level_log').error("Incompleted data collection")
                logging.getLogger('user_level_log').error(msg)
                raise RuntimeError(msg)
                #return False
            #logging.getLogger('user_level_log').error("self._collecting %s" % str(self._collecting) )
            time.sleep(0.2)

        self.detector_hwobj.get_saving_statistics()

        # self.last_saved_image = fullpath

        # generate thumbnails
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

        return True

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
                #self._motor_persistently_set_velocity( self.omega_hwobj, self.omega_init_vel )
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
            #self.detector_hwobj.stop_collection() 
            #self._motor_persistently_move(self.omega_hwobj, self.omega_init_pos) 
            self.omega_hwobj.move( self.omega_init_pos ) 
        except:
            self.logger.error("Omega needs to be stopped before restoring initial position")
            self.omega_hwobj.stop()
            self.omega_hwobj.move(self.omega_init_pos)

        self.scan_delete_motor_data()
        self.unconfigure_ni()
        self.fastshut_hwobj.close() # RB: not sure if it closes when unconfiguring it, just in case

        self.logger.debug("ALBA data_collection_cleanup finished")

    def check_directory(self, basedir):
        if not os.path.exists(basedir):
            try:
                os.makedirs(basedir)
            except OSError as e:
                import errno
                if e.errno != errno.EEXIST:
                    raise

    def collect_finished(self, green):
        logging.getLogger('user_level_log').info("Data collection finished")

# RB 20200915: Jordi didnt have the override of collect_failed in his code.
#    def collect_failed(self, par):
#        self.logger.exception("Data collection failed")
#        self.current_dc_parameters["status"] = 'failed'
#        exc_type, exc_value, exc_tb = sys.exc_info()
#        failed_msg = 'Data collection failed!\n%s' % exc_value
#        self.emit("collectOscillationFailed", (self.owner, False, failed_msg,
#                                               self.current_dc_parameters.get('collection_id'), 1))

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


    def configure_ni(self, startang, total_dist):
        self.logger.debug(
            "Configuring NI660 with pars 0, %s, %s, 0, 1" %
            (startang, total_dist))
        self.cmd_ni_conf(0.0, startang, total_dist, 0, 1)

    def unconfigure_ni(self):
        self.cmd_ni_unconf()

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
        Supersedes AbstractCollect set_mesh_scan_paramters
        """
        self.logger.info('ALBACollect set_mesh_scan_parameters')
        self.logger.info('\t num_lines        : %s' % num_lines)
        self.logger.info('\t total_nb_frames   : %s' % total_nb_frames)
        self.logger.info('\t mesh_center_param : %s' % mesh_center_param)
        self.logger.info('\t mesh_range_param  : %s' % mesh_range_param)
        #self.mesh_center = mesh_center_param
        #self.mesh_range = mesh_range_param
        #self.mesh_num_lines = num_lines
        #self.total_nb_frames = total_nb_frames

    def init_mesh_scan(self):
        for motorname in self.scan_start_positions:
            self.scan_motors_hwobj[motorname].move( self.scan_start_positions[motorname] )
        
    def finalize_mesh_scan(self):
        for motorname in self.scan_init_positions:
            self.scan_motors_hwobj[motorname].move( self.scan_init_positions[motorname] )
    
    def update_lims_with_workflow(self, workflow_id, grid_snapshot_filename):
        pass
    
    def setMeshScanParameters(self, num_lines, num_images_per_line, mesh_range, collect_time_per_line):
        """
        Calculates velocity, starting and end position of the phiy (omegax) motor for each horizontal line scan
                   the step size for the vertical direction
                   
            # Mesh scans are fast in columns (phiz/omegaz), slow in rows (phiy/omegax). 
            # First column start from min omegaz to max omegaz. Rows are collected from min omegax to max omegax.
            # mesh_range is a list with two values, the first value represents the horizontal range, 
            #                                       the second value represents the vertical line scan range
            # number_of_lines represents the number of line scans, ie the number of steps in the horizontal direction
            # number_of_images represents the number of images to be taken for each line scan
            
        """
        mesh_vertical_discrete_step_size = 0
        self.logger.debug('setMeshScanParameters')
        self.logger.debug('\t num_lines %s num_images_per_line %s mesh_range %s collect_time_per_line %s' % 
                             (num_lines, num_images_per_line, mesh_range, collect_time_per_line)
                         )
        beam_size = self.get_beam_size() # TODO get beam size
        self.logger.debug('\t beam size: %s' % str(beam_size))

        self.logger.debug('\t mesh_center: %s' % str(self.mesh_center))
        #self.logger.debug('\t mesh_center[phiy]: %s' % str(self.mesh_center['phiy']) )
        #self.logger.debug('\t mesh_center[phiy]: %s' % str(self.mesh_center[self.mesh_scan_line_motor_name]) )
        self.logger.debug('\t self.mesh_scan_line_motor_name: %s' % self.mesh_scan_line_motor_name)
        self.logger.debug('\t self.mesh_scan_discrete_motor_name: %s' % self.mesh_scan_discrete_motor_name )
        self.logger.debug('\t self.scan_motors_hwobj[self.mesh_scan_line_motor_name]: %s' % self.scan_motors_hwobj[self.mesh_scan_line_motor_name])
        self.logger.debug('\t ( mesh_range[1] / 2 ) %s' % ( mesh_range[1] / 2 ) )
        self.scan_start_positions[self.mesh_scan_line_motor_name] = \
            self.scan_motors_hwobj[self.mesh_scan_line_motor_name].getPosition() + ( mesh_range[1] / 2.0 ) + ( beam_size[1] / 2.0 )
        self.scan_start_positions[self.mesh_scan_discrete_motor_name] = \
            self.scan_motors_hwobj[self.mesh_scan_discrete_motor_name].getPosition() - ( mesh_range[0] / 2 ) 
        self.logger.debug('\t scan_start_positions: %s' % str(self.scan_start_positions))
        #self.scan_start_positions[self.mesh_scan_line_motor_name] = \
        #               self.mesh_center[self.mesh_scan_line_motor_name] - ( self.mesh_range[1] / 2 )
        #self.scan_start_positions[self.mesh_scan_discrete_motor_name] = \
        #               self.mesh_center[self.mesh_discrete_line_motor_name] - ( self.mesh_range[0] / 2 ) + ( beam_size[0] / 2)
                       
        self.logger.debug('\t scan_start_positions: %s' % str(self.scan_start_positions))
        self.scan_end_positions[self.mesh_scan_line_motor_name] = \
                       self.scan_start_positions[self.mesh_scan_line_motor_name] - mesh_range[1] - beam_size[1]
        self.logger.debug('\t scan_end_positions: %s' % str(self.scan_end_positions))
        if num_lines > 1: mesh_vertical_discrete_step_size = mesh_range[0] / ( num_lines - 1 )
        self.logger.debug('\t mesh_vertical_discrete_step_size: %s' % str(mesh_vertical_discrete_step_size))

        self.scan_velocities[self.mesh_scan_line_motor_name] = math.fabs( 
                                                                  ( self.scan_end_positions[self.mesh_scan_line_motor_name] - 
                                                                    self.scan_start_positions[self.mesh_scan_line_motor_name] ) 
                                                                      / collect_time_per_line )
        self.logger.debug('\t scan_velocities: %s' % str(self.scan_velocities))

        return mesh_vertical_discrete_step_size 

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
        self.stop_collect() # What does this do???

    def stop_collect(self):
        """
        Stops data collection, either interrupted by user, or due to failure
        overrides AbstractCollect.stop_collect
        """
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
            self.current_dc_parameters['fileinfo']['process_directory'])

        # create processing directories for each post process
        for proc in ['xds', 'mosflm', 'ednaproc', 'autoproc']:
            self._create_proc_files_directory(proc)

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
            msg = "Could not create directory %s\n%s" % (_directory, str(e))
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

    #def _motor_persistently_set_velocity(self, motor_hwobj, new_velo, Timeout = 10):
        #starttime = time.time()
        #if new_velo is None:
            #self.logger.debug('Could not set motor velocity to %s' % 
                                      #new_velo ) 
            #return
        #while time.time()-starttime < Timeout:
            #try: 
                ##self.logger.debug('Setting motor velocity to %s, motor state is %s' % 
                ##                      ( new_velo, motor_hwobj.getState() ) 
                ##                 )
                #if motor_hwobj.getState() == 3: # TODO: remove all 3 and 5 and replace by AbstractMotor import MotorStates
                    #motor_hwobj.set_velocity(new_velo)
                    #self.logger.debug('Succesfully set motor velocity to %s, motor state is %s' % 
                                         #( new_velo, motor_hwobj.getState() ) 
                                     #)
                    #break
            #except:
                #self.logger.debug('Could not set motor velocity to %s, motor state is %s, trying again' % 
                                      #( new_velo, motor_hwobj.getState() ) 
                                 #)
                #time.sleep(0.2)                                 
        #else:
            #self.logger.error('Could not set motor velocity to %s, motor state is %s Giving up' % 
                                     #( new_velo, motor_hwobj.getState() ) 
                             #)
          
    #def _motor_persistently_syncmove(self, motor_hwobj, new_pos, mode = 'ABS', Timeout = 10):
        #starttime = time.time()
        #if new_pos is None:
            #self.logger.debug('Could not set motor position to %s' % 
                                      #new_pos ) 
            #return
        #while time.time()-starttime < Timeout:
            #try: 
                ##self.logger.debug('Setting motor position to %s, motor state is %s' % 
                ##                      ( new_pos, motor_hwobj.getState() ) 
                ##                 )
                #if motor_hwobj.getState() == 3:# TODO: remove all 3 and 5 and replace by AbstractMotor import MotorStates
                    #if mode == 'ABS':
                        #motor_hwobj.syncMove(new_pos)
                    #else: 
                        #motor_hwobj.syncMoveRelative(new_pos)
                    #self.logger.info('Succesfully set motor position to %s, motor state is %s' % 
                                          #( new_pos, motor_hwobj.getState() ) 
                                     #)
                    #break
            #except:
                #self.logger.error('Could not set motor velocity to %s, motor state is %s, trying again' % 
                                      #( new_pos, motor_hwobj.getState() ) 
                                 #)
                #time.sleep(0.2)
        #else: 
            #self.logger.error('Could not set motor velocity to %s, motor state is %s Giving up' % 
                                     #( new_pos, motor_hwobj.getState() ) 
                             #)

    #def _motor_persistently_move(self, motor_hwobj, new_pos, mode = 'ABS', Timeout = 10):
        #starttime = time.time()
        #if new_pos is None:
            #self.logger.debug('Could not set motor position to %s' % 
                                      #new_pos ) 
            #return
        #while time.time()-starttime < Timeout:
            #try: 
                ##self.logger.debug('Setting motor position to %s, motor state is %s' % 
                ##                      ( new_pos, motor_hwobj.getState() ) 
                ##                 )
                #if motor_hwobj.getState() == 3:# TODO: remove all 3 and 5 and replace by AbstractMotor import MotorStates
                    #if mode == 'ABS':
                         #motor_hwobj.move(new_pos)
                    #else: 
                         #motor_hwobj.moveRelative(new_pos)
                    #self.logger.info('Succesfully set motor position to %s, motor state is %s' % 
                                          #( new_pos, motor_hwobj.getState() ) 
                                     #)
                    #break
            #except:
                #self.logger.error('Could not set motor velocity to %s, motor state is %s, trying again' % 
                                      #( new_pos, motor_hwobj.getState() ) 
                                 #)
                #time.sleep(0.2)
        #else: 
            #self.logger.error('Could not set motor velocity to %s, motor state is %s Giving up' % 
                                      #( new_pos, motor_hwobj.getState() ) 
                             #)

# The following methods are copied to improve error logging, the functionality is the same
    def create_directories(self, *args):
        """
        Descript. :
        """
        for directory in args:
            self.logger.debug('Creating directory: %s' % directory)
            try:
                os.makedirs(directory)
            except OSError as e:
                import errno
                if e.errno != errno.EEXIST:
                    logging.getLogger('user_level_log').error('Error in making the directories, has the permission lockdown been setup properly?' )
                    raise
            
def test_hwo(hwo):
    print("Energy: ", hwo.get_energy())
    print("Transmission: ", hwo.get_transmission())
    print("Resolution: ", hwo.get_resolution())
    print("Shutters (ready for collect): ", hwo.check_shutters())
    print("Supervisor(collect phase): ", hwo.is_collect_phase())

    print("Flux ", hwo.get_flux())
    print("Kappa ", hwo.kappapos_chan.getValue())
    print("Phi ", hwo.phipos_chan.getValue())
