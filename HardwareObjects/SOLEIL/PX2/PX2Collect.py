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

import os
import logging
import gevent
from HardwareRepository.TaskUtils import task
from HardwareRepository.BaseHardwareObjects import HardwareObject
from AbstractCollect import AbstractCollect
import sys
import traceback
import time 

from omega_scan import omega_scan
from inverse_scan import inverse_scan
from reference_images import reference_images
from helical_scan import helical_scan
from fluorescence_spectrum import fluorescence_spectrum
from energy_scan import energy_scan
#from xray_centring import xray_centring
from raster_scan import raster_scan
from nested_helical_acquisition import nested_helical_acquisition
from tomography import tomography
from film import film

from slits import slits1

__credits__ = ["Synchrotron SOLEIL"]
__version__ = "2.3."
__category__ = "General"

class PX2Collect(AbstractCollect):
    """Main data collection class. Inherited from AbstractCollect.
       Collection is done by setting collection parameters and
       executing collect command
    """
    
    experiment_types = ['omega_scan',
                        'reference_images',
                        'inverse_scan',
                        'mad',
                        'helical_scan',
                        'xrf_spectrum',
                        'energy_scan',
                        'raster_scan',
                        'nested_helical_acquisition',
                        'tomography',
                        'film',
                        'optical_centering']
                        
        
    #experiment_types = ['OSC', 
                        #'Collect - Multiwedge', 
                        #'Helical', 
                        #'Mesh', 
                        #'energy_scan', 
                        #'xrf_spectrum', 
                        #'neha'
                        
    def __init__(self, name):
        """
        :param name: name of the object
        :type name: string
        """
        
        AbstractCollect.__init__(self, name)
        
        self.current_dc_parameters = None
        self.osc_id = None
        self.owner = None
        self.aborted_by_user = None
        
        self.slits1 = slits1()
        self.log = logging.getLogger('HWR')
        
    def init(self):
        """Main init method"""
        
        AbstractCollect.init(self)
        
        self.graphics_manager_hwobj = self.getObjectByRole("graphics_manager")
        self.autoprocessing_hwobj = None

        self.emit("collectConnected", (True,))
        self.emit("collectReady", (True, ))  
    
    
    def get_detector_distance(self):
        return self.resolution_hwobj.get_detector_distance()
    
    
    def get_resolution(self):
        return self.resolution_hwobj.get_resolution()
    
    
    def do_collect(self, owner):
        """
        Actual collect sequence
        """
        user_level_log = logging.getLogger("user_level_log")
        user_level_log.info("Collection: Preparing to collect")
        self.emit("collectReady", (False, ))
        self.emit("collectOscillationStarted",
                  (owner, None, None, None, self.current_dc_parameters, None)
                  )
        self.collection_id = None

        try:
           
            # ----------------------------------------------------------------
            self.current_dc_parameters["status"] = "Running"
            self.current_dc_parameters["collection_start_time"] = \
                 time.strftime("%Y-%m-%d %H:%M:%S")

            logging.getLogger("HWR").info(
                "Collection parameters: %s" % str(self.current_dc_parameters)
            )

            user_level_log.info("Collection: Storing data collection in LIMS")
            self.store_data_collection_in_lims()
       
            user_level_log.info("Collection: Creating directories for raw images and processing files")
            self.create_file_directories()

            user_level_log.info("Collection: Getting sample info from parameters")
            self.get_sample_info()
        
            #user_level_log.info("Collect: Storing sample info in LIMS")
            #self.store_sample_info_in_lims()

            if all(item is None for item in self.current_dc_parameters['motors'].values()):
                # No centring point defined
                # create point based on the current position
                current_diffractometer_position = self.diffractometer_hwobj.getPositions()
                for motor in self.current_dc_parameters['motors'].keys():
                    self.current_dc_parameters['motors'][motor] = \
                         current_diffractometer_position.get(motor)
            
            self.take_crystal_snapshots()
            # In order to call the hook with original parameters
            # before update_data_collection_in_lims changes them
            # TODO check why this happens
            self.data_collection_hook()

            user_level_log.info("Collection: Updating data collection in LIMS")
            self.update_data_collection_in_lims()
            self.collection_finished()

        except:
            user_level_log.exception(traceback.format_exc())
            exc_type, exc_value, exc_tb = sys.exc_info()
            failed_msg = 'Data collection failed!\n%s' % exc_value
            self.collection_failed(failed_msg)

        finally:
            self.data_collection_cleanup()
            

    def data_collection_hook(self):
        """Main collection hook"""
        
        self.emit("collectStarted", (None, 1))
        
        if self.aborted_by_user:
            self.collection_failed("Aborted by user")
            self.aborted_by_user = False
            return
        
        parameters = self.current_dc_parameters
        
        self.log.info('data collection parameters received %s' % parameters)
        
        for parameter in parameters:
            self.log.info('PX2Collect %s: %s' % (str(parameter), str(parameters[parameter])))
            
        motors = parameters['motors']
        aligned_position = self.diffractometer_hwobj.translate_from_mxcube_to_md2(motors)
        osc_seq = parameters['oscillation_sequence'][0]
        fileinfo = parameters["fileinfo"]
        sample_reference = parameters['sample_reference']
        experiment_type = parameters['experiment_type'] 
        energy = parameters['energy']
        if energy < 1.e3:
            energy *= 1.e3
        photon_energy = energy
        transmission = parameters['transmission']
        resolution = parameters['resolution']['upper']
        
        exposure_time = osc_seq['exposure_time']
        in_queue = parameters['in_queue'] != False

        overlap = osc_seq['overlap']
        angle_per_frame = osc_seq['range']
        range_per_frame_ref = osc_seq['range_per_frame']
        scan_start_angle = osc_seq['start']
        number_of_images = osc_seq['number_of_images']
        image_nr_start = osc_seq['start_image_number']
        
        directory = str(fileinfo["directory"].strip('\n'))
            
        prefix = str(fileinfo['prefix'])
        template = str(fileinfo['template'])
        run_number = fileinfo['run_number']
        process_directory = fileinfo['process_directory']
        
        do_auto_analysis = parameters['processing']
        sample_reference = parameters['sample_reference']
        
        #space_group = str(sample_reference['space_group'])
        #unit_cell = list(eval(sample_reference['cell']))
        
        #self.emit("fsmConditionChanged",
                  #"data_collection_started",
                  #True)
        
        name_pattern = template[:-8]
        
        #self.log.info('PX2Collect experiment_type: %s' % (experiment_type,))
        
        if experiment_type == 'OSC':
            self.emit("progressInit", ("Collection", 100, False))
            self.log.debug('PX2Collect: executing omega_scan')
            scan_range = angle_per_frame * number_of_images
            scan_exposure_time = exposure_time * number_of_images
            self.log.debug('PX2Collect: omega_scan parameters:\n\tname_pattern: %s\
                                                        \n\tdirectory: %s\
                                                        \n\tscan_range: %.2f\
                                                        \n\tscan_exposure_time: %.2f\
                                                        \n\tscan_start_angle: %.2f\
                                                        \n\tangle_per_frame: %.2f\
                                                        \n\timage_nr_start: %.2f\
                                                        \n\tphoton_energy: %.2f\
                                                        \n\ttransmission: %.2f\
                                                        \n\tresolution: %.2f\
                                                        \n\tprocessing: %s\
                                                        \n\tsample_reference %s' % (name_pattern, directory, scan_range, scan_exposure_time, scan_start_angle, angle_per_frame, image_nr_start, photon_energy, transmission, resolution, do_auto_analysis, sample_reference))
                                                        
            
            experiment = omega_scan(name_pattern,
                                    directory,
                                    scan_range=scan_range,
                                    scan_exposure_time=scan_exposure_time,
                                    scan_start_angle=scan_start_angle,
                                    angle_per_frame=angle_per_frame,
                                    image_nr_start=image_nr_start,
                                    photon_energy=energy,
                                    transmission=transmission,
                                    resolution=resolution,
                                    simulation=False,
                                    diagnostic=True,
                                    analysis=do_auto_analysis,
                                    parent=self)
            
        elif experiment_type == 'Characterization':
            self.emit("progressInit", ("Characterization", 100, False))
            self.log.debug('PX2Collect: executing reference_images')
            number_of_wedges = osc_seq['number_of_images']
            wedge_size = osc_seq['range']
            angle_per_frame = range_per_frame_ref
            number_of_images = wedge_size/angle_per_frame
                
            overlap = osc_seq['overlap']
            scan_start_angles = []
            scan_exposure_time = exposure_time * number_of_images
            scan_range = angle_per_frame * number_of_images
            
            for k in range(number_of_wedges):
                scan_start_angles.append(scan_start_angle + k * -overlap + k * scan_range)

            self.log.debug('PX2Collect: reference_images parameters:\
                                                        \n\tname_pattern: %s\
                                                        \n\tdirectory: %s\
                                                        \n\tscan_range: %.2f\
                                                        \n\tscan_exposure_time: %.2f\
                                                        \n\tscan_start_angles: %s\
                                                        \n\tangle_per_frame: %.2f\
                                                        \n\timage_nr_start: %.2f\
                                                        \n\tphoton_energy: %.2f\
                                                        \n\ttransmission: %.2f\
                                                        \n\tresolution: %.2f' % (name_pattern, directory, scan_range, scan_exposure_time, str(scan_start_angles), angle_per_frame, image_nr_start, photon_energy, transmission, resolution))
            experiment = reference_images(name_pattern,
                                          directory,
                                          scan_range=scan_range,
                                          scan_exposure_time=scan_exposure_time,
                                          scan_start_angles=scan_start_angles,
                                          angle_per_frame=angle_per_frame,
                                          image_nr_start=image_nr_start,
                                          photon_energy=energy,
                                          transmission=transmission,
                                          resolution=resolution,
                                          simulation=False,
                                          diagnostic=True,
                                          analysis=True,
                                          parent=self)
        
        elif experiment_type == 'Helical' and osc_seq['mesh_range'] == ():
            self.emit("progressInit", ("Helical scan", 100, False))
            self.log.debug('PX2Collect: executing helical_scan')
            scan_range = angle_per_frame * number_of_images
            scan_exposure_time = exposure_time * number_of_images
            self.log.debug('helical_pos %s' % self.helical_pos)
            self.log.debug('PX2Collect: helical_scan parameters:\
                                                        \n\tname_pattern: %s\
                                                        \n\tdirectory: %s\
                                                        \n\tscan_range: %.2f\
                                                        \n\tscan_exposure_time: %.2f\
                                                        \n\tscan_start_angle: %.2f\
                                                        \n\tangle_per_frame: %.2f\
                                                        \n\timage_nr_start: %.2f\
                                                        \n\tphoton_energy: %.2f\
                                                        \n\ttransmission: %.2f\
                                                        \n\tresolution: %.2f' % (name_pattern, directory, scan_range, scan_exposure_time, scan_start_angle, angle_per_frame, image_nr_start, photon_energy, transmission, resolution))
            experiment = helical_scan(name_pattern,
                                      directory,
                                      scan_range=scan_range,
                                      scan_exposure_time=scan_exposure_time,
                                      scan_start_angle=scan_start_angle,
                                      angle_per_frame=angle_per_frame,
                                      image_nr_start=image_nr_start,
                                      position_start=self.translate_position(self.helical_pos['1']),
                                      position_end=self.translate_position(self.helical_pos['2']),
                                      photon_energy=energy,
                                      transmission=transmission,
                                      resolution=resolution,
                                      simulation=False,
                                      diagnostic=True,
                                      analysis=True,
                                      parent=self)
            
        elif experiment_type == 'Helical' and osc_seq['mesh_range'] != ():
            self.emit("progressInit", ("X-ray centring", 100, False))
            self.log.debug('PX2Collect: executing xray_centring')
            horizontal_range, vertical_range = osc_seq['mesh_range']
            number_of_lines = osc_seq['number_of_lines']
            scan_range = angle_per_frame * number_of_lines
            self.log.debug('PX2Collect: xray_centring parameters:\
                                                        \n\tname_pattern: %s\
                                                        \n\tdirectory: %s\
                                                        \n\tscan_range: %.2f\
                                                        \n\tscan_exposure_time: %.3f\
                                                        \n\tscan_start_angle: %.2f\
                                                        \n\tangle_per_frame: %.2f\
                                                        \n\timage_nr_start: %.2f\
                                                        \n\tphoton_energy: %.2f\
                                                        \n\ttransmission: %.2f\
                                                        \n\tresolution: %.2f' % (name_pattern, directory, scan_range, scan_exposure_time, scan_start_angle, angle_per_frame, image_nr_start, photon_energy, transmission, resolution))
            experiment = xray_centring(name_pattern,
                                       directory,
                                       diagnostic=True,
                                       parent=self)
            
        elif experiment_type == 'Mesh':
            self.emit("progressInit", ("Mesh scan", 100, False))
            self.log.info('PX2Collect: executing raster_scan')
            number_of_rows = int(osc_seq['number_of_lines'])
            number_of_columns = int(number_of_images/number_of_rows)
            horizontal_range, vertical_range = osc_seq['mesh_range']
            angle_per_line = angle_per_frame * number_of_rows
            if angle_per_line == 0:
                angle_per_line = 0.01
            scan_range = osc_seq['range'] * number_of_rows
            self.log.info('PX2Collect: raster_scan parameters:\
                                                        \n\tname_pattern: %s\
                                                        \n\tdirectory: %s\
                                                        \n\tvertical_range: %.2f\
                                                        \n\thorizontal_range: %.2f\
                                                        \n\tnumber_of_rows: %.2f\
                                                        \n\tnumber_of_columns: %.2f\
                                                        \n\tframe_time: %.2f\
                                                        \n\tscan_start_angle: %.2f\
                                                        \n\tscan_range: %.2f\
                                                        \n\timage_nr_start: %.2f\
                                                        \n\tphoton_energy: %.2f\
                                                        \n\ttransmission: %.2f\
                                                        \n\tresolution: %.2f' % (name_pattern, directory, vertical_range, horizontal_range, number_of_rows, number_of_columns, exposure_time, scan_start_angle, scan_range, image_nr_start, photon_energy, transmission, resolution))
                                                        
            experiment = raster_scan(name_pattern,
                                     directory,
                                     vertical_range,
                                     horizontal_range,
                                     position=aligned_position,
                                     number_of_rows=number_of_rows,
                                     number_of_columns=number_of_columns,
                                     frame_time=exposure_time,
                                     scan_start_angle=scan_start_angle,
                                     scan_range=angle_per_line,
                                     image_nr_start=image_nr_start,
                                     photon_energy=energy,
                                     transmission=transmission,
                                     resolution=resolution,
                                     simulation=False,
                                     diagnostic=True,
                                     analysis=True,
                                     parent=self)
        
        self.experiment = experiment
        self.experiment.execute()
            
        #for image in range(number_of_images):
            #if self.aborted_by_user:
                #self.ready_event.set()
                #return

            ##Uncomment to test collection failed
            ##if image == 5:
            ##    self.emit("collectOscillationFailed", (self.owner, False, 
            ##       "Failed on 5", parameters.get("collection_id")))
            ##    self.ready_event.set()
            ##    return

            #gevent.sleep(exposure_time)
            #self.emit("collectImageTaken", image)
            #self.emit("progressStep", (int(float(image) / number_of_images * 100)))
        
   
    def translate_position(self, position):
        translation = {'sampx': 'CentringX', 'sampy': 'CentringY', 'phix': 'AlignmentX', 'phiy': 'AlignmentY', 'phiz': 'AlignmentZ'}
        translated_position = {}
        for key in position:
            if key in translation:
                translated_position[translation[key]] = position[key]
            else:
                translated_position[key] = position[key]
        return translated_position
    
    def trigger_auto_processing(self, process_event, frame_number, params_dict=None):
        """
        Descript. : 
        """
        if self.autoprocessing_hwobj is not None:
            self.autoprocessing_hwobj.execute_autoprocessing(process_event, 
                self.current_dc_parameters, frame_number, self.run_processing_after)
            
    @task
    def _take_crystal_snapshot(self, filename):
        self.graphics_manager_hwobj.save_scene_snapshot(filename)
        
    @task
    def _take_crystal_animation(self, animation_filename, duration_sec):
        """Rotates sample by 360 and composes a gif file
           Animation is saved as the fourth snapshot
        """
        self.graphics_manager_hwobj.save_scene_animation(animation_filename, duration_sec)

    @task 
    def move_motors(self, motor_position_dict):
        """
        Descript. : 
        """        
        self.diffractometer_hwobj.move_motors(motor_position_dict)
   
                
    def store_image_in_lims_by_frame_num(self, frame, motor_position_id=None):
        """
        Descript. :
        """
        try:
            image_id = self.store_image_in_lims(frame)
            return image_id
        except:
            return -1
    
    def stopCollect(self, owner="MXCuBE"):
        """
        Descript. :
        """
        self.aborted_by_user = True 
        self.experiment.stop()
        self.collection_failed("Aborted by user")

    def set_helical_pos(self, helical_pos):
        self.helical_pos = helical_pos
        
    def get_slit_gaps(self):
        return self.get_slits_gap()
    
    def get_slits_gap(self):
        return self.slits1.get_horizontal_gap(), self.slits1.get_vertical_gap()

