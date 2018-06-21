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
        self.log = logging.getLogger('user_level_log')
        
    def init(self):
        """Main init method"""
        
        AbstractCollect.init(self)
        
        self.graphics_manager_hwobj = self.getObjectByRole("graphics_manager")
        
        self.emit("collectConnected", (True,))
        self.emit("collectReady", (True, ))  
    
    def get_detector_distance(self):
        return self.resolution_hwobj.get_detector_distance()
    
    def get_resolution(self):
        return self.resolution_hwobj.get_resolution()
    
    def data_collection_hook(self):
        """Main collection hook"""
        
        if self.aborted_by_user:
            self.collection_failed("Aborted by user")
            self.aborted_by_user = False
            return
        
        parameters = self.current_dc_parameters
        
        self.log.info('data collection parameters received %s' % parameters)
        
        for parameter in parameters:
            self.log.info('PX2Collect %s: %s' % (str(parameter), str(parameters[parameter])))
            
        
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
        scan_start_angle = osc_seq['start']
        number_of_images = osc_seq['number_of_images']
        image_nr_start = osc_seq['start_image_number']
        
        directory = fileinfo["directory"]
        prefix = fileinfo['prefix']
        template = fileinfo['template']
        run_number = fileinfo['run_number']
        process_directory = fileinfo['process_directory']
        
        #space_group = str(sample_reference['space_group'])
        #unit_cell = list(eval(sample_reference['cell']))
        
        #self.emit("fsmConditionChanged",
                  #"data_collection_started",
                  #True)
        
        name_pattern = template[:-7]
        
        self.log.info('PX2Collect experiment_type: %s' % (experiment_type,))
        
        
        if experiment_type == 'OSC':
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
                                                        \n\tresolution: %.2f' % (name_pattern, directory, scan_range, scan_exposure_time, scan_start_angle, angle_per_frame, image_nr_start, photon_energy, transmission, resolution))
                                                        
            
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
                                    simulation=False)
            
        elif experiment_type == 'Characterization':
            self.log.debug('PX2Collect: executing reference_images')
            number_of_wedges = osc_seq['number_of_images']
            wedge_size = osc_seq['wedge_size']
            overlap = osc_seq['overlap']
            scan_start_angles = []
            scan_exposure_time = exposure_time * wedge_size
            scan_range = angle_per_frame * wedge_size
            
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
                                          simulation=False)
        
        elif experiment_type == 'Helical' and osc_seq['mesh_range'] == ():
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
                                      simulation=False)
            
        elif experiment_type == 'Helical' and osc_seq['mesh_range'] != ():
            self.log.debug('PX2Collect: executing xray_centring')
            horizontal_range, vertical_range = osc_seq['mesh_range']
            
            self.log.debug('PX2Collect: xray_centring parameters:\
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
            experiment = xray_centring(name_pattern,
                                       directory)
            
        elif experiment_type == 'Mesh':
            self.log.debug('PX2Collect: executing raster_scan')
            number_of_columns = osc_seq['number_of_lines']
            number_of_rows = int(number_of_images/number_of_columns)
            horizontal_range, vertical_range = osc_seq['mesh_range']
            angle_per_line = angle_per_frame * number_of_columns
            
            self.log.debug('PX2Collect: raster_scan parameters:\
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
                                                        \n\tresolution: %.2f' % (name_pattern, directory, vertical_range, horizontal_range, number_of_rows, number_of_columns, frame_time, scan_start_angle, scan_range, image_nr_start, photon_energy, transmission, resolution))
                                                        
            experiment = raster_scan(name_pattern,
                                     directory,
                                     vertical_range,
                                     horizontal_range,
                                     number_of_rows,
                                     number_of_columns,
                                     frame_time=exposure_time,
                                     scan_start_angle=scan_start_angle,
                                     scan_range=angle_per_line,
                                     image_nr_start=image_nr_start,
                                     photon_energy=energy,
                                     transmission=transmission,
                                     resolution=resolution,
                                     simulation=False)
        
        self.experiment = experiment
        
        #self.experiment.execute()
            
            
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
    
    def trigger_auto_processing(self, process_event, params_dict, frame_number):
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
        image_id = self.store_image_in_lims(frame)
        return image_id
    
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

