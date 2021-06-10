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

import os
import logging
import gevent
import json
import tempfile
import redis
from mxcubecore.TaskUtils import task
from mxcubecore.BaseHardwareObjects import HardwareObject
from mxcubecore.HardwareObjects.abstract.AbstractCollect import AbstractCollect

from omega_scan import omega_scan
from inverse_scan import inverse_scan
from reference_images import reference_images
from helical_scan import helical_scan
from fluorescence_spectrum import fluorescence_spectrum
from energy_scan import energy_scan

# from xray_centring import xray_centring
from raster_scan import raster_scan
from nested_helical_acquisition import nested_helical_acquisition
from tomography import tomography
from film import film
from mxcubecore import HardwareRepository as HWR

from slits import slits1

__credits__ = ["Synchrotron SOLEIL"]
__version__ = "2.3."
__category__ = "General"


class PX2Collect(AbstractCollect):
    """Main data collection class. Inherited from AbstractCollect.
       Collection is done by setting collection parameters and
       executing collect command
    """

    experiment_types = [
        "omega_scan",
        "reference_images",
        "inverse_scan",
        "mad",
        "helical_scan",
        "xrf_spectrum",
        "energy_scan",
        "raster_scan",
        "nested_helical_acquisition",
        "tomography",
        "film",
        "optical_centering",
    ]

    # experiment_types = ['OSC',
    # 'Collect - Multiwedge',
    # 'Helical',
    # 'Mesh',
    # 'energy_scan',
    # 'xrf_spectrum',
    # 'neha'

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
        self.redis = redis.StrictRedis(host='172.19.10.23')
        self.collection_id = -1
        
    def init(self):
        AbstractCollect.init(self)
        self.emit("collectConnected", (True,))
        self.emit("collectReady", (True,))

    def _collect(self, cp):
        """Main collection hook"""
        print('PX2Collect _collect')
        parameters = cp
        self.emit("collectStarted", (None, 1))
        
        if hasattr(self, 'experiment'):
            del self.experiment
            
        if self.aborted_by_user == True:
            self.collection_finished("Aborted by user")
            self.aborted_by_user = False
            return
        
        parameters = self.current_dc_parameters

        self.log.info('in data_collection_hook, self.collection_id %s' % self.collection_id)

        # save a json file for the autoprocessing
        execute_XDSME = eval(self.redis.get('XDSME'))
        execute_autoPROC = eval(self.redis.get('autoPROC'))
        autoproc_options = {'xdsme': execute_XDSME, 'autoproc': execute_autoPROC}
        #print('type(parameters)', type(parameters))
        processing_parameters = dict(parameters)
        processing_parameters['collection_id'] = self.collection_id
        processing_parameters["autoproc_options"] = {'xdsme': execute_XDSME, 'autoproc': execute_autoPROC}
        jsonstr = json.dumps(processing_parameters)
        fd, name = tempfile.mkstemp(dir="/tmp")
        os.write(fd, jsonstr.encode("utf-8"))
        os.close(fd)
        
        self.log.info('data collection parameters received %s' % parameters)
        
        log_hwr = logging.getLogger('HWR')
        for parameter in parameters:
            log_hwr.info('PX2Collect %s: %s' % (str(parameter), str(parameters[parameter])))
         
        shutterless = parameters['shutterless']
        motors = parameters['motors']
        aligned_position = HWR.beamline.diffractometer.translate_from_mxcube_to_md2(motors)
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
        
        directory = str(fileinfo["directory"].strip('\n'))
            
        prefix = str(fileinfo['prefix'])
        template = str(fileinfo['template'])
        run_number = fileinfo['run_number']
        process_directory = fileinfo['process_directory']
        
        if parameters['processing'] in ['True', True]:
            do_auto_analysis = True
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
                                                        \n\tposition: %s\
                                                        \n\tscan_range: %.2f\
                                                        \n\tscan_exposure_time: %.2f\
                                                        \n\tscan_start_angle: %.2f\
                                                        \n\tangle_per_frame: %.2f\
                                                        \n\timage_nr_start: %.2f\
                                                        \n\tphoton_energy: %.2f\
                                                        \n\ttransmission: %.2f\
                                                        \n\tresolution: %.2f\
                                                        \n\tprocessing: %s\
                                                        \n\tsample_reference %s' % (name_pattern, directory, aligned_position, scan_range, scan_exposure_time, scan_start_angle, angle_per_frame, image_nr_start, photon_energy, transmission, resolution, do_auto_analysis, sample_reference))
                                                        
            
            experiment = omega_scan(name_pattern,
                                    directory,
                                    position=aligned_position,
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
            try:
                wedge_size = osc_seq['range']
            except KeyError:
                wedge_size = 10
            try:
                range_per_frame_ref = osc_seq['range_per_frame']
            except KeyError:
                range_per_frame_ref = 0.1
            angle_per_frame = range_per_frame_ref
            number_of_images = wedge_size/angle_per_frame
            
            #overlap = osc_seq['overlap']
            overlap = -90 + wedge_size
            
            scan_start_angles = []
            scan_exposure_time = exposure_time * number_of_images
            scan_range = angle_per_frame * number_of_images
            
            for k in range(number_of_wedges):
                scan_start_angles.append(scan_start_angle + k * -overlap + k * scan_range)

            self.log.debug('PX2Collect: reference_images parameters:\
                                                        \n\tname_pattern: %s\
                                                        \n\tdirectory: %s\
                                                        \n\tposition: %s\
                                                        \n\tscan_range: %.2f\
                                                        \n\tscan_exposure_time: %.2f\
                                                        \n\tscan_start_angles: %s\
                                                        \n\tangle_per_frame: %.2f\
                                                        \n\timage_nr_start: %.2f\
                                                        \n\tphoton_energy: %.2f\
                                                        \n\ttransmission: %.2f\
                                                        \n\tresolution: %.2f' % (name_pattern, directory, aligned_position, scan_range, scan_exposure_time, str(scan_start_angles), angle_per_frame, image_nr_start, photon_energy, transmission, resolution))
            experiment = reference_images(name_pattern,
                                          directory,
                                          position=aligned_position,
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
                                          analysis=do_auto_analysis,
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
                                      analysis=do_auto_analysis,
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
            
            if shutterless == True:
                scan_range = angle_per_frame * number_of_rows
            else:
                scan_range = angle_per_frame

            if scan_range == 0:
                scan_range = 0.01
                
            try:
                nimages_per_point = int(self.redis.get('nimages_per_point'))
            except:
                nimages_per_point = 1
            try:
                npasses = int(self.redis.get('npasses'))
            except:
                npasses = 1
            try:
                dark_time_between_passes = float(self.redis.get('dark_time_between_passes'))
            except:
                dark_time_between_passes = 0.
            try:
                fast_axis = self.redis.get('fast_axis')
            except:
                fast_axis = 'vertical'
                
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
                                                        \n\tresolution: %.2f\
                                                        \n\tshutterless: %s\
                                                        \n\tfast_axis: %s\
                                                        \n\tnimages_per_point: %d\
                                                        \n\tnpasses: %d\
                                                        \n\tdark_time_between_passes: %.2f' % (name_pattern, directory, vertical_range, horizontal_range, number_of_rows, number_of_columns, exposure_time, scan_start_angle, scan_range, image_nr_start, photon_energy, transmission, resolution, shutterless, fast_axis, nimages_per_point, npasses, dark_time_between_passes))
                                                        
            experiment = raster_scan(name_pattern,
                                     directory,
                                     vertical_range,
                                     horizontal_range,
                                     position=aligned_position,
                                     number_of_rows=number_of_rows,
                                     number_of_columns=number_of_columns,
                                     frame_time=exposure_time,
                                     scan_start_angle=scan_start_angle,
                                     scan_range=scan_range,
                                     image_nr_start=image_nr_start,
                                     photon_energy=energy,
                                     transmission=transmission,
                                     resolution=resolution,
                                     shutterless=shutterless,
                                     scan_axis=fast_axis,
                                     nimages_per_point=nimages_per_point,
                                     npasses=npasses,
                                     dark_time_between_passes=dark_time_between_passes,
                                     use_centring_table=True,
                                     simulation=False,
                                     diagnostic=True,
                                     analysis=True,
                                     parent=self)
        
        self.experiment = experiment
        
        if self.experiment._stop_flag == False:
            self.experiment.execute()
            
            if do_auto_analysis == True:
                self.log.info('executing autoprocessing-px2 %s' % name)
                os.system('autoprocessing-px2 %s &' % name)
                
        self.emit_collection_finished()

    def translate_position(self, position):
        translation = {
            "sampx": "CentringX",
            "sampy": "CentringY",
            "phix": "AlignmentX",
            "phiy": "AlignmentY",
            "phiz": "AlignmentZ",
        }
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
        if HWR.beamline.offline_processing is not None:
            HWR.beamline.offline_processing.execute_autoprocessing(
                process_event,
                self.current_dc_parameters,
                frame_number,
                self.run_processing_after,
            )

    @task
    def _take_crystal_snapshot(self, filename):
        HWR.beamline.sample_view.save_snapshot(filename)

    @task
    def _take_crystal_animation(self, animation_filename, duration_sec):
        """Rotates sample by 360 and composes a gif file
           Animation is saved as the fourth snapshot
        """
        HWR.beamline.sample_view.save_scene_animation(animation_filename, duration_sec)

    @task
    def move_motors(self, motor_position_dict):
        """
        Descript. :
        """
        return

    def emit_collection_finished(self):
        """Collection finished beahviour
        """
        if self.current_dc_parameters["experiment_type"] != "Collect - Multiwedge":
            #self.update_data_collection_in_lims()

            last_frame = self.current_dc_parameters["oscillation_sequence"][0][
                "number_of_images"
            ]
            if last_frame > 1:
                pass
                #self.store_image_in_lims_by_frame_num(last_frame)
            if (
                self.current_dc_parameters["experiment_type"] in ("OSC", "Helical")
                and self.current_dc_parameters["oscillation_sequence"][0]["overlap"]
                == 0
                and last_frame > 19
            ):
                self.trigger_auto_processing("after", self.current_dc_parameters, 0)

        success_msg = "Data collection successful"
        #self.current_dc_parameters["status"] = success_msg
        self.emit(
            "collectOscillationFinished",
            (
                self.owner,
                True,
                success_msg,
                self.current_dc_parameters.get("collection_id"),
                self.osc_id,
                self.current_dc_parameters,
            ),
        )
        self.emit("collectEnded", self.owner, success_msg)
        self.emit("collectReady", (True,))
        self.emit("progressStop", ())
        self.emit("fsmConditionChanged", "data_collection_successful", True)
        self.emit("fsmConditionChanged", "data_collection_started", False)
        self._collecting = None
        self.ready_event.set()

    def store_image_in_lims_by_frame_num(self, frame, motor_position_id=None):
        """
        Descript. :
        """
        image_id = None
        self.trigger_auto_processing("image", self.current_dc_parameters, frame)
        image_id = self._store_image_in_lims(self.current_dc_parameters, frame)
        return image_id

    def stopCollect(self, owner="MXCuBE"):
        """
        Descript. :
        """
        self.aborted_by_user = True
        self.cmd_collect_abort()
        self.emit_collection_failed("Aborted by user")

    def set_helical(self, helical=True):
        self.helical = helical
        
    def set_helical_pos(self, helical_pos):
        self.helical_pos = helical_pos

    def get_slit_gaps(self):
        return self.get_slits_gap()

    def get_slits_gap(self):
        return self.slits1.get_horizontal_gap(), self.slits1.get_vertical_gap()
