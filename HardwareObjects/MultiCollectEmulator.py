import os
import subprocess
import logging
import math
import f90nml
import General
from MultiCollectMockup import MultiCollectMockup
from HardwareRepository import HardwareRepository
from TaskUtils import task


class MultiCollectEmulator(MultiCollectMockup):
    def __init__(self, name):
        MultiCollectMockup.__init__(self, name)
        self._running_process = None

        # TODO get appropriate value
        # We must have a value for functions to work
        # This ought to eb OK for a Pilatus 6M (See TangoResolution object)
        self.det_radius = 212.

        self._detector_distance = 300.
        self._wavelength = 1.0

        
    def init(self):
        MultiCollectMockup.init(self)
        self.gphl_workflow_hwobj = self.getObjectByRole('gphl_workflow')
        if not self.gphl_workflow_hwobj:
            raise ValueError("Emulator requires GPhL workflow installation")

    def make_image_file_template(self, data_collect_parameters):

        file_parameters = data_collect_parameters["fileinfo"]

        file_parameters["suffix"] = self.bl_config.detector_fileext
        image_file_template = "%(prefix)s_%(run_number)s_%%%%%%%%.%(suffix)s" % file_parameters
        file_parameters["template"] = image_file_template

    @task
    def data_collection_hook(self, data_collect_parameters):
        print('@~@~ emulator starthook')

        self.make_image_file_template(data_collect_parameters)

        # Get program locations
        print('@~@~ properties',  self.gphl_workflow_hwobj,
              sorted(self.gphl_workflow_hwobj.getProperties()))
        gphl_installation_dir = self.gphl_workflow_hwobj.getProperty(
            'gphl_installation_dir'
        )
        dd = self.gphl_workflow_hwobj['gphl_program_locations'].getProperties()
        license_directory = dd['co.gphl.wf.bdg_licence_dir']
        simcal_executive = os.path.join(
            gphl_installation_dir, dd['co.gphl.wf.simcal.bin']
        )

        # Get environmental variables
        envs = {'BDG_home':license_directory or gphl_installation_dir}
        for tag, val in self['environment_variables'].getProperties().items():
            envs[str(tag)] = str(val)

        for tt in sorted(envs.items()):
            print ('@~@~ env %s : %s' % tt, type(tt[1]))

        # get input data
        fp = os.path.join(
            HardwareRepository.HardwareRepository().getHardwareRepositoryPath(),
            self.gphl_workflow_hwobj.getProperty('gphl_config_subdir'),
            'simcal_template.nml'
        )
        input_data = f90nml.read(fp)

        sample_dir = os.path.join(
            HardwareRepository.HardwareRepository().getHardwareRepositoryPath(),
            self.gphl_workflow_hwobj.getProperty('gphl_samples_subdir'),
            self.getProperty('sample_name')
        )
        crystal_input = f90nml.read(os.path.join(sample_dir, 'crystal.nml'))
        print('\n@~@~ collect_parameters')
        for tt in data_collect_parameters.items():
            print('---> %s %s' % tt)

        input_data['setup_list'].update(crystal_input['simcal_crystal_list'])

        sweeps = []
        for osc in data_collect_parameters['oscillation_sequence']:
            motors = data_collect_parameters['motors']
            sweep = input_data['sweep_list'].copy()
            sweep['lambda'] = General.h_over_e/data_collect_parameters['energy']
            sweep['exposure'] = osc['exposure_time']
            sweep['image_no'] = osc['start_image_number']
            sweep['n_frames'] = osc['number_of_images']
            sweep['step_deg'] = osc['range']
            # NBNB hardwired for omega scan TODO
            sweep['omega_deg'] = osc['start']
            sweep['kappa_deg'] = motors['kappa']
            sweep['phi_deg'] = motors['kappa_phi']

            ss =  self.gphl_workflow_hwobj.getProperty('translation_axis_roles')
            print('@~@~', ss, ss.split(), list(motors.get(x) or 0.0 for x in ss.split()))
            sweep['trans_xyz'] = list(motors.get(x) or 0.0 for x in ss.split())

            # get resolution limit and detector distance
            resolution = data_collect_parameters['resolution']['upper']
            self.set_resolution(resolution)
            sweep['res_limit'] = resolution
            sweep['det_coord'] = self.get_detector_distance()

            name_template = os.path.join(
                data_collect_parameters['fileinfo']['directory'],
                data_collect_parameters['fileinfo']['template']
            )
            sweep['name_template'] = General.to_ascii(name_template)
            # Skipped: axis_no=3, spindle_deg=0.0, two_theta_deg=0.0, mu_air=-1, mu_sensor=-1

            sweeps.append(sweep)

        if len(sweeps) == 1:
            # NBNB in current code we can have only one sweep here,
            # but it will work for multiple
            input_data['sweep_list'] = sweep
        else:
            input_data['sweep_list'] = sweeps

        # NB outfile is the echo output of the input file;
        # image files templates ar set in the input file
        file_info = data_collect_parameters['fileinfo']
        print ('@~@~1 processdir', os.path.exists(file_info['process_directory']),
               file_info['process_directory'])
        print ('@~@~1 datadir', os.path.exists(file_info['directory']),
               file_info['directory'])
        if not os.path.exists(file_info['process_directory']):
            os.makedirs(file_info['process_directory'])
        if not os.path.exists(file_info['directory']):
            os.makedirs(file_info['directory'])
        infile = os.path.join(file_info['process_directory'], 'simcal_in.nml')
        f90nml.write(input_data, infile, force=True)
        print('@~@~ infile', infile)
        outfile = os.path.join(file_info['process_directory'], 'simcal_out.nml')
        hklfile = os.path.join(sample_dir, 'sample.hkli')
        command_list = [simcal_executive, '--input', infile, '--output', outfile,
                        '--hkl', hklfile]

        memory_pool =  self.getProperty('simcal_memory_pool')
        if memory_pool:
            command_list.extend(( '--memory-pool', str(memory_pool)))
        print('@~@~ command_list', command_list)

        try:
            self._running_process = subprocess.Popen(command_list, stdout=None,
                                                     stderr=None, env=envs)
        except:
            logging.getLogger('HWR').error('Error in spawning workflow application')
            raise


        return

    @task
    def data_collection_end_hook(self, data_collect_parameters):
        logging.getLogger('HWR').info(
            'Waiting for simcal collection emulation.'
        )
        # NBNB TODO put in time-out, somehow
        if self._running_process is not None:
            return_code = self._running_process.wait()
            if return_code:
                raise RuntimeError("simcal process terminated with return code %s"
                                   % return_code)
            else:
                logging.getLogger('HWR').info(
                    'Simcal collection emulation successful'
                )

        return

    @task
    def set_resolution(self, new_resolution):
        self._detector_distance = self.res2dist(new_resolution)

    @task
    def move_detector(self, detector_distance):
        self._detector_distance = detector_distance

    def set_wavelength(self, wavelength):
        self._wavelength = wavelength

    def set_energy(self, energy):
        self.set_wavelength(General.h_over_e/energy)

    def get_wavelength(self):
        return self._wavelength

    def get_detector_distance(self):
        return self._detector_distance

    def get_resolution(self):
        return self.dist2res()


    def res2dist(self, res=None):
        current_wavelength = self._wavelength

        if res is None:
            res = self._resolution

        try:
            ttheta = 2*math.asin(current_wavelength / (2*res))
            return self.det_radius / math.tan(ttheta)
        except:
            return None

    def dist2res(self, dist=None):
        current_wavelength = self._wavelength
        if dist is None:
            dist = self._detector_distance

        try:
            ttheta = math.atan(self.det_radius / dist)

            if ttheta:
                return current_wavelength / (2*math.sin(ttheta/2))
            else:
                return None
        except Exception:
            logging.getLogger().exception("error while calculating resolution")
            return None
