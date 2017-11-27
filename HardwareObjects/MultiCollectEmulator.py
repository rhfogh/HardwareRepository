import os
import subprocess
import logging
import f90nml
import General
from MultiCollectMockup import MultiCollectMockup
from HardwareRepository import HardwareRepository
from TaskUtils import task


class MultiCollectEmulator(MultiCollectMockup):
    def __init__(self, name):
        MultiCollectMockup.__init__(self, name)
        self._running_process = None
        
    def init(self):
        MultiCollectMockup.init(self)
        self.gphl_workflow_hwobj = self.getObjectByRole('gphl_workflow')
        if not self.gphl_workflow_hwobj:
            raise ValueError("Emulator requires GPhL workflow installation")

    @task
    def data_collection_hook(self, data_collect_parameters):
        print('@~@~ emulator starthook')

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
        envs.update(self['environment_variables'].getProperties())

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

        # TODO what if thereis more than one sweep or oscillation?
        osc = data_collect_parameters['oscillation_sequence'][0]
        motors = data_collect_parameters['motors']
        sweep = input_data['sweep_list']
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

        # Still missing: res_limit, det_coord, name_template
        # Skipped: axis_no=3, spindle_deg=0.0, two_theta_deg=0.0, mu_air=-1, mu_sensor=-1


        # NB outfile is the echo output of the inp[ut file; image files
        # templates ar set in the input file
        file_info = data_collect_parameters['fileinfo']
        if not os.path.exists(file_info['process_directory']):
            os.makedirs(file_info['process_directory'])
        infile = os.path.join(file_info['process_directory'], 'simcal_in.nml')
        f90nml.write(input_data, infile, force=True)
        print('@~@~ infile', infile)
        outfile = os.path.join(file_info['process_directory'], 'simcal_out.nml')
        hklfile = os.path.join(sample_dir, 'sample.hkli')
        command_list = [simcal_executive, '-input', infile, 'output', outfile,
                        '-hkl', hklfile]

        memory_pool =  self.getProperty('simcal_memory_pool')
        if memory_pool:
            command_list.extend(( '--memory-pool', memory_pool))
        print('@~@~ command_list', command_list)
        return

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
