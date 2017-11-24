import os
import subprocess
import logging
from MultiCollectMockup import MultiCollectMockup


class MultiCollectEmulator(MultiCollectMockup):
    def __init__(self, name):
        MultiCollectMockup.__init__(self, name)
        self.gphl_workflow_hwobj = self.getObjectByRole('gphl_workflow')
        if not self.gphl_workflow_hwobj:
            raise ValueError("Emulator requires GPhL workflow installation")
        self._running_process = None
        
    def init(self):
        MultiCollectMockup.init(self)

    @task
    def data_collection_hook(self, data_collect_parameters):
        print('@~@~ emulator starthook')

        # Get program locations
        gphl_installation_dir = self.gphl_workflow_hwobj['gphl_installation_dir']
        dd = self.gphl_workflow_hwobj['program_locations'].getProperties()
        license_directory = dd['co.gphl.wf.bdg_licence_dir']
        simcal_executive = os.path.join(
            gphl_installation_dir, dd['co.gphl.wf.simcal.bin']
        )

        # Get environmental variables
        envs = {'BDG_home':license_directory or gphl_installation_dir}
        envs.update(self['environment_variables'].getProperties())

        # Files NBNB TODO
        # NB outfile is the echo output of the inp[ut file; image files
        # templates ar set in the input file
        infile = None
        outfile = None
        hklfile = None
        command_list = [simcal_executive, '-input', infile, 'output', outfile,
                        '-hkl', hklfile, '--memory-pool',
                        self['simcal_memory_pool']]

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
        return_code = self._running_process.wait()
        if return_code:
            raise RuntimeError("simcal process terminated with return code %s"
                               % return_code)
        else:
            logging.getLogger('HWR').info(
                'Simcal collection emulation successful'
            )

        return
