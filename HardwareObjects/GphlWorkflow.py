#! /usr/bin/env python
# encoding: utf-8
"""Global phasing workflow runner
"""

__copyright__ = """
  * Copyright © 2016 - 2017 by Global Phasing Ltd.
"""
__author__ = "rhfogh"
__date__ = "06/04/17"

import logging
import uuid
import time
import sys

import gevent
import gevent.event

import General
from HardwareRepository.BaseHardwareObjects import HardwareObject
from HardwareRepository.HardwareRepository import dispatcher
from HardwareRepository.HardwareRepository import HardwareRepository

import queue_model_objects_v1 as queue_model_objects
from queue_entry import QUEUE_ENTRY_STATUS

States = General.States


class GphlWorkflow(HardwareObject, object):
    """Global Phasing workflow runner.
    """

    # Imported here to keep it out of the shared top namespace
    # NB, by the time the code gets here, HardwareObjects is on the PYTHONPATH
    # as is HardwareRepository
    # NB accessed as self.GphlMessages
    import GphlMessages

    # object states
    valid_states = [
        States.OFF,     # Not active
        States.ON,      # Active, awaiting execution order
        States.OPEN,    # Active, awaiting input
        States.RUNNING, # Active, executing workflow
    ]

    def __init__(self, name):
        HardwareObject.__init__(self, name)
        self._state = States.OFF

        # Event to handle data requests from mxcube
        self._gevent_event = None

        # HO that handles connection to GPhL workflow runner
        self.workflow_connection = None

        # Needed to allow methods to put new actions on the queue
        # TODO change to _queue_entry and add getters and setters.
        self.queue_entry = None

        # Event to handle waiting for answers from server
        self._gphl_process_finished = None

        # Subprocess names to track which subprocess is getting info
        self._server_subprocess_names = {}

        # Execution timeout waiting for the workflow engine
        self.execution_timeout = None

        # Directory for GPhL beamline configuration files
        self.gphl_beamline_config = None

        # Rotation axis role names, ordered from holder towards sample
        self.rotation_axis_roles = []

        # Translation axis role names
        self.translation_axis_roles = []

        # Name of centring method to use
        self.centring_method = None

    def _init(self):
        pass

    def init(self):

        # Used only here, so let us keep the import out of the module top
        from GphlWorkflowConnection import GphlWorkflowConnection

        self.execution_timeout = self.getProperty('execution_timeout')

        self.rotation_axis_roles = self.getProperty('rotation_axis_roles').split()
        self.translation_axis_roles = self.getProperty('translation_axis_roles').split()

        self.centring_method = self.getProperty('centring_method')

        workflow_connection = GphlWorkflowConnection()
        dd = (self['connection_parameters'].getProperties()
              if self.hasObject('connection_parameters') else {})
        workflow_connection.init(**dd)
        self.workflow_connection = workflow_connection


        relative_file_path = self.getProperty('gphl_config_subdir')
        self.gphl_beamline_config = HardwareRepository().findInRepository(
            relative_file_path
        )

        # Set up local listeners
        dispatcher.connect(self.echo_info_string,
                           'GPHL_INFO',
                           workflow_connection)
        dispatcher.connect(self.echo_subprocess_started,
                           'GPHL_SUBPROCESS_STARTED',
                           workflow_connection)
        dispatcher.connect(self.echo_subprocess_stopped,
                           'GPHL_SUBPROCESS_STOPPED',
                           workflow_connection)
        dispatcher.connect(self.get_configuration_data,
                           'GPHL_REQUEST_CONFIGURATION',
                           workflow_connection)
        dispatcher.connect(self.setup_data_collection,
                           'GPHL_GEOMETRIC_STRATEGY',
                           workflow_connection)
        dispatcher.connect(self.collect_data,
                           'GPHL_COLLECTION_PROPOSAL',
                           workflow_connection)
        dispatcher.connect(self.select_lattice,
                           'GPHL_CHOOSE_LATTICE',
                           workflow_connection)
        dispatcher.connect(self.process_centring_request,
                           'GPHL_REQUEST_CENTRING',
                           workflow_connection)
        dispatcher.connect(self.obtain_prior_information,
                           'GPHL_OBTAIN_PRIOR_INFORMATION',
                           workflow_connection)
        dispatcher.connect(self.prepare_for_centring,
                           'GPHL_PREPARE_FOR_CENTRING',
                           workflow_connection)
        dispatcher.connect(self.workflow_aborted,
                           'GPHL_WORKFLOW_ABORTED',
                           workflow_connection)
        dispatcher.connect(self.workflow_completed,
                           'GPHL_WORKFLOW_COMPLETED',
                           workflow_connection)
        dispatcher.connect(self.workflow_failed,
                           'GPHL_WORKFLOW_FAILED',
                           workflow_connection)

        self._gevent_event = gevent.event.Event()
        self.set_state(States.ON)


    def get_available_workflows(self):
        """Get list of workflow description dictionaries.

        The structure is modeled on the EDNAworkflow function of the same name;
        for now the dictionaries have items 'name' and 'doc'"""

        result = []

        for wf_node in self['workflows']:
            result.append(dict((
                ('name',wf_node.name()),
                ('doc', wf_node.getProperty('documentation', default_value=''))
            )))
        #
        return result

    def get_state(self):
        return self._state

    def set_state(self, value):
        if value in self.valid_states:
            self._state = value
            self.emit('stateChanged', (value, ))
        else:
            raise RuntimeError("GphlWorlflow set to invalid state: s"
                               % value)

    def workflow_end(self):
        """
        The workflow has finished, sets the state to 'ON'
        """
        if not self._gphl_process_finished.ready():
            # stop waiting process - workflow_end will be re-called from there
            self._gphl_process_finished.set("Workflow Terminated")
            return

        self.queue_entry = None
        self._gphl_process_finished = None
        self.set_state(States.ON)
        # If necessary unblock dialog
        if not self._gevent_event.is_set():
            self._gevent_event.set()

    # TODO dialog handling
    # def open_dialog(self, dict_dialog):
    #     # If necessary unblock dialog
    #     if not self._gevent_event.is_set():
    #         self._gevent_event.set()
    #     self.params_dict = dict()
    #     if "reviewData" in dict_dialog and "inputMap" in dict_dialog:
    #         review_data = dict_dialog["reviewData"]
    #         for dictEntry in dict_dialog["inputMap"]:
    #             if "value" in dictEntry:
    #                 value = dictEntry["value"]
    #             else:
    #                 value = dictEntry["defaultValue"]
    #             self.params_dict[dictEntry["variableName"]] = str(value)
    #         self.emit('parametersNeeded', (review_data, ))
    #         self.state.value = "OPEN"
    #         self._gevent_event.clear()
    #         while not self._gevent_event.is_set():
    #             self._gevent_event.wait()
    #             time.sleep(0.1)
    #     return self.params_dict
    #
    # def get_values_map(self):
    #     return self.params_dict
    #
    # def set_values_map(self, params):
    #     self.params_dict = params
    #     self._gevent_event.set()

    def abort(self):
        logging.getLogger("HWR").info('Aborting current workflow')
        # If necessary unblock dialog
        self.workflow_end()

        dispatcher.send(
            self.GphlMessages.message_type_to_signal['BeamlineAbort'], self,
            message="GPhL workflow run aborted from GphlWorkflow HardwareObject"
        )

    def execute(self, queue_entry):

        self.queue_entry = queue_entry

        try:
            # If necessary unblock dialog
            if not self._gevent_event.is_set():
                self._gevent_event.set()
            self.set_state(States.RUNNING)

            # Start GPhL workflow handling
            self._gphl_process_finished = gevent.event.AsyncResult()

            # Fork off workflow server process
            self.workflow_connection.start_workflow(
                queue_entry.get_data_model()
            )

            final_message = self._gphl_process_finished.get(
                timeout=self.execution_timeout
            )
            if final_message is None:
                final_message = 'Timeout'
                self.abort()
            logging.getLogger("user_level_log").info(
                "GPhL Workflow end : %s" % final_message
            )
        finally:
            self.workflow_end()

    # Message handlers:

    def workflow_aborted(self, payload, correlation_id):
        # NB Echo additional content later
        self._gphl_process_finished.set(payload.__class__.__name__)

    def workflow_completed(self, payload, correlation_id):
        # NB Echo additional content later
        self._gphl_process_finished.set(payload.__class__.__name__)

    def workflow_failed(self, payload, correlation_id):
        # NB Echo additional content later
        self._gphl_process_finished.set(payload.__class__.__name__)

    def echo_info_string(self, payload, correlation_id=None):
        """Print text info to console,. log etc."""
        subprocess_name = self._server_subprocess_names.get(correlation_id)
        if subprocess_name:
            logging.info ('%s: %s' % (subprocess_name, payload))
        else:
            logging.info(payload)

    def echo_subprocess_started(self, payload, correlation_id):
        name = payload.name
        if correlation_id:
            self._server_subprocess_names[correlation_id] = name
        logging.info('%s : STARTING' % name)

    def echo_subprocess_stopped(self, payload, correlation_id):
        try:
            name = self._server_subprocess_names.pop(correlation_id)
        except KeyError:
            name = 'Unknown process'
        logging.info('%s : FINISHED' % name)

    def get_configuration_data(self, payload, correlation_id):
        data_location = self.gphl_beamline_config
        return self.GphlMessages.ConfigurationData(data_location)

    def queryCollectionStrategy(self, geometric_strategy):
        """Display collection strategy for user approval,
        and query parameters needed"""

        result = {}

        isInterleaved = geometric_strategy.isInterleaved
        allowed_widths = geometric_strategy.allowedWidths
        default_width_index = geometric_strategy.defaultWidthIdx or 0

        # TODO put user display/query here

        # For now return default values
        result['imageWidth'] = allowed_widths[default_width_index]
        result['transmission'] = 1.0  # 100%
        result['exposure'] = 0.035  # random value
        if isInterleaved:
            result['wedgeWidth'] = 10
        #
        return result


    def setup_data_collection(self, payload, correlation_id):
        geometric_strategy = payload
        # NB this call also asks for OK/abort of strategy, hence put first
        parameters = self.queryCollectionStrategy(geometric_strategy)
        user_modifiable = geometric_strategy.isUserModifiable

        goniostatSweepSettings = {}
        goniostatTranslations = []
        for sweep in geometric_strategy.sweeps:
            sweepSetting = sweep.goniostatSweepSetting
            requestedRotationId = sweepSetting.id
            if requestedRotationId not in goniostatSweepSettings:

                if user_modifiable:
                    # Query user for new rotationSetting and make it,
                    # sweepSetting = 'New Instance'
                    logging.getLogger('HWR').warning(
                        "User modification of sweep settings not implemented. Ignored"
                    )
                goniostatSweepSettings[sweepSetting.id] = sweepSetting
                # NB there is no provision for NOT making a new translation
                # object if you are making no changes
                goniostatTranslation = self.center_sample(sweepSetting,
                                                          requestedRotationId)
                goniostatTranslations.append(goniostatTranslation)

        sampleCentred = self.GphlMessages.SampleCentred(
            goniostatTranslations=goniostatTranslations,
            **parameters
        )
        return sampleCentred


    def collect_data(self, payload, correlation_id):
        collection_proposal = payload

        beamline_setup_hwobj = self.queue_entry.beamline_setup
        resolution_hwobj = self.queue_entry.beamline_setup.getObjectByRole(
            "resolution"
        )
        queue_model_hwobj = HardwareRepository().getHardwareObject(
            'queue-model'
        )
        queue_manager = HardwareRepository().getHardwareObject(
            'queue'
        )

        relative_image_dir = collection_proposal.relativeImageDir

        session = self.queue_entry.beamline_setup.getObjectByRole(
            "session"
        )

        # # NO. By the time this is called, the queue will be executing already
        # if queue_manager.is_executing():
        #     message = "Cannot start data collection, queue is already executing"
        #     logging.getLogger('HWR').error(message)
        #     self.workflow_connection.abort_workflow(message)
        #     return self.GphlMessages.CollectionDone(
        #         proposalId=collection_proposal.id, status=1)

        # NBNB TODO for now we are NOT asking for confirmation
        # and NOT allowing the change of relativeImageDir
        # Maybe later

        gphl_workflow_model = self.queue_entry.get_data_model()


        new_dcg_name = 'GPhL Data Collection'
        new_dcg_model = queue_model_objects.TaskGroup()
        new_dcg_model.set_enabled(False)
        new_dcg_model.set_name(new_dcg_name)
        new_dcg_model.set_number(
            gphl_workflow_model.get_next_number_for_name(new_dcg_name)
        )
        queue_model_hwobj.add_child(gphl_workflow_model, new_dcg_model)

        sample = gphl_workflow_model.get_sample_node()
        # There will be exactly one for the kinds of collection we are doing
        crystal = sample.crystals[0]
        data_collections = []
        for scan in collection_proposal.scans:
            sweep = scan.sweep
            acq = queue_model_objects.Acquisition()

            # Get defaults, even though we override most of them
            acq_parameters = (
                beamline_setup_hwobj.get_default_acquisition_parameters()
            )
            acq.acquisition_parameters = acq_parameters

            acq_parameters.first_image = scan.imageStartNum
            acq_parameters.num_images = scan.width.numImages
            acq_parameters.osc_start = scan.start
            acq_parameters.osc_range = scan.width.imageWidth
            # acq_parameters.kappa = self._get_kappa_axis_position()
            # acq_parameters.kappa_phi = self._get_kappa_phi_axis_position()
            # acq_parameters.overlap = overlap
            acq_parameters.exp_time = scan.exposure.time
            acq_parameters.num_passes = 1
            # NBNB TODO this parameter must be queried, somehow.
            acq_parameters.resolution = resolution_hwobj.currentResolution
            acq_parameters.energy = General.h_over_e/sweep.beamSetting.wavelength
            # NB TODO comes in as 0 <= x <- 1  Check this is OK.
            acq_parameters.transmission = scan.exposure.transmission
            # acq_parameters.shutterless = self._has_shutterless()
            # acq_parameters.detector_mode = self._get_roi_modes()
            acq_parameters.inverse_beam = False
            # acq_parameters.take_dark_current = True
            # acq_parameters.skip_existing_images = False
            # acq_parameters.take_snapshots = True

            # Edna also sets screening_id
            # Edna also sets osc_end

            goniostatRotation = sweep.goniostatSweepSetting
            goniostatTranslation = goniostatRotation.translation
            dd = dict((x, goniostatRotation.axisSettings[x])
                      for x in self.rotation_axis_roles)
            if goniostatTranslation is not None:
                for tag in self.translation_axis_roles:
                    val = goniostatTranslation.axisSettings.get(tag)
                    if val is not None:
                        dd[tag] = val
            dd[goniostatRotation.scanAxis] = scan.start
            acq_parameters.centred_position = (
                queue_model_objects.CentredPosition(dd)
            )

            # Path_template
            path_template = beamline_setup_hwobj.get_default_path_template()
            acq.path_template = path_template
            path_template.directory = session.get_image_directory(
                relative_image_dir
            )
            filename_params = scan.filenameParams
            ss = filename_params.get('run_number')
            path_template.run_number = int(ss) if ss else 1
            prefix = filename_params.get('prefix', '')
            ib_component = filename_params.get('inverse_beam_component_sign',
                                               '')
            ll = []
            if prefix:
                ll.append(prefix)
            if ib_component:
                ll.append(ib_component)
            path_template.base_prefix = '_'.join(ll)
            path_template.mad_prefix = (
                filename_params.get('beam_setting_index') or ''
            )
            path_template.wedge_prefix = (
                filename_params.get('gonio_setting_index') or ''
            )
            path_template.start_num = acq_parameters.first_image
            path_template.num_files = acq_parameters.num_images

            data_collection = queue_model_objects.DataCollection([acq], crystal)
            data_collections.append(data_collection)

            data_collection.set_enabled(False)
            data_collection.set_name(path_template.get_prefix())
            data_collection.set_number(path_template.run_number)
            queue_model_hwobj.add_child(new_dcg_model, data_collection)

        data_collection_entry = queue_manager.get_entry_with_model(
            new_dcg_model
        )
        try:
            queue_manager.execute_entry(data_collection_entry)
        except:
            typ, val, trace = sys.exc_info()
            self.emit("GPHL_BEAMLINE_ABORT",
                      "%s raised during data collection" % typ.__name__)
            # self.workflow_connection.abort_workflow(
            #     message="%s raised during data collection" % typ.__name__
            # )
            raise
        else:
            if data_collection_entry.status == QUEUE_ENTRY_STATUS.FAILED:
                # TODO NBNB check if these status codes are corerct
                status = 1
            else:
                status = 0

            # NB, uses last path_template,
            # but directory should be the same for all
            return self.GphlMessages.CollectionDone(
                status=status,
                proposalId=collection_proposal.id,
                imageRoot=path_template.directory
            )



    def select_lattice(self, payload, correlation_id):
        choose_lattice = payload
        #pass
        raise NotImplementedError()

        ## Display solution and query user for lattice

        ## Create SelectedLattice and return it

    def process_centring_request(self, payload, correlation_id):
        request_centring = payload

        logging.info ('Start centring no. %s of %s'
                      % (request_centring.currentSettingNo,
                         request_centring.totalRotations))

        ## Rotate sample to RotationSetting
        goniostatRotation = request_centring.goniostatRotation
        # goniostatTranslation = goniostatRotation.translation
        #
        # # # NBNB it is up to beamline setup etc. to ensure that the
        # # # axis names are correct - and this is what SampleCentring uses
        # # name = 'GPhL_centring_%s' % request_centring.currentSettingNo
        # # sc_model = queue_model_objects.SampleCentring(
        # #     name=name, kappa=axisSettings['kappa'],
        # #     kappa_phi=axisSettings['kappa_phi']
        # # )
        #
        # # NBNB TODO redo when we have a specific diffractometer to work off.
        # diffractometer = self.queue_entry.beamline_setup.getObjectByRole(
        #     "diffractometer"
        # )
        # dd = dict((x, goniostatRotation.axisSettings[x])
        #           for x in self.rotation_axis_roles)
        # if goniostatTranslation is not None:
        #     for tag in self.translation_axis_roles:
        #         val = goniostatTranslation.axisSettings.get(tag)
        #         if val is not None:
        #             dd[tag] = val
        # diffractometer.move_motors(dd)
        # diffractometer.start_centring_method(method=self.centring_method)
        #
        # positionsDict = diffractometer.getPositions()
        # dd = dict((x, positionsDict[x]) for x in self.translation_axis_roles)
        # goniostatTranslation = self.GphlMessages.GoniostatTranslation(
        #     rotation=goniostatRotation,
        #     requestedRotationId= goniostatRotation.id, **dd
        # )

        goniostatTranslation = self.center_sample(goniostatRotation)

        if (request_centring.currentSettingNo >=
                request_centring.totalRotations):
            returnStatus = 'DONE'
        else:
            returnStatus = 'NEXT'
        #
        return self.GphlMessages.CentringDone(
            returnStatus, timestamp=time.time(),
            goniostatTranslation=goniostatTranslation
        )

    def center_sample(self, goniostatRotation, requestedRotationId=None):

        queue_model_hwobj = HardwareRepository().getHardwareObject(
            'queue-model'
        )
        queue_manager = HardwareRepository().getHardwareObject(
            'queue'
        )

        goniostatTranslation = goniostatRotation.translation

        # # NBNB it is up to beamline setup etc. to ensure that the
        # # axis names are correct - and this is what SampleCentring uses
        # name = 'GPhL_centring_%s' % request_centring.currentSettingNo
        # sc_model = queue_model_objects.SampleCentring(
        #     name=name, kappa=axisSettings['kappa'],
        #     kappa_phi=axisSettings['kappa_phi']
        # )

        # NBNB TODO redo when we have a specific diffractometer to work off.
        # diffractometer = self.queue_entry.beamline_setup.getObjectByRole(
        #     "diffractometer"
        # )
        dd = dict((x, goniostatRotation.axisSettings[x])
                  for x in self.rotation_axis_roles)
        if goniostatTranslation is not None:
            for tag in self.translation_axis_roles:
                val = goniostatTranslation.axisSettings.get(tag)
                if val is not None:
                    dd[tag] = val


        # diffractometer.move_motors(dd)
        # diffractometer.start_centring_method(method=self.centring_method)


        centring_model = queue_model_objects.SampleCentring(motor_positions=dd)
        queue_model_hwobj.add_child(self.queue_entry.get_data_model(),
                                    centring_model)
        centring_entry = queue_manager.get_entry_with_model(centring_model)
        try:
            queue_manager.execute_entry(centring_entry)
        except:
            typ, val, trace = sys.exc_info()
            self.emit("GPHL_BEAMLINE_ABORT",
                      "%s raised during data collection" % typ.__name__)
            raise

        centring_result = centring_model.get_centring_result()
        if centring_result:
            positionsDict = centring_result.as_dict()
            dd = dict((x, positionsDict[x])
                      for x in self.translation_axis_roles)
            return self.GphlMessages.GoniostatTranslation(
                rotation=goniostatRotation,
                requestedRotationId=requestedRotationId, **dd
            )
        else:
            self.emit("GPHL_BEAMLINE_ABORT", "No Centring result found")

    def prepare_for_centring(self, payload, correlation_id):

        # TODO Add pop-up confirmation box ('Ready for centring?')

        return self.GphlMessages.ReadyForCentring()

    def obtain_prior_information(self, payload, correlation_id):

        workflow_model = self.queue_entry.get_data_model()
        sample_model = workflow_model.get_sample_node()
        resolution_hwobj = self.queue_entry.beamline_setup.getObjectByRole(
            "resolution"
        )

        crystals = sample_model.crystals
        if crystals:
            crystal = crystals[0]

            unitCell = self.GphlMessages.UnitCell(
                crystal.cell_a, crystal.cell_b, crystal.cell_c,
                crystal.cell_alpha, crystal.cell_beta, crystal.cell_gamma,
            )
            space_group = crystal.space_group
        else:
            unitCell = space_group = None

        # TODO NBNB this must be queried/modified/confirmed by user input
        wavelengths = []
        for role, value in workflow_model.wavelengths.items():
            wavelengths.append(
                self.GphlMessages.PhasingWavelength(wavelength=value, role=role)
            )

        # NBNB TODO Resolution needs to be set. For now take the current value
        resolution = resolution_hwobj.currentResolution

        userProvidedInfo = self.GphlMessages.UserProvidedInfo(
            scatterers=(),
            lattice=None,
            spaceGroup=space_group,
            cell=unitCell,
            expectedResolution=resolution,
            isAnisotropic=None,
            phasingWavelengths=wavelengths
        )
        # NBNB TODO scatterers, lattice, isAnisotropic, phasingWavelengths are
        # not obviously findable and would likely have to be set explicitly
        # in UI. Meanwhile leave them empty

        # TODO needs user interface to take input

        # Look for existing uuid
        for text in sample_model.lims_code, sample_model.code, sample_model.name:
            if text:
                try:
                    existing_uuid = uuid.UUID(text)
                except:
                    # The error expected if this goes wrong is ValueError.
                    # But whatever the error we want to continue
                    pass
                else:
                    # Text was a valid uuid string. Use the uuid.
                    break
        else:
            existing_uuid = None

        # TODO re-check if this is correct
        rootDirectory = workflow_model.path_template.directory

        priorInformation = self.GphlMessages.PriorInformation(
            sampleId=existing_uuid or uuid.uuid1(),
            sampleName=(sample_model.name or sample_model.code
                        or sample_model.lims_code),
            rootDirectory=rootDirectory,
            userProvidedInfo=userProvidedInfo
        )
        #
        return priorInformation
