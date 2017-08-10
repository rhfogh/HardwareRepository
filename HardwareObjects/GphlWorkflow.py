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

import gevent
import gevent.event

import General
from HardwareRepository.BaseHardwareObjects import HardwareObject
from HardwareRepository.HardwareRepository import dispatcher
from HardwareRepository.HardwareRepository import HardwareRepository

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

    def _init(self):
        pass

    def init(self):

        # Used only here, so let us keep the import out of the module top
        from GphlWorkflowConnection import GphlWorkflowConnection

        self.execution_timeout = self.getProperty('execution_timeout')

        self.rotation_axis_roles = self.getProperty('rotation_axis_roles').split()
        self.translation_axis_roles = self.getProperty('translation_axis_roles').split()


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
        dispatcher.connect(self.centre_sample,
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

    def setup_data_collection(self, payload, correlation_id):
        geometric_strategy = payload
        raise NotImplementedError()

        ## Display GeometricStrategy, with RotationSetting ID.

        ## Query imageWidth, transmission, exposure and wedgeWidth
        ## depending on values for userModifiable and isInterleaved.

        ## Create SampleCentred object and set user entered values

        # NBNB psdeudocode
        goniostatRotationIds = set()
        for sweep in geometric_strategy.sweeps:
            setting = sweep.goniostatSweepSetting
            if setting.ID not in goniostatRotationIds:
                goniostatRotationIds.add(setting.ID)
                ## Rotate sample to setting
                ## Optionally translate to attached translation setting
                ## Query user for alternative rotation
                ## If alternative rotation create new setting object
                ## and rotate to new setting
                ## Trigger centring dialogue
                ## If translation or rotation setting is changed
                ## (at first: ALWAYS) then:
                ##   Create GoniostatTranslation
                ##   and add it to SampleCentred.goniostatTranslations

        ## Return SampleCentred


    def collect_data(self, payload, correlation_id):
        collection_proposal = payload

        ## Display collection proposal in suitable form
        ## Query  relativeImageDir,
        ## and ask for go/nogo decision

        # NBNB pseudocode
        for scan in collection_proposal.scans:
            pass
            ## rotate to scan.sweep.goniostatSweepSetting position
            ## and translate to corresponding translation position

            ## Set beam, detector and beamstop
            ## set up acquisition and acquire

            ## NB the entire sequence can be put on the queue at once
            ## provided the motor movements can be  queued.

        ## return collectionDone
        raise NotImplementedError()

    def select_lattice(self, payload, correlation_id):
        choose_lattice = payload
        #pass
        raise NotImplementedError()

        ## Display solution and query user for lattice

        ## Create SelectedLattice and return it

    def centre_sample(self, payload, correlation_id):
        request_centring = payload

        logging.info ('Start centring no. %s of %s'
                      % (request_centring.currentSettingNo,
                         request_centring.totalRotations))

        ## Rotate sample to RotationSetting
        goniostatRotation = request_centring.goniostatRotation
        axisSettings = goniostatRotation.axisSettings

        # # NBNB it is up to beamline setup etc. to ensure that the
        # # axis names are correct - and this is what SampleCentring uses
        # name = 'GPhL_centring_%s' % request_centring.currentSettingNo
        # sc_model = queue_model_objects.SampleCentring(
        #     name=name, kappa=axisSettings['kappa'],
        #     kappa_phi=axisSettings['kappa_phi']
        # )

        # NBNB TODO redo when we have a specific diffractometer to work off.
        diffractometer = self.queue_entry.beamline_setup.getObjectByRole(
            "diffractometer"
        )
        dd = dict((x, axisSettings[x]) for x in self.rotation_axis_roles)
        diffractometer.move_motors(dd)
        diffractometer.start_2D_centring()

        positionsDict = diffractometer.getPositions()
        dd = dict((x, positionsDict[x]) for x in self.translation_axis_roles)
        goniostatTranslation = self.GphlMessages.GoniostatTranslation(
            rotation=goniostatRotation,
            requestedRotationId= goniostatRotation.id, **dd
        )
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

    def prepare_for_centring(self, payload, correlation_id):

        # TODO Add pop-up confirmation box ('Ready for centring?')

        return self.GphlMessages.ReadyForCentring()

    def obtain_prior_information(self, payload, correlation_id):

        sample_node_id = self.dictParameters.get('sample_node_id')
        queue_model = self.getObjectByRole("QueueModel")
        sample_model = queue_model.get_node(sample_node_id)

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

        userProvidedInfo = self.GphlMessages.UserProvidedInfo(
            scatterers=(),
            lattice=None,
            spaceGroup=space_group,
            cell=unitCell,
            expectedResolution=None,
            isAnisotropic=None,
            phasingWavelengths=()
        )
        # NB scatterers, lattice, isAnisotropic, phasingWavelengths,
        # and expectedResolution are
        # not obviously findable and would likely have to be set explicitly
        # in UI. Meanwhile leave them empty

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

        # TODO check if this is correct
        rootDirectory = self.path_template.get_archive_directory()

        priorInformation = self.GphlMessages.PriorInformation(
            sampleId=existing_uuid or uuid.uuid1(),
            sampleName=(sample_model.name or sample_model.code
                        or sample_model.lims_code),
            rootDirectory=rootDirectory,
            userProvidedInfo=userProvidedInfo
        )
        #
        return priorInformation
