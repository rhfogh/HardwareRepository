#! /usr/bin/env python
# encoding: utf-8
"""Global phasing workflow runner
"""

__copyright__ = """
  * Copyright © 2016 - 2017 by Global Phasing Ltd.
"""
__author__ = "rhfogh"
__date__ = "06/04/17"

import os
import gevent
import gevent.event
import uuid
import logging
import time
import queue_model_objects_v1 as queue_model_objects
from HardwareRepository.HardwareRepository import dispatcher
from HardwareRepository.BaseHardwareObjects import HardwareObject
from HardwareObjects.GphlWorkflowConnection import GphlWorkflowConnection
from HardwareObjects.General import States


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

    def _init(self):
        pass

    def init(self):

        self.execution_timeout = self.getProperty('execution_timeout')
        workflow_connection = GphlWorkflowConnection()
        dd = (self['connection_parameters'].getProperties()
              if self.hasObject('connection_parameters') else {})
        workflow_connection.init(**dd)
        self.workflow_connection = workflow_connection

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
        # If necessary unblock dialog
        self.queue_entry = None
        if not self._gevent_event.is_set():
            self._gevent_event.set()
        if not self._gphl_process_finished.is_set():
            self._gphl_process_finished.set()
        self.set_state(States.ON)

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

            # Wait for workflow execution to finish
            # Queue child entries are set up and triggered through dispatcher
            final_message = self._gphl_process_finished.get(
                timeout=self.execution_timeout
            )
            if final_message is None:
                final_message = 'Timeout'
                self.abort()
            self.echo_info(final_message)
        finally:
            self.workflow_end()

    # Message handlers:

    def workflow_aborted(self, message_type, workflow_aborted):
        # NB Echo additional content later
        self._gphl_process_finished.set(message_type)

    def workflow_completed(self, message_type, workflow_completed):
        # NB Echo additional content later
        self._gphl_process_finished.set(message_type)

    def workflow_failed(self, message_type, workflow_failed):
        # NB Echo additional content later
        self._gphl_process_finished.set(message_type)

    def echo_info_string(self, info, correlation_id):
        """Print text info to console,. log etc."""
        # TODO implement properly
        subprocess_name = self._server_subprocess_names.get(correlation_id)
        if subprocess_name:
            logging.info ('%s: %s' % (subprocess_name, info))
        else:
            logging.info(info)

    def echo_subprocess_started(self, subprocess_started, correlation_id):
        name =subprocess_started.name
        if correlation_id:
            self._server_subprocess_names[name] = correlation_id
        logging.info('%s : STARTING' % name)

    def echo_subprocess_stopped(self, subprocess_stopped, correlation_id):
        name =subprocess_stopped.name
        if correlation_id in self._server_subprocess_names:
            del self._server_subprocess_names[name]
        logging.info('%s : FINISHED' % name)

    def get_configuration_data(self, request_configuration,
                               correlation_id):
        data_location = self.getProperty('beamline_configuration_directory')
        return self.GphlMessages.ConfigurationData(data_location)

    def setup_data_collection(self, geometric_strategy, correlation_id):
        pass
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


    def collect_data(self, collection_proposal, correlation_id):
        pass

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

    def select_lattice(self, choose_lattice, correlation_id):
        pass
        raise NotImplementedError()

        ## Display solution and query user for lattice

        ## Create SelectedLattice and return it

    def centre_sample(self, request_centring, correlation_id):

        logging.info ('Start centring no. %s of %s'
                      % (request_centring.currentSettingNo,
                         request_centring.totalRotations))

        ## Rotate sample to RotationSetting
        goniostatRotation = request_centring.goniostatRotation
        axisSettings = goniostatRotation.axisSettings

        # NBNB it is up to beamline setup etc. to ensure that the
        # axis names are correct - and this is what SampleCentring uses
        name = 'GPhL_centring_%s' % request_centring.currentSettingNo
        sc_model = queue_model_objects.SampleCentring(
            name=name, kappa=axisSettings['kappa'],
            kappa_phi=axisSettings['kappa_phi']
        )
        # PROBLEM 1 - How do you get from here to a SampleCentring queue item?
        # a.k.a: Why is SampleCentringQueueItem not instantiated anywhere?
        # PROBLEM 2 - how do you put omega positioning on the queue?


        diffractometer = self.getObjectByRole("diffractometer")
        positionsDict = diffractometer.getPositions()
        # # TODO check that axis names match beamline, or translate them
        # diffractometer.moveMotors(axisSettings)


        ## Trigger centring dialogue

        ## When done get translation setting

        ## Create GoniostatTranslation and return CentringDone

        raise NotImplementedError()

    def prepare_for_centring(self, gphl_message, correlation_id):

        raise NotImplementedError()

        return self.GphlMessages.ReadyForCentring()

    def obtain_prior_information(self, gphl_message, correlation_id):

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
        return priorInformation,