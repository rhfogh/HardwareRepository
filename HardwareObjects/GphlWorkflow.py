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
import gevent._threading

import General
from HardwareRepository.BaseHardwareObjects import HardwareObject
from HardwareRepository.HardwareRepository import HardwareRepository

import queue_model_objects_v1 as queue_model_objects
import queue_model_enumerables_v1 as queue_model_enumerables
from queue_entry import QUEUE_ENTRY_STATUS

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

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

        # HO that handles connection to GPhL workflow runner
        self._workflow_connection = None

        # Needed to allow methods to put new actions on the queue
        self._queue_entry = None

        # cache dictionary to handle parameters transferred through signals
        self._gphl_parameters = {}

        # event to handle waiting for parameter input
        self._gevent_event = None

        # Message - processing function map
        self._processor_functions = {}

        # Subprocess names to track which subprocess is getting info
        self._server_subprocess_names = {}

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

        self._gevent_event = gevent.event.Event()

        self.rotation_axis_roles = self.getProperty('rotation_axis_roles').split()
        self.translation_axis_roles = self.getProperty('translation_axis_roles').split()
        self.java_binary = self.getProperty('java_binary')
        self.gphl_subdir = self.getProperty('gphl_subdir')

        workflow_connection = GphlWorkflowConnection()
        dd = (self['connection_parameters'].getProperties()
              if self.hasObject('connection_parameters') else {})
        workflow_connection.init(**dd)
        self._workflow_connection = workflow_connection


        relative_file_path = self.getProperty('gphl_config_subdir')
        self.gphl_beamline_config = HardwareRepository().findInRepository(
            relative_file_path
        )

        # Set up processing functions map
        self._processor_functions = {
            'String':self.echo_info_string,
            'SubprocessStarted':self.echo_subprocess_started,
            'SubprocessStopped':self.echo_subprocess_stopped,
            'RequestConfiguration':self.get_configuration_data,
            'GeometricStrategy':self.setup_data_collection,
            'CollectionProposal':self.collect_data,
            'ChooseLattice':self.select_lattice,
            'RequestCentring':self.process_centring_request,
            'PrepareForCentring':self.prepare_for_centring,
            'ObtainPriorInformation':self.obtain_prior_information,
            'WorkflowAborted':self.workflow_aborted,
            'WorkflowCompleted':self.workflow_completed,
            'WorkflowFailed':self.workflow_failed,
        }


        self.set_state(States.ON)


    def get_available_workflows(self):
        """Get list of workflow description dictionaries."""

        # TODO this could be cached for speed

        result = OrderedDict()
        if self.hasObject('workflow_options'):
            options = self['workflow_options'].getProperties()
        else:
            options = {}
        if self.hasObject('workflow_properties'):
            properties = self['workflow_properties'].getProperties()
        else:
            properties = {}
        if self.hasObject('invocation_options'):
            invocation_options = self['invocation_options'].getProperties()
        else:
            invocation_options = {}
        if self.hasObject('invocation_properties'):
            invocation_properties = self['invocation_properties'].getProperties()
        else:
            invocation_properties = {}

        for wf_node in self['workflows']:
            name = wf_node.name()
            wf_dict = {'name':name,
                       'application':wf_node.getProperty('application'),
                       'documentation':wf_node.getProperty('documentation',
                                                           default_value=''),
                       'collect_data':wf_node.getProperty('collect_data',
                                                           default_value='true')
            }
            result[name] = wf_dict
            wf_dict['options'] = dd = options.copy()
            if wf_node.hasObject('options'):
                dd.update(wf_node['options'].getProperties())
                relative_file_path = dd.get('file')
                if relative_file_path is not None:
                    # Special case - this option must be modified before use
                    dd['file'] = HardwareRepository().findInRepository(
                        relative_file_path
                    )
            wf_dict['properties'] = dd = properties.copy()
            if wf_node.hasObject('properties'):
                dd.update(wf_node['properties'].getProperties())
            wf_dict['invocation_properties'] = dd = invocation_properties.copy()
            if wf_node.hasObject('invocation_properties'):
                dd.update(wf_node['invocation_properties'].getProperties())
            wf_dict['invocation_options'] = dd = invocation_options.copy()
            if wf_node.hasObject('invocation_options'):
                dd.update(wf_node['invocation_options'].getProperties())

            if wf_node.hasObject('wavelengths'):
                wf_dict['wavelengths'] = dd = OrderedDict()
                for wavelength in wf_node['wavelengths']:
                    dd[wavelength.getProperty('role')] = (
                        wavelength.getProperty('value')
                    )
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

        self._queue_entry = None
        if not self._gevent_event.is_set():
            self._gevent_event.set()
        self.set_state(States.ON)
        self._server_subprocess_names.clear()
        self._workflow_connection._workflow_ended()


    def abort(self, message=None):
        logging.getLogger("HWR").info('MXCuBE aborting current GPhL workflow')
        self._workflow_connection.abort_workflow(message=message)

    def execute(self, queue_entry):

        self._queue_entry = queue_entry

        try:
            self.set_state(States.RUNNING)
            if not self._gevent_event.is_set():
                self._gevent_event.set()

            workflow_queue = gevent._threading.Queue()
            # Fork off workflow server process
            self._workflow_connection.start_workflow(workflow_queue,
                                                     queue_entry.get_data_model()
                                                     )

            while True:
                while workflow_queue.empty():
                    time.sleep(0.1)

                tt = workflow_queue.get_nowait()
                if tt is StopIteration:
                    break

                message_type, payload, correlation_id, result_list = tt
                func = self._processor_functions.get(message_type)
                if func is None:
                    logging.getLogger("HWR").error(
                        "GPhL message %s not recognised by MXCuBE. Terminating..."
                        % message_type
                    )
                    break
                else:
                    logging.getLogger("HWR").info("GPhL queue processing %s"
                                                  % message_type)
                    response = func(payload, correlation_id)
                    if result_list is not None:
                        result_list.append((response, correlation_id))

        except:
            logging.getLogger("HWR").error(
                "Uncaught error during GPhL workflow execution",
                exc_info=True
            )
            raise

    # Message handlers:

    def workflow_aborted(self, payload, correlation_id):
        logging.getLogger("user_level_log").info(
            "GPhL Workflow aborted."
        )

    def workflow_completed(self, payload, correlation_id):
        logging.getLogger("user_level_log").info(
            "GPhL Workflow completed."
        )

    def workflow_failed(self, payload, correlation_id):
        logging.getLogger("user_level_log").info(
            "GPhL Workflow failed."
        )

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

        isInterleaved = geometric_strategy.isInterleaved
        allowed_widths = geometric_strategy.allowedWidths
        default_width_index = geometric_strategy.defaultWidthIdx or 0

        # NBNB TODO userModifiable

        orientations = {}
        total_width = 0
        for sweep in geometric_strategy.sweeps:
            total_width += sweep.width
            rotation_id = sweep.goniostatSweepSetting._id
            sweeps = orientations.get(rotation_id, [])
            sweeps.append(sweep)
            orientations[rotation_id] = sweeps

        lines = ["""Geometric strategy:
    Total rotation %d.1 degrees""" % total_width]

        axis_names = self.rotation_axis_roles

        for rotation_id, sweeps in sorted(orientations.items()):
            goniostatRotation = sweeps[0].goniostatSweepSetting
            axis_settings = goniostatRotation.axisSettings
            scan_axis = goniostatRotation.scanAxis
            ss = "\nOrientation: %s" % ', '.join('%s=%s' % (x, axis_settings.get(x))
                                                  for x in axis_names)
            lines.append(ss)
            for sweep in sweeps:
                wavelength = sweep.beamSetting.wavelength
                start = sweep.start
                width = sweep.width
                ss = ("sweep: wavelength=%s, width=%s degrees"
                      % (wavelength, width))
                lines.append(ss)
        info_text = '\n'.join(lines)


        # TODO put user display/query here

        # For now return default values
        field_list = [
            {'variableName':'info_text',
             'uiLabel':'Information',
             'type':'textblock',
             'defaultValue':info_text,
             'textChoices':[str(x) for x in allowed_widths],
             },
            {'variableName':'imageWidth',
             'uiLabel':'Image width',
             'type':'combo',
             'defaultValue':str(allowed_widths[default_width_index]),
             'textChoices':[str(x) for x in allowed_widths],
             },

            # NB Transmissio i9s in % in UI, but in 0-1 in workflow
            {'variableName':'transmission',
             'uiLabel':'Transmission',
             'type':'text',
             'value':'100',
             'unit':'%',
             'lowerBound':0.0,
             'upperBound':100.0,
             },
            {'variableName':'exposure',
             'uiLabel':'Exposure Time',
             'type':'text',
             'value':'0.037',
             'unit':'s',
             'lowerBound':0.0,
             'upperBound':0.1,
             },
        ]
        if isInterleaved:
            field_list.append({'variableName':'wedgeWidth',
                              'uiLabel':'Images per wedge',
                              'type':'text',
                              'value':'10',
                              'unit':'',
                              'lowerBound':0,
                              'upperBound':1000,}
                          )
        # ednaxmlhelper does not work, elementtrees used wrong. Skip this xml!
        # xml_description = General.createDawnBeanDecoderXML(field_list)
        self._gphl_parameters = dict(
            (x['variableName'], x.get('value') or x.get('defaultValue'))
            for x in field_list
        )
        self.emit('parametersNeeded', (field_list, ))
        self._gevent_event.clear()
        while not self._gevent_event.is_set():
            self._gevent_event.wait()
            time.sleep(0.1)

        params = self._gphl_parameters
        result = {}
        tag = 'imageWidth'
        value = params.get(tag)
        if value:
            result[tag] = float(value)
        tag = 'exposure'
        value = params.get(tag)
        if value:
            result[tag] = float(value)
        tag = 'transmission'
        value = params.get(tag)
        if value:
            # Convert from % to fraction
            result[tag] = float(value)/100
        tag = 'wedgeWidth'
        value = params.get(tag)
        if value:
            result[tag] = int(value)

        return result

    def set_gphl_parameters(self, parameter_values):

        logging.getLogger('HWR').debug("Setting GPhL parameters: %s"
                                       % parameter_values)
        self._gphl_parameters.clear()
        self._gphl_parameters.update(parameter_values)
        self._gevent_event.set()

    def get_gphl_parameters(self):
        result =  self._gphl_parameters.copy()
        logging.getLogger('HWR').debug("Getting GPhL parameters: %s" % result)
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

        beamline_setup_hwobj = self._queue_entry.beamline_setup
        resolution_hwobj = self._queue_entry.beamline_setup.getObjectByRole(
            "resolution"
        )
        queue_model_hwobj = HardwareRepository().getHardwareObject(
            'queue-model'
        )
        queue_manager = HardwareRepository().getHardwareObject(
            'queue'
        )

        relative_image_dir = collection_proposal.relativeImageDir

        session = self._queue_entry.beamline_setup.getObjectByRole(
            "session"
        )

        # NBNB TODO for now we are NOT asking for confirmation
        # and NOT allowing the change of relativeImageDir
        # Maybe later

        gphl_workflow_model = self._queue_entry.get_data_model()


        new_dcg_name = 'GPhL Data Collection'
        new_dcg_model = queue_model_objects.TaskGroup()
        new_dcg_model.set_enabled(True)
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
            acq_parameters.resolution = gphl_workflow_model.get_resolution()
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

            data_collection.set_enabled(True)
            data_collection.set_name(path_template.get_prefix())
            data_collection.set_number(path_template.run_number)
            queue_model_hwobj.add_child(new_dcg_model, data_collection)

        data_collection_entry = queue_manager.get_entry_with_model(
            new_dcg_model
        )
        queue_manager.execute_entry(data_collection_entry)

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

        centring_model = queue_model_objects.SampleCentring(motor_positions=dd)
        queue_model_hwobj.add_child(self._queue_entry.get_data_model(),
                                    centring_model)
        centring_entry = queue_manager.get_entry_with_model(centring_model)

        queue_manager.execute_entry(centring_entry)

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
            self.abort("No Centring result found")

    def prepare_for_centring(self, payload, correlation_id):

        # TODO Add pop-up confirmation box ('Ready for centring?')

        return self.GphlMessages.ReadyForCentring()

    def obtain_prior_information(self, payload, correlation_id):

        workflow_model = self._queue_entry.get_data_model()
        sample_model = workflow_model.get_sample_node()

        cp = workflow_model.processing_parameters
        cell_params = list(getattr(cp, x)
                           for x in ['cell_a', 'cell_b', 'cell_c',
                                     'cell_alpha', 'cell_beta', 'cell_gamma']
                           )
        if all(cell_params):
            unitCell = self.GphlMessages.UnitCell(*cell_params)
        else:
            unitCell = None

        space_group = queue_model_enumerables.SPACEGROUP_NUMBERS.get(
            cp.space_group
        )
        point_group = None

        wavelengths = []
        for role, value in workflow_model.get_wavelengths().items():
            wavelengths.append(
                self.GphlMessages.PhasingWavelength(wavelength=value, role=role)
            )

        userProvidedInfo = self.GphlMessages.UserProvidedInfo(
            scatterers=(),
            lattice=None,
            pointGroup=point_group,
            spaceGroup=space_group,
            cell=unitCell,
            expectedResolution=workflow_model.get_resolution(),
            isAnisotropic=None,
            phasingWavelengths=wavelengths
        )

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
