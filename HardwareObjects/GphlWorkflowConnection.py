# encoding: utf-8
""""Global Phasing py4j workflow server connection"""

__copyright__ = """
  * Copyright © 2016 - ${YEAR} by Global Phasing Ltd. All rights reserved
"""
__author__ = "rhfogh"
__date__ = "04/11/16"

import logging
import os
import socket
import subprocess
import uuid
import signal
import time

import gevent.monkey
import gevent.event
from py4j import clientserver

import General
import GphlMessages

States = General.States
try:
    # Needed for 3.6(?) onwards
    from importlib import reload
except ImportError:
    # Works for earlier versions, including Python 2.6
    from imp import reload

try:
    # This file already does the alternative imports plus some tweaking
    # TODO It ought to be moved out as an accessible Util file, but meanwhile
    # Here we take care of the case where it is missing.
    from HardwareRepository.dispatcher import dispatcher
except ImportError:
    try:
        from louie import dispatcher
    except ImportError:
        from pydispatch import dispatcher
        from pydispatch import robustapply
        from pydispatch import saferef
        saferef.safe_ref = saferef.safeRef
        robustapply.robust_apply = robustapply.robustApply


class GphlWorkflowConnection(object):
# class GphlWorkflowConnection(object):
    """
    This HO acts as a gateway to the Global Phasing workflow engine.
    """

    # object states
    valid_states = [
        States.OFF,     # Not connected to remote server
        States.ON,      # Connected, inactive, awaiting start (or disconnect)
        States.RUNNING, # Server is active and will produce next message
        States.OPEN,    # Server is waiting for a message from the beamline
    ]
    
    def __init__(self):

        # Py4J gateway to external workflow program
        self._gateway = None

        # ID for current workflow calculation
        self._enactment_id = None

        # Name of workflow being executed.
        self._workflow_name = None

        # Queue for communicating with MXCuBE HardwareObject
        self.workflow_queue = None
        self._await_result = None
        self._running_process = None

        self._state = States.OFF

        # py4j connection parameters
        self.python_address = None
        self.python_port = None
        self.java_address = None
        self.java_port = None

        
    def _init(self):
        pass

    def init(self, python_address=None, python_port=None,
             java_address=None, java_port=None):

        self.python_address = python_address
        self.python_port = python_port
        self.java_address = java_address
        self.java_port = java_port

        self._open_connection()

    def get_state(self):
        """Returns a member of the General.States enumeration"""
        return self._state

    def set_state(self, value):
        if value in self.valid_states:
            self._state = value
            dispatcher.send('stateChanged', self, self._state)
        else:
            raise RuntimeError("GphlWorkflowConnection set to invalid state: %s"
                               % value)

    def get_workflow_name(self):
        """Name of currently executing workflow"""
        return self._workflow_name

    def to_java_time(self, time):
        """Convert time in seconds since the epoch (python time) to Java time value"""
        return self._gateway.jvm.java.lang.Long(int(time*1000))

    def _open_connection(self):

        python_parameters = {}
        val = self.python_address
        if val is not None:
            python_parameters['address'] = val
        val = self.python_port
        if val is not None:
            python_parameters['port'] = val

        java_parameters = {'auto_convert':True}
        val = self.java_address
        if val is not None:
            java_parameters['address'] = val
        val = self.java_port
        if val is not None:
            java_parameters['port'] = val

        logging.getLogger('HWR').debug("GPhL Open connection %s %s %s %s"
                                       % (self.python_address, self.python_port,
                                          self.java_address, self.java_port))

        # set sockets and threading to standard before running py4j
        # NBNB this can cause ERRORS if socket or thread have been
        # patched with non-default parameters
        # It is the best we can do, though
        #
        # These should use is_module_patched,
        # but that is not available in gevent 1.0
        socket_patched = 'socket' in gevent.monkey.saved
        reload(socket)
        try:
            self._gateway = clientserver.ClientServer(
                java_parameters=clientserver.JavaParameters(**java_parameters),
                python_parameters=clientserver.PythonParameters(
                    **python_parameters),
                python_server_entry_point=self)
        finally:
            # patch back to starting state
            if socket_patched:
                gevent.monkey.patch_socket()

    def start_workflow(self, workflow_queue, workflow_model_obj):

        self.workflow_queue = workflow_queue

        if self.get_state() != States.OFF:
            # NB, for now workflow is started as the connection is made,
            # so we are never in state 'ON'/STANDBY
            raise RuntimeError("Workflow is already running, cannot be started")

        self._workflow_name = workflow_model_obj.get_type()

        # TODO Here we make and send the workflow start-run message
        # NB currently done under 'wfrun' alias
        #  This forks off the server process and returns None
        commandList = [workflow_model_obj.java_binary]

        for keyword, value in workflow_model_obj.get_invocation_properties().items():
            commandList.extend(General.javaProperty(keyword, value))

        for keyword, value in workflow_model_obj.get_invocation_options().items():
            commandList.extend(General.commandOption(keyword, value))

        commandList.append(workflow_model_obj.invocation_classname)

        for keyword, value in workflow_model_obj.get_workflow_properties().items():
            commandList.extend(General.javaProperty(keyword, value))

        workflow_options = workflow_model_obj.get_workflow_options()
        calibration_name = workflow_options.get('calibration')
        if calibration_name:
            # Expand calibration base name
            workflow_options['calibration'] = (
                '%s_%s' % (calibration_name,  workflow_model_obj.get_name())
            )
        for keyword, value in workflow_options.items():
            commandList.extend(General.commandOption(keyword, value))
        #
        wdir = workflow_options.get('wdir')
        if not os.path.isdir(wdir):
            try:
                os.makedirs(wdir)
            except:
                # No need to raise error - program will fail downstream
                logging.getLogger('HWR').error(
                    "Could not create GPhL working directory: %s" % wdir
                )


        for ss in commandList:
            ss = ss.split('=')[-1]
            if ss.startswith('/') and not '*' in ss and not os.path.exists(ss):
                logging.getLogger('HWR').warning(
                    "File does not exist : %s" % ss
                )

        logging.getLogger('HWR').info("GPhL execute :\n%s" % ' '.join(commandList))

        try:
            self._running_process = subprocess.Popen(commandList, stdout=None,
                                                     stderr=None)
        except:
            logging.getLogger().error('Error in spawning workflow application')
            raise

        self.set_state(States.RUNNING)

        logging.getLogger('HWR').debug("GPhL workflow pid, returncode : %s, %s"
                                       % (self._running_process.pid,
                                          self._running_process.returncode))

    def _workflow_ended(self):
        logging.getLogger('HWR').debug("GPhL workflow ended")

        self._enactment_id = None
        self._workflow_name = None
        self.workflow_queue = None
        self._await_result = None
        self.set_state(States.OFF)

        xx = self._running_process
        if xx is not None:
            try:
                if xx.poll() is not None:
                    xx.send_signal(signal.SIGINT)
                    time.sleep(3)
                    if xx.poll() is not None:
                        xx.terminate()
                        time.sleep(9)
                        if xx.poll() is not None:
                            xx.kill()
            except:
                logging.getLogger('HWR').info(
                    "Exception while terminating external workflow process %s"
                    % xx)
                logging.getLogger('HWR').info("Error was:",
                    exc_info=True)
            self._running_process = None

    def _close_connection(self):

        # TODO - should maybe be called when object is deleted?
        # NBNB currently not called

        logging.getLogger('HWR').debug("GPhL Close connection ")
        xx = self._gateway
        if xx is not None:
            try:
                # Exceptions 'can easily happen' (py4j docs)
                # We prefer to catch them here than to have them caught and echoed downstream
                xx.shutdown(raise_exception=True)
            except:
                logging.getLogger('HWR').debug(
                    "Exception during py4j gateway shutdown. Ignored"
                )
            self._gateway = None

    def abort_workflow(self, message=None):
        """Abort workflow - may be called from controller in any state"""

        logging.getLogger('HWR').info("Aborting workflow: %s" % message)
        logging.getLogger('user_level_log').info("Aborting workflow ...")


        if self._await_result is not None:
            # Workflow waiting for answer - send abort
            self._await_result = [(GphlMessages.BeamlineAbort(), None)]

        # Shut down hardware object
        qu = self.workflow_queue
        if qu is None:
            self._workflow_ended()
        else:
            # If the queue is running,
            # workflow_ended will be called from post_execute
            qu.put_nowait(StopIteration)


    def processText(self, py4jMessage):
        """Receive and process info message from workflow server
        Return goes to server"""
        xx = self._decode_py4j_message(py4jMessage)
        message_type = xx.message_type
        payload = xx.payload
        correlation_id = xx.correlation_id
        enactment_id = xx.enactment_id

        if not payload:
            logging.getLogger('HWR').warning(
                "GPhL Empty or unparsable information message. Ignored"
            )

        else:
            if not enactment_id:
                logging.getLogger('HWR').warning(
                    "GPhL information message lacks enactment ID:"
                )
            elif self._enactment_id != enactment_id:
                logging.getLogger('HWR').warning(
                    "Workflow enactment I(D %s != info message enactment ID %s."
                    % (self._enactment_id, enactment_id)
                    )

            self.workflow_queue.put_nowait((message_type, payload,
                                            correlation_id, None))
        #
        return None

    def processMessage(self, py4jMessage):
        """Receive and process message from workflow server
        Return goes to server"""

        xx = self._decode_py4j_message(py4jMessage)
        message_type = xx.message_type
        payload = xx.payload
        correlation_id = xx.correlation_id
        enactment_id = xx.enactment_id
        
        
        if not enactment_id:
            logging.getLogger('HWR').error(
                "GPhL message lacks enactment ID - sending 'Abort' to external workflow"
            )
            return self._response_to_server(GphlMessages.BeamlineAbort(),
                                            correlation_id)

        elif self._enactment_id is None:
            # NB this should be made less primitive
            # once we are past direct function calls
            self._enactment_id = enactment_id

        elif self._enactment_id != enactment_id:
            logging.getLogger('HWR').error(
                "Workflow enactment ID %s != message enactment ID %s"
                " - sending 'Abort' to external workflow"
                % (self._enactment_id, enactment_id)
            )
            return self._response_to_server(GphlMessages.BeamlineAbort(),
                                            correlation_id)

        elif not payload:
            logging.getLogger('HWR').error(
                "GPhL message lacks payload - sending 'Abort' to external workflow"
            )
            return self._response_to_server(GphlMessages.BeamlineAbort(),
                                            correlation_id)

        if  message_type in ('SubprocessStarted', 'SubprocessStopped'):

            self.workflow_queue.put_nowait((message_type, payload,
                                            correlation_id, None))
            return None

        elif  message_type in ('RequestConfiguration',
                             'GeometricStrategy',
                             'CollectionProposal',
                             'ChooseLattice',
                             'RequestCentring',
                             'ObtainPriorInformation',
                             'PrepareForCentring'):
            # Requests:
            self._await_result = []
            self.set_state(States.OPEN)
            self.workflow_queue.put_nowait((message_type, payload,
                                            correlation_id, self._await_result))
            while not self._await_result:
                time.sleep(0.1)
            result, correlation_id = self._await_result.pop(0)
            self._await_result = None
            self.set_state(States.RUNNING)
            return self._response_to_server(result, correlation_id)

        elif message_type in ('WorkflowAborted',
                              'WorkflowCompleted',
                              'WorkflowFailed'):
            self.workflow_queue.put_nowait((message_type, payload,
                                            correlation_id, None))
            self.workflow_queue.put_nowait(StopIteration)
            return None

        else:
            logging.getLogger('HWR').error(
                "GPhL Unknown message type: %s - aborting" % message_type
            )
            return self._response_to_server(GphlMessages.BeamlineAbort(),
                                            correlation_id)

    def _extractResponse(self, responses, message_type):
        result = abort_message = None

        validResponses = [tt for tt in responses if tt[1] is not None]
        if not validResponses:
            abort_message = "No valid response to %s request" % message_type
        elif len(validResponses) == 1:
            result =  validResponses[0][1]
        else:
            abort_message = ("Too many responses to %s request"
                             % message_type)
        #
        return result, abort_message

    #Conversion to Python

    def _decode_py4j_message(self, py4jMessage):
        """Extract messageType and convert py4J object to python object"""

        # Determine message type
        message_type = py4jMessage.getPayloadClass().getSimpleName()

        xx = py4jMessage.getEnactmentId()
        enactment_id = xx and xx.toString()

        xx = py4jMessage.getCorrelationId()
        correlation_id = xx and xx.toString()
        logging.getLogger('HWR').debug(
            "GPhL incoming: message=%s, jobId=%s,  messageId=%s"
            % (message_type, enactment_id, correlation_id)
        )

        if message_type == 'String':
            payload =  py4jMessage.getPayload()

        else:
            if message_type.endswith('Impl'):
                message_type = message_type[:-4]
            converterName = '_%s_to_python' % message_type

            try:
                # determine converter function
                converter = getattr(self, converterName)
            except AttributeError:
                logging.getLogger('HWR').error(
                    "GPhL Message type %s not recognised (no %s function)"
                    % (message_type, converterName)
                )
                payload = None
            else:
                try:
                    # Convert to Python objects
                    payload = converter(py4jMessage.getPayload())
                except NotImplementedError:
                    logging.getLogger('HWR').error(
                        'Processing of GPhL message %s not implemented'
                        % message_type
                    )
                    payload = None
        #
        return GphlMessages.ParsedMessage(message_type, payload,
                                          enactment_id, correlation_id)

    def _RequestConfiguration_to_python(self, py4jRequestConfiguration):
        return GphlMessages.RequestConfiguration()

    def _ObtainPriorInformation_to_python(self, py4jObtainPriorInformation):
        return GphlMessages.ObtainPriorInformation()

    def _PrepareForCentring_to_python(self, py4jPrepareForCentring):
        return GphlMessages.PrepareForCentring()

    def _GeometricStrategy_to_python(self, py4jGeometricStrategy):
        uuidString = py4jGeometricStrategy.getId().toString()
        sweeps = frozenset(self._Sweep_to_python(x)
                           for x in py4jGeometricStrategy.getSweeps()
                           )
        return GphlMessages.GeometricStrategy(
            isInterleaved=py4jGeometricStrategy.isInterleaved(),
            isUserModifiable=py4jGeometricStrategy.isUserModifiable(),
            allowedWidths=py4jGeometricStrategy.getAllowedWidths(),
            defaultWidthIdx=py4jGeometricStrategy.getDefaultWidthIdx(),
            sweeps=sweeps,
            id=uuid.UUID(uuidString)
        )

    def _SubprocessStarted_to_python(self, py4jSubprocessStarted):
        return GphlMessages.SubprocessStarted(
            name=py4jSubprocessStarted.getName()
        )

    def _SubprocessStopped_to_python(self, py4jSubprocessStopped):
        return GphlMessages.SubprocessStopped()

    def _ChooseLattice_to_python(self, py4jChooseLattice):
        format = py4jChooseLattice.getFormat().toString()
        solutions = py4jChooseLattice.getSolutions()
        lattices = py4jChooseLattice.getLattice()
        return GphlMessages.ChooseLattice(format=format, solutions=solutions,
                                          lattices=lattices)

    def _CollectionProposal_to_python(self, py4jCollectionProposal):
        uuidString = py4jCollectionProposal.getId().toString()
        strategy = self._GeometricStrategy_to_python(
            py4jCollectionProposal.getStrategy()
        )
        id2Sweep = dict((str(x.id),x) for x in strategy.sweeps)
        scans = []
        for py4jScan in py4jCollectionProposal.getScans():
            sweep = id2Sweep[py4jScan.getSweep().getId().toString()]
            scans.append(self._Scan_to_python(py4jScan, sweep))
        return GphlMessages.CollectionProposal(
            relativeImageDir=py4jCollectionProposal.getRelativeImageDir(),
            strategy=strategy,
            scans=scans,
            id=uuid.UUID(uuidString)
        )

    def __WorkflowDone_to_python(self, py4jWorkflowDone, cls):
        Issue = GphlMessages.Issue
        issues = []
        for py4jIssue in py4jWorkflowDone.getIssues():
            component = py4jIssue.getComponent()
            message = py4jIssue.getMessage()
            code = py4jIssue.getCode()
            issues.append(Issue(component=component, message=message,
                                code=code))
        #
        return cls(issues=issues)

    def _WorkflowCompleted_to_python(self, py4jWorkflowCompleted):
        return self.__WorkflowDone_to_python(py4jWorkflowCompleted,
                                             GphlMessages.WorkflowCompleted)

    def _WorkflowAborted_to_python(self, py4jWorkflowAborted):
        return self.__WorkflowDone_to_python(py4jWorkflowAborted,
                                             GphlMessages.WorkflowAborted)

    def _WorkflowFailed_to_python(self, py4jWorkflowFailed):
        return self.__WorkflowDone_to_python(py4jWorkflowFailed,
                                             GphlMessages.WorkflowFailed)

    def _RequestCentring_to_python(self, py4jRequestCentring):
        goniostatRotation = self._GoniostatRotation_to_python(
            py4jRequestCentring.getGoniostatRotation()
        )
        return GphlMessages.RequestCentring(
            currentSettingNo=py4jRequestCentring.getCurrentSettingNo(),
            totalRotations=py4jRequestCentring.getTotalRotations(),
            goniostatRotation=goniostatRotation
        )

    def _GoniostatRotation_to_python(self, py4jGoniostatRotation):
        if py4jGoniostatRotation is None:
            return None

        # NB the translation link is NOT converted.
        # If present it will(must) be set when the GoniostatTranslation is
        # created

        uuidString = py4jGoniostatRotation.getId().toString()

        axisSettings = py4jGoniostatRotation.getAxisSettings()
        #
        return GphlMessages.GoniostatRotation(id=uuid.UUID(uuidString),
                                          **axisSettings)

    def _BeamstopSetting_to_python(self, py4jBeamstopSetting):
        if py4jBeamstopSetting is None:
            return None
        uuidString = py4jBeamstopSetting.getId().toString()
        axisSettings = py4jBeamstopSetting.getAxisSettings()
        #
        return GphlMessages.BeamstopSetting(id=uuid.UUID(uuidString),
                                        **axisSettings)

    def _DetectorSetting_to_python(self, py4jDetectorSetting):
        if py4jDetectorSetting is None:
            return None
        uuidString = py4jDetectorSetting.getId().toString()
        axisSettings = py4jDetectorSetting.getAxisSettings()
        #
        return GphlMessages.DetectorSetting(id=uuid.UUID(uuidString),
                                        **axisSettings)

    def _BeamSetting_to_python(self, py4jBeamSetting):
        if py4jBeamSetting is None:
            return None
        uuidString = py4jBeamSetting.getId().toString()
        #
        return GphlMessages.BeamSetting(id=uuid.UUID(uuidString),
                                    wavelength=py4jBeamSetting.getWavelength())


    def _GoniostatSweepSetting_to_python(self, py4jGoniostatSweepSetting):
        if py4jGoniostatSweepSetting is None:
            return None
        uuidString = py4jGoniostatSweepSetting.getId().toString()
        axisSettings = py4jGoniostatSweepSetting.getAxisSettings()
        scanAxis = py4jGoniostatSweepSetting.getScanAxis()
        return GphlMessages.GoniostatSweepSetting(id=uuid.UUID(uuidString),
                                          scanAxis=scanAxis,
                                          **axisSettings)

    def _Sweep_to_python(self, py4jSweep):

        # NB scans are not set - where scans are present in a message,
        # the link is set from the Scan side.

        uuidString = py4jSweep.getId().toString()
        return GphlMessages.Sweep(
            goniostatSweepSetting=self._GoniostatSweepSetting_to_python(
                py4jSweep.getGoniostatSweepSetting()
            ),
            detectorSetting=self._DetectorSetting_to_python(
                py4jSweep.getDetectorSetting()
            ),
            beamSetting=self._BeamSetting_to_python(
                py4jSweep.getBeamSetting()
            ),
            start=py4jSweep.getStart(),
            width=py4jSweep.getWidth(),
            beamstopSetting=self._BeamstopSetting_to_python(
                py4jSweep.getBeamstopSetting()
            ),
            sweepGroup=py4jSweep.getSweepGroup(),
            id=uuid.UUID(uuidString)
        )

    def _ScanExposure_to_python(self, py4jScanExposure):
        uuidString = py4jScanExposure.getId().toString()
        return GphlMessages.ScanExposure(
            time=py4jScanExposure.getTime(),
            transmission=py4jScanExposure.getTransmission(),
            id=uuid.UUID(uuidString)
        )

    def _ScanWidth_to_python(self, py4jScanWidth):
        uuidString = py4jScanWidth.getId().toString()
        return GphlMessages.ScanWidth(
            imageWidth=py4jScanWidth.getImageWidth(),
            numImages=py4jScanWidth.getNumImages(),
            id=uuid.UUID(uuidString)
        )

    def _Scan_to_python(self, py4jScan, sweep):
        uuidString = py4jScan.getId().toString()
        return GphlMessages.Scan(
            width=self._ScanWidth_to_python(py4jScan.getWidth()),
            exposure=self._ScanExposure_to_python(py4jScan.getExposure()),
            imageStartNum=py4jScan.getImageStartNum(),
            start=py4jScan.getStart(),
            sweep=sweep,
            filenameParams=py4jScan.getFilenameParams(),
            id=uuid.UUID(uuidString)
        )


    # Conversion to Java

    def _payload_to_java(self, payload):
        """Convert Python payload object to java"""

        payloadType = payload.__class__.__name__

        if payloadType == 'ConfigurationData':
            return self._ConfigurationData_to_java(payload)

        elif payloadType == 'BeamlineAbort':
            return self._BeamlineAbort_to_java(payload)

        elif payloadType == 'ReadyForCentring':
            return self._ReadyForCentring_to_java(payload)

        elif payloadType == 'SampleCentred':
            return self._SampleCentred_to_java(payload)

        elif payloadType == 'CollectionDone':
            return self._CollectionDone_to_java(payload)

        elif payloadType == 'SelectedLattice':
            return self._SelectedLattice_to_java(payload)

        elif payloadType == 'CentringDone':
            return self._CentringDone_to_java(payload)

        elif payloadType == 'PriorInformation':
            return self._PriorInformation_to_java(payload)

        else:
            raise ValueError("Payload %s not supported for conversion to java"
                             % payloadType)


    def _response_to_server(self, payload, correlation_id):
        """Create py4j message from py4j wrapper and current ids"""

        if self._enactment_id is None:
            enactment_id = None
        else:
            enactment_id = self._gateway.jvm.java.util.UUID.fromString(
                self._enactment_id
            )

        if correlation_id is not None:
            correlation_id = self._gateway.jvm.java.util.UUID.fromString(
                correlation_id
            )

        logging.getLogger('HWR').debug(
            "GPhL - response=%s jobId=%s messageId=%s"
            % (payload.__class__.__name__, enactment_id, correlation_id)
        )

        py4j_payload = self._payload_to_java(payload)

        try:
            response = self._gateway.jvm.co.gphl.sdcp.py4j.Py4jMessage(
                py4j_payload, enactment_id, correlation_id
            )
        except:
            self.abort_workflow(message="Error sending reply (%s) to server"
                                % py4j_payload.getClass().getSimpleName())
        else:
            return response

    def _CentringDone_to_java(self, centringDone):
        return self._gateway.jvm.astra.messagebus.messages.information.CentringDoneImpl(
            self._gateway.jvm.co.gphl.beamline.v2_unstable.instrumentation.CentringStatus.valueOf(
                centringDone.status
            ),
            self.to_java_time(centringDone.timestamp),
            self._GoniostatTranslation_to_java(
                centringDone.goniostatTranslation
            )
        )

    def _ConfigurationData_to_java(self, configurationData):
        return self._gateway.jvm.astra.messagebus.messages.information.ConfigurationDataImpl(
            self._gateway.jvm.java.io.File(configurationData.location)
        )

    def _ReadyForCentring_to_java(self, readyForCentring):
        return self._gateway.jvm.astra.messagebus.messages.control.ReadyForCentringImpl(
        )

    def _BeamlineAbort_to_java(self, beamlineAbort):
        return self._gateway.jvm.astra.messagebus.messages.information.BeamlineAbort(
        )

    def _PriorInformation_to_java(self, priorInformation):

        builder = self._gateway.jvm.astra.messagebus.messages.information.PriorInformationImpl.Builder(
            self._gateway.jvm.java.util.UUID.fromString(
                str(priorInformation.sampleId)
            )
        )
        xx = priorInformation.sampleName
        if xx:
            builder = builder.sampleName(xx)
        xx = priorInformation.rootDirectory
        if xx:
            builder = builder.sampleName(xx)
        # images not implemented yet - awaiting uses
        # indexingResults not implemented yet - awaiting uses
        builder = builder.userProvidedInfo(
            self._UserProvidedInfo_to_java(priorInformation.userProvidedInfo)
        )
        #
        return builder.build()

    def _SampleCentred_to_java(self, sampleCentred):

        cls = self._gateway.jvm.astra.messagebus.messages.information.SampleCentredImpl

        if sampleCentred.interleaveOrder:
            result = cls(General.int2Float(sampleCentred.imageWidth),
                         sampleCentred.wedgeWidth,
                         General.int2Float(sampleCentred.exposure),
                         General.int2Float(sampleCentred.transmission),
                         list(sampleCentred.interleaveOrder)
                         # self._gateway.jvm.String(sampleCentred.interleaveOrder).toCharArray()
                         )
        else:
            result = cls(General.int2Float(sampleCentred.imageWidth),
                         General.int2Float(sampleCentred.exposure),
                         General.int2Float(sampleCentred.transmission)
                         )

        beamstopSetting = sampleCentred.beamstopSetting
        if beamstopSetting is not None:
            result.setBeamstopSetting(
                self._BeamstopSetting_to_java(beamstopSetting)
            )

        translationSettings = sampleCentred.goniostatTranslations
        if translationSettings:
            result.setGoniostatTranslations(
                list(self._GoniostatTranslation_to_java(x)
                     for x in translationSettings)
            )
        #
        return result

    def _CollectionDone_to_java(self, collectionDone):
        proposalId = self._gateway.jvm.java.util.UUID.fromString(
            str(collectionDone.proposalId)
        )
        return self._gateway.jvm.astra.messagebus.messages.information.CollectionDoneImpl(
            proposalId, collectionDone.imageRoot, collectionDone.status
        )

    def _SelectedLattice_to_java(self, selectedLattice):
        javaFormat = self._gateway.jvm.co.gphl.beamline.v2_unstable.domain_types.IndexingFormat.valueOf(
            selectedLattice.format
        )
        return self._gateway.jvm.astra.messagebus.messages.information.SelectedLatticeImpl(
            javaFormat, selectedLattice.solution
        )

    def _BeamlineAbort_to_java(self, beamlineAbort):
        return self._gateway.jvm.astra.messagebus.messages.instructions.BeamlineAbortImpl()


    def _UserProvidedInfo_to_java(self, userProvidedInfo):

        if userProvidedInfo is None:
            return None

        builder = self._gateway.jvm.astra.messagebus.messages.information.UserProvidedInfoImpl.Builder()

        for scatterer in userProvidedInfo.scatterers:
            builder = builder.addScatterer(
                self._AnomalousScatterer_to_java(scatterer)
            )
        if userProvidedInfo.lattice:
            builder = builder.lattice(
                self._gateway.jvm.co.gphl.beamline.v2_unstable.domain_types.CrystalSystem.valueOf(
                    userProvidedInfo.lattice
                )
            )
        xx = userProvidedInfo.spaceGroup
        if xx:
            builder = builder.spaceGroup(xx)
        xx = userProvidedInfo.cell
        if xx is not None:
            builder = builder.cell(
                self._UnitCell_to_java(xx)
            )
        if userProvidedInfo.expectedResolution:
            builder = builder.expectedResolution(
                General.int2Float(userProvidedInfo.expectedResolution)
            )
        xx = userProvidedInfo.isAnisotropic
        if xx is not None:
            builder = builder.anisotropic(xx)
        for phasingWavelength in userProvidedInfo.phasingWavelengths:
            builder.addPhasingWavelength(
                self._PhasingWavelength_to_java(phasingWavelength)
            )
        #
        return builder.build()

    def _AnomalousScatterer_to_java(self, anomalousScatterer):

        if anomalousScatterer is None:
            return None

        jvm_beamline = self._gateway.jvm.co.gphl.beamline.v2_unstable

        py4jElement = jvm_beamline.domain_types.ChemicalElement.valueOf(
            anomalousScatterer.element
        )
        py4jEdge = jvm_beamline.domain_types.AbsorptionEdge.valueOf(
            anomalousScatterer.edge
        )
        return self._gateway.jvm.astra.messagebus.messages.domain_types.AnomalousScattererImpl(
            py4jElement, py4jEdge
        )

    def _UnitCell_to_java(self, unitCell):

        if unitCell is None:
            return None

        lengths = [General.int2Float(x) for x in unitCell.lengths]
        angles = [General.int2Float(x) for x in unitCell.angles]
        return self._gateway.jvm.astra.messagebus.messages.domain_types.UnitCellImpl(
            lengths[0], lengths[1], lengths[2], angles[0], angles[1], angles[2]
        )

    def _PhasingWavelength_to_java(self, phasingWavelength):

        if phasingWavelength is None:
            return None

        javaUuid = self._gateway.jvm.java.util.UUID.fromString(
            str(phasingWavelength.id)
        )
        return self._gateway.jvm.astra.messagebus.messages.information.PhasingWavelengthImpl(
            javaUuid, General.int2Float(phasingWavelength.wavelength),
            phasingWavelength.role
        )

    def _GoniostatTranslation_to_java(self, goniostatTranslation):

        if goniostatTranslation is None:
            return None

        gts = goniostatTranslation
        javaUuid = self._gateway.jvm.java.util.UUID.fromString(str(gts.id))
        javaRotationId = self._gateway.jvm.java.util.UUID.fromString(
            str(gts.requestedRotationId)
        )
        axisSettings = dict(((x, General.int2Float(y))
                             for x,y in gts.axisSettings.items()))
        newRotation = gts.newRotation
        if newRotation:
            javaNewRotation = self._GoniostatRotation_to_java(newRotation)
            return self._gateway.jvm.astra.messagebus.messages.instrumentation.GoniostatTranslationImpl(
                axisSettings, javaUuid, javaRotationId, javaNewRotation
            )
        else:
            return self._gateway.jvm.astra.messagebus.messages.instrumentation.GoniostatTranslationImpl(
                axisSettings, javaUuid, javaRotationId
            )

    def _GoniostatRotation_to_java(self, goniostatRotation):

        if goniostatRotation is None:
            return None

        grs = goniostatRotation
        javaUuid = self._gateway.jvm.java.util.UUID.fromString(str(grs.id))
        axisSettings = dict(((x, General.int2Float(y))
                             for x,y in grs.axisSettings.items()))
        # NBNB The final None is necessary because there is no non-deprecated
        # constructor that takes two UUIDs. Eventually the deprecated
        # constructor will disappear and we can remove the None
        return self._gateway.jvm.astra.messagebus.messages.instrumentation.GoniostatRotationImpl(
            axisSettings, javaUuid, None
        )

    def _BeamstopSetting_to_java(self, beamStopSetting):

        if beamStopSetting is None:
            return None

        javaUuid = self._gateway.jvm.java.util.UUID.fromString(
            str(beamStopSetting.id)
        )
        axisSettings = dict(((x, General.int2Float(y))
                             for x,y in beamStopSetting.axisSettings.items()))
        return self._gateway.jvm.astra.messagebus.messages.instrumentation.BeamstopSettingImpl(
            axisSettings, javaUuid
        )

    class Java(object):
        implements = ["co.gphl.py4j.PythonListener"]


class DummyGphlWorkflowModel(object):
    """Dummy equivalent of Gphl workflow task node, for testing"""
    def __init__(self):
        # TaskNode.__init__(self)
        self.path_template = None
        self._type = str()
        self._requires_centring = False
        self.invocation_classname = None
        self.java_binary = None
        self._connection_parameters = {}
        self._invocation_properties = {}
        self._invocation_options = {}
        self._workflow_properties = {}
        self._workflow_options = {}

    # Workflow type, or name (string).
    def get_type(self):
        return self._type
    def set_type(self, workflow_type):
        self._type = workflow_type

    # Keyword-value dictionary of connection_parameters (for py4j connection)
    def get_connection_parameters(self):
        return dict(self._connection_parameters)
    def set_connection_parameters(self, valueDict):
        dd = self._connection_parameters
        dd.clear()
        if valueDict:
            dd.update(valueDict)

    # Keyword-value dictionary of invocation_properties (for execution command)
    def get_invocation_properties(self):
        return dict(self._invocation_properties)
    def set_invocation_properties(self, valueDict):
        dd = self._invocation_properties
        dd.clear()
        if valueDict:
            dd.update(valueDict)

    # Keyword-value dictionary of invocation_options (for execution command)
    def get_invocation_options(self):
        return dict(self._invocation_options)
    def set_invocation_options(self, valueDict):
        dd = self._invocation_options
        dd.clear()
        if valueDict:
            dd.update(valueDict)

    # Keyword-value dictionary of workflow_properties (for execution command)
    def get_workflow_properties(self):
        return dict(self._workflow_properties)
    def set_workflow_properties(self, valueDict):
        dd = self._workflow_properties
        dd.clear()
        if valueDict:
            dd.update(valueDict)

    # Keyword-value dictionary of workflow_options (for execution command)
    def get_workflow_options(self):
        return dict(self._workflow_options)
    def set_workflow_options(self, valueDict):
        dd = self._workflow_options
        dd.clear()
        if valueDict:
            dd.update(valueDict)

def testGphlConnection():
    """Test communication to GPhL workflow application"""

    connection = GphlWorkflowConnection()
    wf = getDummyWorkflowModel(
        baseDirectory='/home/rhfogh/pycharm/MXCuBE-Qt_26r',
        gphlInstallation='/public/xtal'
    )
    connection.start_workflow(wf)


def getDummyWorkflowModel(baseDirectory, gphlInstallation):
    self = DummyGphlWorkflowModel()
    self._type = 'TranslationalCalibrationTest'
    self.java_binary = '%s/java/bin/java' % baseDirectory

    self.invocation_classname = 'co.gphl.wf.workflows.WFTransCal'

    dd = {
        'file.encoding':'UTF-8',
    }
    self.set_invocation_properties(dd)

    dd = {
        'cp':'%s/gphl_java_classes/*' % baseDirectory,
    }
    self.set_invocation_options(dd)

    dd = {
        'co.gphl.sdcp.xdsbin':'%s/Xds/XDS-INTEL64_Linux_x86_64/xds_par' % gphlInstallation,
        'co.gphl.wf.bdg_licence_dir':'%s/Server-nightly-alpha-bdg-linux64' % gphlInstallation,
        'co.gphl.wf.stratcal.bin':'%s/Server-nightly-alpha-bdg-linux64/autoPROC/bin/linux64/stratcal' % gphlInstallation,
        'co.gphl.wf.simcal_predict.bin':'%s/Server-nightly-alpha-bdg-linux64/autoPROC/bin/linux64/simcal_predict' % gphlInstallation,
        'co.gphl.wf.transcal.bin':'%s/Server-nightly-alpha-bdg-linux64/autoPROC/bin/linux64/transcal' % gphlInstallation,
        'co.gphl.wf.recen.bin':'%s/Server-nightly-alpha-bdg-linux64/autoPROC/bin/linux64/recen' % gphlInstallation,
        'co.gphl.wf.diffractcal.bin':'path/to/diffractcal',
        'co.gphl.wf.simcal_predict.b_wilson':1.5e-3,
        'co.gphl.wf.simcal_predict.cell_dim_sd_scale':26.0,
        'co.gphl.wf.simcal_predict.mosaicity':0.2,
    }
    self.set_workflow_properties(dd)

    dd = {'wdir':os.path.join('/tmp/mxcube_testdata/visitor/idtest000/id-test-eh1/20130611/PROCESSED_DATA',
                              'GPHL'),
          'calibration':'transcal',
          'file':'%s/HardwareRepository/tests/xml/gphl_config/TransCalTest.inp' % baseDirectory,
          'beamline':'py4j::',
          'persistname':'persistence',
          'wfprefix':'gphl_wf_',
          }
    self.set_workflow_options(dd)
    #
    return self

if __name__ == '__main__':
    testGphlConnection()
