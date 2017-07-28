# encoding: utf-8
""""Global Phasing py4j workflow server connection"""

__copyright__ = """
  * Copyright © 2016 - ${YEAR} by Global Phasing Ltd. All rights reserved
"""
__author__ = "rhfogh"
__date__ = "04/11/16"


import uuid
import subprocess
from py4j import clientserver
import GphlMessages
from HardwareObjects import General 
States = General.States

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

    def start_workflow(self, workflow_model_obj):

        if self.get_state() != States.OFF:
            # NB, for now workflow is started as the connection is made,
            # so we are never in state 'ON'/STANDBY
            raise RuntimeError("Workflow is already running, cannot be started")


        dispatcher.connect(self.abort_workflow, 'GPHL_BEAMLINE_ABORT')


        self._workflow_name = workflow_model_obj.get_type()

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

        self._gateway = clientserver.ClientServer(
            java_parameters=clientserver.JavaParameters(**java_parameters),
            python_parameters=clientserver.PythonParameters(
                **python_parameters),
            python_server_entry_point=self)


        # TODO Here we make and send the workflow start-run message
        # NB currently done under 'wfrun' alias
        #  This forks off the server process and returns None
        commandList = ['java']

        for keyword, value in workflow_model_obj.get_invocation_properties():
            commandList.extend(General.javaProperty(keyword, value))

        for keyword, value in workflow_model_obj.get_invocation_options():
            commandList.extend(General.commandOption(keyword, value))

        commandList.append(workflow_model_obj.invocation_classname)

        for keyword, value in workflow_model_obj.get_workflow_properties():
            commandList.extend(General.javaProperty(keyword, value))

        for keyword, value in workflow_model_obj.get_workflow_options():
            commandList.extend(General.commandOption(keyword, value))
        #
        subprocess.Popen(commandList)

        self.set_state(States.RUNNING)

    def _workflow_ended(self):

        self._enactment_id = None
        self._workflow_name = None
        self._state = States.ON

    def _close_connection(self):
        self._gateway = None
        self._state = States.OFF

    def abort_workflow(self, message=None):
        """Abort workflow - may be called from controller in any state"""

        payload = "Workflow aborted from Beamline"

        if self.get_state() == States.OFF:
            raise RuntimeError("Workflow is off, cannot be aborted")

        # NB signals will have no effect if controller is already deleted.
        self._workflow_ended()
        if message:
            payload = "%s: %s" % (payload, message)
        dispatcher.send(GphlMessages.message_type_to_signal['String'],
                        self, payload=payload)

    def _receive_from_server(self, py4jMessage):
        """Receive and process message from workflow server
        Return goes to server"""

        xx = py4jMessage.getEnactmentId()
        enactment_id = xx and xx.toString()

        xx = py4jMessage.getCorrelationId()
        correlation_id = xx and xx.toString()
        message_type, payload = self._decode_py4j_message(py4jMessage)

        if self.get_state() == 'ON':
            # Workflow has been aborted from beamline.
            return self._response_to_server(GphlMessages.BeamlineAbort(),
                                            correlation_id)

        elif self.get_state() == States.OFF and message_type == 'WorkflowAborted':
            # This is the end of an abort process. Ignore
            return None

        # Not aborting, get on with the work
        self.set_state(States.OPEN)

        # Also serves to trigger abort at end of function
        abort_message = None

        if not payload:
            abort_message = ("Payload could not be decoded for message %s"
                             % message_type)

        elif not enactment_id:
            abort_message = "Received message with empty enactment_id"

        else:
            # Set send_signal and enactment_id, testing for errors
            try:
                send_signal = GphlMessages.message_type_to_signal[message_type]
            except KeyError:
                abort_message = ("Unknown message type from server: %s"
                                 % message_type)
            else:

                if self._enactment_id is None:
                    # NB this should be made less primitive
                    # once we are past direct function calls
                    self._enactment_id = enactment_id
                elif self._enactment_id != enactment_id:
                    abort_message = (
                        "Workflow process id %s != message process id %s"
                        % (self._enactment_id, enactment_id)
                    )

        if not abort_message:

            if message_type in ('String',
                                'SubprocessStarted',
                                'SubprocessStopped'):
                # INFO messages to echo - no response to server needed
                responses = dispatcher.send(send_signal, self, payload=payload,
                                            correlation_id=correlation_id)
                result = None

            elif message_type in ('RequestConfiguration',
                                  'GeometricStrategy',
                                  'CollectionProposal',
                                  'ChooseLattice',
                                  'RequestCentring',
                                  'ObtainPriorInformation',
                                  'PrepareForCentring'):
                # Requests:
                responses = dispatcher.send(send_signal, self, payload=payload,
                                            correlation_id=correlation_id)
                result, abort_message = (
                    self._extractResponse(responses, message_type)
                )

            elif message_type in ('WorkflowAborted',
                                  'WorkflowCompleted',
                                  'WorkflowFailed'):
                self._workflow_ended()
                # Server has terminated by itself, so there is nothing to return
                self._close_connection()
                return None

            else:
                abort_message = ("Unknown message type: %s" % message_type)

        if abort_message:
            # We do not need to wait for the return after the Beamline abort
            # message before we close the connection.
            self.abort_workflow(message=abort_message)
            self._close_connection()
            return self._response_to_server(GphlMessages.BeamlineAbort(),
                                            correlation_id)

        elif result is None:
            # No response expected
            self.set_state(States.RUNNING)
            return None

        else:

            self.set_state(States.RUNNING)
            return self._response_to_server(result, correlation_id)


    # NBNB TODO temporary fix - remove when Java calls have been renamed
    msgToBcs = _receive_from_server

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

    def _decode_py4j_message(self, message):
        """Extract messageType and convert py4J object to python object"""

        # Determine message type
        messageType = message.getPayloadClass().getSimpleName()
        if messageType.endswith('Impl'):
            messageType = messageType[:-4]
        converterName = '_%s_to_python' % messageType

        if self.debug:
            print ('@~@~ processMessage', messageType, converterName,
                   hasattr(self, converterName))

        try:
            # determine converter function
            converter = getattr(self, converterName)
        except AttributeError:
            print ("Message type %s not recognised (no %s function)"
                   % (messageType, converterName))
            result = None
        else:
            try:
                # Convert to Python objects
                result = converter(message.getPayload())
            except NotImplementedError:
                print('Processing of message %s not implemented' % messageType)
                result = None
        #
        return messageType, result

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
                                    wavelength=py4jBeamSetting.wavelength)


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

        if self.get_state() != States.OPEN:
            self.abort_workflow(message="Reply (%s) to server out of context."
                                % payload.__class__.__name__)

        try:
            if self._enactment_id is None:
                enactment_id = None
            else:
                enactment_id = self._gateway.jvm.java.util.UUID.fromString(
                    self.enactment_id
                )

            if correlation_id is not None:
                correlation_id = self._gateway.jvm.java.util.UUID.fromString(
                    correlation_id
                )

            py4j_payload = self._payload_to_java(payload)

            response = self._gateway.jvm.co.gphl.sdcp.py4j.Py4jMessage(
                enactment_id, correlation_id, py4j_payload
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
            centringDone.timestamp,
            self._GoniostatTranslation_to_java(
                centringDone.goniostatTranslation
            )
        )

    def _ConfigurationData_to_java(self, configurationData):
        return self._gateway.jvm.astra.messagebus.messages.information.ConfigurationDataImpl(
            self._gateway.jvm.java.io.File(configurationData.location)
        )

    def _ReadyForCentring_to_java(self, readyForCentring):
        return self._gateway.jvm.astra.messagebus.messages.information.ReadyForCentringImpl(
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
        builder = builder.sampleName(priorInformation.sampleName)
        # if priorInformation.referenceFile:
        #     builder = builder.referenceFile(self._gateway.jvm.java.net.URL(
        #         priorInformation.referenceFile)
        #     )
        builder = builder.rootDirectory(priorInformation.rootDirectory)
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
        builder = builder.spaceGroup(userProvidedInfo.spaceGroup)
        builder = builder.cell(
            self._UnitCell_to_java(userProvidedInfo.cell)
        )
        if userProvidedInfo.expectedResolution:
            builder = builder.expectedResolution(
                General.int2Float(userProvidedInfo.expectedResolution)
            )
        builder = builder.anisotropic(userProvidedInfo.isAnisotropic)
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
            str(gts.requestedRotationId.id)
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
