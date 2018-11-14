
from HardwareRepository import HardwareRepository
import logging
import urllib2
import os
from cookielib import CookieJar

from suds.transport.http import HttpAuthenticated
from suds.client import Client
from suds import WebFault
from suds.sudsobject import asdict

from ISPyBClient2 import  ISPyBClient2, _CONNECTION_ERROR_MSG, trace, utf_encode
from urllib2 import URLError
import traceback
from collections import namedtuple
# The WSDL root is configured in the hardware object XML file.
#_WS_USERNAME, _WS_PASSWORD have to be configured in the HardwareObject XML file.
_WSDL_ROOT = ''
_WS_BL_SAMPLE_URL = _WSDL_ROOT + 'ToolsForBLSampleWebService?wsdl'
_WS_SHIPPING_URL = _WSDL_ROOT + 'ToolsForShippingWebService?wsdl'
_WS_COLLECTION_URL = _WSDL_ROOT + 'ToolsForCollectionWebService?wsdl'
_WS_AUTOPROC_URL = _WSDL_ROOT + 'ToolsForAutoprocessingWebService?wsdl'
_WS_USERNAME = None
_WS_PASSWORD = None

SampleReference = namedtuple('SampleReference', ['code',
                                                 'container_reference',
                                                 'sample_reference',
                                                 'container_code'])

class SOLEILISPyBClient(ISPyBClient2):
           
    def __init__(self, name):
        ISPyBClient2.__init__(self, name)
        
        self.logger = logging.getLogger('ispyb_client')
        
        try:
            formatter = \
                logging.Formatter('%(asctime)s %(levelname)s %(message)s')
            hdlr = logging.FileHandler('/home/experiences/proxima2a/com-proxima2a/MXCuBE_v2_logs/ispyb_client.log')
            hdlr.setFormatter(formatter)
            self.logger.addHandler(hdlr) 
        except:
            pass

        self.logger.setLevel(logging.DEBUG)
        
    def init(self):
        """
        Init method declared by HardwareObject.
        """
        self.authServerType = self.getProperty("authServerType", "ldap")
        if self.authServerType == "ldap":
            # Initialize ldap
            self.ldapConnection=self.getObjectByRole('ldapServer')
            if self.ldapConnection is None:
                logging.getLogger("HWR").debug('LDAP Server is not available')
        
        self.loginType = self.getProperty("loginType", "proposal")
        self.loginTranslate = self.getProperty("loginTranslate", True)
        self.session_hwobj = self.getObjectByRole('session')
        self.beamline_name = self.session_hwobj.beamline_name

        self.ws_root = self.getProperty('ws_root')
        self.ws_username = self.getProperty('ws_username')
        if not self.ws_username:
            self.ws_username = _WS_USERNAME
        self.ws_password = self.getProperty('ws_password')
        if not self.ws_password:
            self.ws_password = _WS_PASSWORD
        
        self.ws_collection = self.getProperty('ws_collection')
        self.ws_shipping = self.getProperty('ws_shipping')
        self.ws_tools = self.getProperty('ws_tools')
        
        self.logger.debug("SOLEILISPyBClient: Initializing SOLEIL ISPyB Client")
        self.logger.debug("   - using http_proxy = %s " % os.environ['http_proxy'])

        try:

            if self.ws_root:
                logging.info("SOLEILISPyBClient: attempting to connect to %s" % self.ws_root)
                print "SOLEILISPyBClient: attempting to connect to %s" % self.ws_root
                
                try: 
                    self._shipping = self._wsdl_shipping_client()
                    self._collection = self._wsdl_collection_client()
                    self._tools_ws = self._wsdl_tools_client()
                    logging.debug("SOLEILISPyBClient: extracted from ISPyB values for shipping, collection and tools")
                except: 
                    print traceback.print_exc()
                    logging.exception("SOLEILISPyBClient: %s" % _CONNECTION_ERROR_MSG)
                    return
        except:
            print traceback.print_exc()
            logging.getLogger("HWR").exception(_CONNECTION_ERROR_MSG)
            return

        try:
            proposals = self.session_hwobj['proposals']
            
            for proposal in proposals:
                code = proposal.code
                self._translations[code] = {}
                try:
                    self._translations[code]['ldap'] = proposal.ldap
                except AttributeError:
                    pass
                try:
                    self._translations[code]['ispyb'] = proposal.ispyb
                except AttributeError:
                    pass
                try:
                    self._translations[code]['gui'] = proposal.gui
                except AttributeError:
                    pass
        except IndexError:
            pass
        except:
            pass
        
    def translate(self, code, what):  
        """
        Given a proposal code, returns the correct code to use in the GUI,
        or what to send to LDAP, user office database, or the ISPyB database.
        """
        if what == "ispyb":
            return "mx"
        return ""

    def _wsdl_shipping_client(self):
        return self._wsdl_client(self.ws_shipping)

    def _wsdl_tools_client(self):
        return self._wsdl_client(self.ws_tools)

    def _wsdl_collection_client(self):
        return self._wsdl_client(self.ws_collection)

    def _wsdl_client(self, service_name):

        # Handling of redirection at soleil needs cookie handling
        cj = CookieJar()
        url_opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))

        trans = HttpAuthenticated(username = self.ws_username, password = self.ws_password)
        logging.debug('_wsdl_client service_name %s - trans %s' % (service_name, trans))
        
        trans.urlopener = url_opener
        urlbase = service_name + "?wsdl"
        locbase = service_name
       
        ws_root = self.ws_root.strip()

        url = ws_root + urlbase
        loc = ws_root + locbase
        
        print '_wsdl_client, url', url
        ws_client = Client(url, transport=trans, timeout=3, \
                           location=loc, cache = None)

        return ws_client

    def prepare_collect_for_lims(self, mx_collect_dict):
        # Attention! directory passed by reference. modified in place
        
        prop = 'EDNA_files_dir' 
        path = mx_collect_dict[prop] 
        ispyb_path = self.session_hwobj.path_to_ispyb( path )
        mx_collect_dict[prop] = ispyb_path

        prop = 'process_directory' 
        path = mx_collect_dict['fileinfo'][prop] 
        ispyb_path = self.session_hwobj.path_to_ispyb( path )
        mx_collect_dict['fileinfo'][prop] = ispyb_path
        
        for i in range(4):
            try: 
                prop = 'xtalSnapshotFullPath%d' % (i+1)
                path = mx_collect_dict[prop] 
                ispyb_path = self.session_hwobj.path_to_ispyb( path )
                logging.debug("SOLEIL ISPyBClient - %s is %s " % (prop, ispyb_path))
                mx_collect_dict[prop] = ispyb_path
            except:
                pass

    def prepare_image_for_lims(self, image_dict):
        for prop in [ 'jpegThumbnailFileFullPath', 'jpegFileFullPath']:
            try:
                path = image_dict[prop] 
                ispyb_path = self.session_hwobj.path_to_ispyb( path )
                image_dict[prop] = ispyb_path
            except:
                pass


    @trace
    def get_login_from_proposal_number(self, proposal_number, proposal_code='mx'):
        person = self._shipping.service.findPersonByProposal(proposal_code, proposal_number)
        return person.login
    
    @trace
    def get_proposal_by_username(self, username, proposal_number=0, proposal_code='mx'):
        
        logging.getLogger("ispyb_client").debug('get_proposal_by_username() username %s' % username)
        if username.isdigit():
            proposal_number = username
            username = self.get_login_from_proposal_number(username)
            
        logging.getLogger("ispyb_client").debug('get_proposal_by_username() username %s' % username)

        empty_dict = {'Proposal': {}, 'Person': {}, 'Laboratory': {}, 'Session': {}, 'status': {'code':'error'}}

        if not self._shipping:
           logging.getLogger("ispyb_client").\
                warning("Error in get_proposal: Could not connect to server," + \
                          " returning empty proposal")
           return empty_dict


        try:
            try:
                person = self._shipping.service.findPersonByLogin(username, self.beamline_name)
            except WebFault, e:
                logging.getLogger("ispyb_client").warning(str(e))
                person = {}

            try:
                proposal = self._shipping.service.findProposalByLoginAndBeamline(username, self.beamline_name)
                if not proposal:
                    logging.getLogger("ispyb_client").warning("Error in get_proposal: No proposal has been found to  the user, returning empty proposal")
                    return empty_dict
                proposal_code   = proposal.code
                proposal_number = proposal.number
            except WebFault, e:
                logging.getLogger("ispyb_client").warning(str(e))
                proposal = {}

            try:
                lab = self._shipping.service.findLaboratoryByCodeAndNumber(proposal_code, proposal_number)
            except WebFault, e:
                logging.getLogger("ispyb_client").warning(str(e))
                lab = {}

            try:
                res_sessions = self._collection.service.\
                    findSessionsByProposalAndBeamLine(proposal_code,
                                                           proposal_number,
                                                           self.beamline_name)
                sessions = []

                # Handels a list of sessions
                for session in res_sessions:
                    if session is not None :
                        try:
                            session.startDate = \
                                datetime.strftime(session.startDate,
                                                  "%Y-%m-%d %H:%M:%S")
                            session.endDate = \
                                datetime.strftime(session.endDate,
                                                  "%Y-%m-%d %H:%M:%S")
                        except:
                            pass

                        sessions.append(utf_encode(asdict(session)))

            except WebFault, e:
                logging.getLogger("ispyb_client").warning(str(e))
                sessions = []

        except URLError:
            logging.getLogger("ispyb_client").warning(_CONNECTION_ERROR_MSG)
            return empty_dict


        logging.getLogger("ispyb_client").info( str(sessions) )
        return  {'Proposal': utf_encode(asdict(proposal)),
                 'Person': utf_encode(asdict(person)),
                 'Laboratory': utf_encode(asdict(lab)),
                 'Session': sessions,
                 'status': {'code':'ok'}}
  
    @trace
    def get_proposals_by_user(self, username, proposal_list=[], res_proposal=[]):
        
        logging.getLogger("ispyb_client").debug('get_proposals_by_user() username %s' % username)
        if username.isdigit():
            username = self.get_login_from_proposal_number(username)
            
        logging.getLogger("ispyb_client").debug('get_proposals_by_user() username %s' % username)
        
        if self._disabled:
            return proposal_list

        if self._shipping:
            try:
               proposals = eval(self._shipping.service.\
                  findProposalsByLoginName(username))  
               if proposal_list is not None:
                   for proposal in proposals:
                        if proposal['type'].upper() in ['MX', 'MB'] and \
                           proposal not in proposal_list:
                           proposal_list.append(proposal)
            except WebFault, e:
               proposal_list = []
               logging.getLogger("ispyb_client").error(e.message)

            proposal_list = newlist = sorted(proposal_list,
                key=lambda k: int(k['proposalId'])) 

            res_proposal = []
            if len(proposal_list) > 0:
                for proposal in proposal_list:
                    proposal_code = proposal['code']
                    proposal_number = proposal['number']

                    #person
                    try:
                        person = self._shipping.service.\
                                      findPersonByProposal(proposal_code,
                                                           proposal_number)
                        if not person:
                            person = {}
                    except WebFault, e:
                        logging.getLogger("ispyb_client").error(e.message)
                        person = {}

                    #lab
                    try:
                        lab = self._shipping.service.\
                                   findLaboratoryByProposal(proposal_code,
                                                            proposal_number)
                        if not lab:
                            lab = {}
                    except WebFault, e:
                        logging.getLogger("ispyb_client").error(e.message)
                        lab = {}

                    #sessions
                    try:
                        res_sessions = self._collection.service.\
                               findSessionsByProposalAndBeamLine(proposal_code,
                                                                 proposal_number,
                                                                 self.beamline_name)
                        sessions = []
                        for session in res_sessions:
                            if session is not None :
                                try:
                                    session.startDate = \
                                        datetime.strftime(session.startDate,
                                                          "%Y-%m-%d %H:%M:%S")
                                    session.endDate = \
                                        datetime.strftime(session.endDate,
                                                          "%Y-%m-%d %H:%M:%S")
                                except:
                                    pass
                                sessions.append(utf_encode(asdict(session)))

                    except WebFault, e:
                        logging.getLogger("ispyb_client").error(e.message)
                        sessions = []

                    
                    res_proposal.append({'Proposal': proposal,
                                         'Person': utf_encode(asdict(person)),
                                         'Laboratory': utf_encode(asdict(lab)),
                                         'Session' : sessions})
            else:
                logging.getLogger("ispyb_client").\
                   warning("No proposals for user %s found" %username)
        else:
            logging.getLogger("ispyb_client").\
                exception("Error in get_proposal: Could not connect to server," + \
                          " returning empty proposal")
        return res_proposal 
    
def test_hwo(hwo):
    proposal_code = 'mx'
    proposal_number = '20100023' 
    proposal_psd = 'tisabet'

    print "Trying to login to ispyb" 
    info = hwo.login(proposal_number, proposal_psd)
    print "logging in returns: ", str(info)

def test():
    import os
    hwr_directory = os.environ["XML_FILES_PATH"]

    hwr = HardwareRepository.HardwareRepository(os.path.abspath(hwr_directory))
    hwr.connect()

    db = hwr.getHardwareObject("/singleton_objects/dbconnection")
    
    #print 'db', db
    #print 'dir(db)', dir(db)
    #print 'db._SOLEILISPyBClientShipping', db._SOLEILISPyBClientShipping
    #print 'db.Shipping', db.Shipping
    
    proposal_code = 'mx'
    proposal_number = '20100023' 
    proposal_psd = 'tisabet'
    
    #print 'db._shipping.service.findPersonByProposal(proposal_code, proposal_number)'
    person = db._shipping.service.findPersonByProposal(proposal_code, proposal_number)
    print 'person'
    print person
    
    person2 = db._shipping.service.findPersonByLogin(person.login)
    print 'person2'
    print person2
    
    #lab = db._shipping.service.findLaboratoryByProposal(proposal_code, proposal_number)
    #print 'lab'
    #print lab
    
    sessions = db._collection.service.findSessionsByProposalAndBeamLine(proposal_code, proposal_number, db.beamline_name)
    print 'sessions'
    print sessions
    
    proposals = db._shipping.service.findProposalsByLoginName(person.login)
    print 'proposals'
    print proposals
    
    proposal = db.get_proposal(proposal_code, proposal_number)
    print 'proposal'
    print proposal

    session_id = sessions[0]['sessionId']
    print 'session_id'
    print session_id
    samples = db.get_samples(proposal['Proposal']['proposalId'], proposal['Session'][0]['sessionId'])
    response_samples = db._tools_ws.service.findSampleInfoLightForProposal(proposal_number, 'PROXIMA2A')
    print 'samples'
    print samples
   
    print 'response_samples'
    print response_samples
    #print 'db.get_proposal(proposal_code, proposal_number)'
    #info = db.get_proposal(proposal_code, proposal_number)# proposal_number)
    #print info
    
    #print 'db.get_proposals_by_user(proposal_number)'
    #info = db.get_proposals_by_user(proposal_number)
    #print info
    
    #print 'db.get_proposal_by_username(proposal_number)'
    #info = db.get_proposal_by_username(proposal_number)
    #print info
    
    #print 'db.login(proposal_number, proposal_psd)'
    #info = db.login(proposal_number, proposal_psd)
    #print info
    
if __name__ == '__main__':
    test()

