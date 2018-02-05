#!/usr/bin/python
# -*- coding: utf-8 -*-

# (c) 2018, BROADSoftware
#
# This software is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This software is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this software. If not, see <http://www.gnu.org/licenses/>.




DOCUMENTATION = '''
---
module: storm_topology
short_description: Manage storm topology state
description:
     - Allow to fetch topology status and to change it by activating, deactivating or killing it.
notes:
    - All operations are performed using Storm UI REST API 
requirements: [ ]
author: 
    - "Serge ALEXANDRE"
options:
  ui_url:
    description:
      - 'The URL of the Storm UI, without path'
    required: true
    default: None
  name:
    description:
      - 'Topology name'
    required: true
    default: None
  state:
    description:
      - 'target topology state: active,stopped,inactive,restarted,reloaded,get'
    required: true
    default: None
  launching_script:
    description:
      - 'Script to launch topology. Must be an absolute path'
    required: If state==started,restarted or reloaded
    default: None
  kerberos:
    description:
      - 'Boolean. Storm UI access require kerberos authentication'
    required: false
    default: false
'''

EXAMPLES = '''
'''


HAS_REQUESTS = False

try:
    import requests
    HAS_REQUESTS = True
except ImportError, AttributeError:
    # AttributeError if __version__ is not present
    pass

HAS_KERBEROS = False
try:
    from requests_kerberos import HTTPKerberosAuth
    HAS_KERBEROS = True
except ImportError:
    pass


# Global, to allow access from error
module = None

def error(message, *args):
    x = "" + message.format(*args)
    module.fail_json(msg = x)    

class Parameters:
    pass

# These are the status possible value in the json response
class Status:
    ACTIVE="ACTIVE" 
    INACTIVE="INACTIVE"
    REBALANCING="REBALANCING"   
    KILLED="KILLED"
    UNEXISTING="UNEXISTING"

# Some tokens of interest in the json response
class Token:
    STATUS="status"
    ID="id"

# These are the possible value of the state command.
class State:
    ACTIVE="active"
    KILLED="killed"
    INACTIVE="inactive"
    UNEXISTING="unexisting"
    GET="get"

class StormRestApi:
    
    def __init__(self, endpoint, kerberos):
        self.endpoint = endpoint
        if kerberos:
            if not HAS_KERBEROS:
                error("'python-requests-kerberos' package is not installed")
            else:
                self.kerberos_auth = HTTPKerberosAuth()
        else:
            self.kerberos_auth = None
    
    def get(self, path):
        url = self.endpoint + path
        resp = requests.get(url, auth=self.kerberos_auth)
        if resp.status_code == 200:
            result = resp.json()
            return result
        else:
            error("Invalid returned http code '{0}' when calling GET on '{1}'".format(resp.status_code, url))
            
    def post(self, path):
        url = self.endpoint + path
        resp = requests.post(url, auth=self.kerberos_auth)
        if resp.status_code != 200:
            result = resp.json()
            if result != None:
                #r = json.loads(result)
                r = result
                if "errorMessage" in r:
                    errmsg = r["errorMessage"]
                else:
                    errmsg = "Unknown error"
            error("Invalid returned http code '{0}' when calling POST on '{1}'\n{2}".format(resp.status_code, url, errmsg))
    

    def getTopologyByName(self, name):
        result = self.get("/api/v1/topology/summary")
        if "topologies" in result:
            for topology in result["topologies"]:
                if topology["name"] == name:
                    return topology
        return None
                
    def killTopology(self, p):
        topology = self.getTopologyByName(p.name)
        if topology != None:
            if topology[Token.STATUS] != Status.KILLED:
                p.changed = True
                if not p.check_mode:
                    self.post("/api/v1/topology/{}/kill/{}".format(topology[Token.ID], p.waitTime))
        else:
            error("Unexisting topology {}".format(name))
            
    def activateTopology(self, p):
        topology = self.getTopologyByName(p.name)
        if topology != None:
            if topology[Token.STATUS] == Satus.INACTIVE:
                p.changed = True
                if not p.check_mode:
                    self.post("/api/v1/topology/{}/activate".format(topology[Token.ID]))
        else:
            error("Unexisting topology {}".format(p.name))

    def deactivateTopology(self, p):
        topology = self.getTopologyByName(p.name)
        if topology != None:
            if topology[Token.STATUS] == Status.ACTIVE:
                p.changed = True
                if not p.check_mode:
                    self.post("/api/v1/topology/{}/deactivate".format(topology[Token.ID]))
        else:
            error("Unexisting topology {}".format(p.name))



            
def main():
    
    global module
    module = AnsibleModule(
        argument_spec = dict(
            ui_url = dict(required=True),
            name = dict(required=True),
            state = dict(required=True, choices=['active','killed','inactive','get']),
            wait_time = dict(required=False, type='int', default=30),
            kerberos = dict(required=False, type='bool', default=False)
            
        ),
        supports_check_mode=True
    )
    
    if not HAS_REQUESTS:
        error(msg="python-requests package is not installed")    
    
    p = Parameters()
    p.uiUrl = module.params['ui_url']
    p.name = module.params['name']
    p.state = module.params['state']
    p.waitTime = module.params['wait_time']
    p.kerberos = module.params['kerberos']
    p.checkMode = module.check_mode
    p.changed = False

    api = StormRestApi(p.uiUrl, p.kerberos)
    
    if p.state == State.ACTIVE:
        api.activateTopology(p)
        module.exit_json(changed=p.changed)
    elif p.state == State.INACTIVE:
        api.deactivateTopology(p)
    elif p.state == State.KILLED:
        api.killTopology(p)
    elif p.state == State.UNEXISTING:
        api.killTopology(p)
        topology = self.getTopologyByName(p.name)
        while topology != None:
            time.sleep(1)
            topology = self.getTopologyByName(p.name)
    elif p.state == State.GET:
        pass
    else:
        error("Unknown state {}".format(p.state))
    
    topology = api.getTopologyByName(p.name)
    if topology != None:
        status = topology[Token.STATUS]
    else:
        status = Status.UNEXISTING
    module.exit_json(changed=p.changed, status=status.lower())


from ansible.module_utils.basic import *
if __name__ == '__main__':
    main()

