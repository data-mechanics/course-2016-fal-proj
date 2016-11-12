###############################################################################
## 
## dml.py
##
##   Python library providing common functionalities for building Data
##   Mechanics platform components.
##
##   Web:     datamechanics.org
##   Version: 0.0.12.0
##
##

import sys     # To parse command line arguments.
import os.path # To check if a file exists.
import types
import json
import pymongo

###############################################################################
##

"""
An interface error occurs if a user of the library tries defining an algorithm
class without providing definitions for the required methods.
"""
class InterfaceError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

"""
A metaclass for creating contributor-authored (static) classes for platform
algorithms, and a corresponding base class that uses that metaclass.
"""
class MetaAlgorithm(type):
    def __new__(cls, clsname, bases, dct):
        methods = {name:val for name, val in dct.items()}
        if clsname != 'Algorithm':
            if 'contributor' not in methods or type(methods['contributor']) != str:
                raise InterfaceError("The class definition for " + clsname + " does not identify a contributor.")

            if 'reads' not in methods or type(methods['reads']) != list:
                raise InterfaceError("The class definition for " + clsname + " does not list the data sets it reads.")
            reads_types = list({type(x) for x in methods['reads']})
            if len(reads_types) > 0 and (len(reads_types) != 1 or reads_types[0] != str):
                raise InterfaceError("The class definition for " + clsname + " has a non-name in its list of data sets it reads.")

            if 'writes' not in methods or type(methods['writes']) != list:
                raise InterfaceError("The class definition for " + clsname + " does not list the data sets it writes.")
            writes_types = list({type(x) for x in methods['writes']})
            if len(writes_types) > 0 and (len(writes_types) != 1 or writes_types[0] != str):
                raise InterfaceError("The class definition for " + clsname + " has a non-name in its list of data sets it writes.")

            if 'execute' not in methods or isinstance(methods['execute'], types.FunctionType):
                raise InterfaceError("The class definition for " + clsname + " does not define a static 'execute' method.")
            if 'provenance' not in methods or isinstance(methods['execute'], types.FunctionType):
                raise InterfaceError("The class definition for " + clsname + " does not define a static 'provenance' method.")
        return super(MetaAlgorithm, cls).__new__(cls, clsname, bases, dict(dct.items()))

class Algorithm(metaclass=MetaAlgorithm):
    __dml__ = True

"""
An environment error occurs if a user of the library tries running a script
that loads the library in an environment that does not provide the appropriate
configuration and credentials files for a Data Mechanics platform instance.
"""
class EnvironmentError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

"""
Process the command line parameters supplied to the script loading
this module.
"""
class Parameters():
    def __init__(self, arguments):
        self.trial = ('--trial' in arguments) or ('--t' in arguments)

parameters = Parameters(sys.argv[1:])
options = parameters # Public synonym.

"""
We check that the environment provides an appropriate configuration file and
an appropriate authentication credentials file for third-party services.
"""
pathToConfig = "../config.json"
if not os.path.isfile(pathToConfig):
    raise EnvironmentError(\
        "No valid configuration file found at '"\
        + pathToConfig\
        + "'. All scripts must be located within an immediate "\
        + "subdirectory of the platform instance root directory."\
    )

pathToAuth = "../auth.json"
if not os.path.isfile(pathToAuth):
    raise EnvironmentError(\
        "No valid credentials file found at '"\
        + pathToAuth\
        + "'. All scripts must be located within an immediate "\
        + "subdirectory of the platform instance root directory."\
    )
auth = json.loads(open(pathToAuth).read())

"""
Extend the PyMongo database.Database class with customized
methods for creating and dropping collections within repositories.
"""
def customElevatedCommand(db, f, arg, op = None):
    """
    Wrapper to create custom commands for managing the repository that
    require temporary elevation to an authenticated account with higher
    privileges.
    """
    config = json.loads(open(pathToConfig).read())
    user = db.command({"connectionStatus":1})['authInfo']['authenticatedUsers'][0]['user']
    if op != 'record' and arg.split(".")[0] != user:
        arg = user + '.' + arg
    db.logout()
    db.authenticate(config['admin']['name'], config['admin']['pwd'])
    result = f(arg, user, user)
    db.logout()
    db.authenticate(user, user)
    return result

def createTemporary(self, name):
    """
    Wrapper for creating a temporary repository collection
    that can be removed after a particular computation is complete.
    """
    return customElevatedCommand(self, self.system_js.createTemp, name)

def createPermanent(self, name):
    """
    Wrapper for creating a repository collection that should remain
    after it is derived.
    """
    return customElevatedCommand(self, self.system_js.createPerm, name)

def dropTemporary(self, name):
    """
    Wrapper for removing a temporary repository collection.
    """
    return customElevatedCommand(self, self.system_js.dropTemp, name)

def dropPermanent(self, name):
    """
    Wrapper for removing a permanent repository collection.
    """
    return customElevatedCommand(self, self.system_js.dropPerm, name)

def record(self, raw):
    """
    Wrapper for recording a provenance document. Since MongoDB
    does not support fields with the reserved "$" character, we
    replace this character with "@".
    """
    raw = raw.replace('"$"', '"@"')
    return customElevatedCommand(self, self.system_js.record, raw, 'record')

"""
We extend the pymongo Database class with the additional methods
defined above.
"""
pymongo.database.Database.createTemporary = createTemporary
pymongo.database.Database.createTemp = createTemporary
pymongo.database.Database.createPermanent = createPermanent
pymongo.database.Database.createPerm = createPermanent
pymongo.database.Database.dropTemporary = dropTemporary
pymongo.database.Database.dropTemp = dropTemporary
pymongo.database.Database.dropPermanent = dropPermanent
pymongo.database.Database.dropPerm = dropPermanent
pymongo.database.Database.record = record

##eof