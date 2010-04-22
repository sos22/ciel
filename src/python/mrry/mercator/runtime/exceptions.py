'''
Created on 22 Apr 2010

@author: dgm36
'''

class RuntimeSkywritingError(Exception):
    def __init__(self):
        pass

class ExecutionInterruption(Exception):
    def __init__(self):
        pass
    
class FeatureUnavailableException(ExecutionInterruption):
    def __init__(self, feature_name):
        self.feature_name = feature_name

    def __repr__(self):
        return 'FeatureUnavailableException(feature_name="%s")' % (self.feature_name, )
        
class ReferenceUnavailableException(ExecutionInterruption):
    def __init__(self, ref, continuation):
        self.ref = ref
        self.continuation = continuation
        
    def __repr__(self):
        return 'ReferenceUnavailableException(ref=%s)' % (repr(self.ref), )