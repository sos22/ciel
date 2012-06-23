# Copyright (c) 2011 Derek Murray <Derek.Murray@cl.cam.ac.uk>
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
from ciel.logger import CielLogger
import logging
import ciel.runtime.stopwatch

# Important exports:
#
# CIEL_VERSION_STRING -- a simple string describing the current version of Ciel
# engine -- an alias for cherrypy.engine, or a stub if cherrypy is unavailable
# log -- The main logger, either the cherrypy one or CielLogger

CIEL_VERSION_STRING = 'Ciel version 0.1'

try:
    import cherrypy
    log = cherrypy.log
    _handler = logging.StreamHandler()
    log.error_log.addHandler(_handler)
    #_logger = log._get_builtin_handler(cherrypy.log.error_log, "screen")
    _handler.setLevel(logging.INFO)
except ImportError:
    log = CielLogger()
    _logger = log
    
try:
    import cherrypy
    engine = cherrypy.engine
except ImportError:
    
    class EngineStub:
        def __getattr__(self, name):
            log.error('Attempted to access method %s of stub engine' % name, 'ENGINE', logging.WARN, False)
            def method(*args):
                return
            return method
    
    engine = EngineStub()
    
def set_log_level(lvl):
    _handler.setLevel(lvl)

stopwatch = ciel.runtime.stopwatch.Stopwatch()
