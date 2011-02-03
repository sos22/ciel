
import sys
import skypy

def skypy_callback(str):
    print >>sys.stderr, "Skypy got callback: ", str
    return (str + " and Skypy too")

def skypy_main():
    sw_ret = skypy.spawn_exec("swi", sw_file_ref=skypy.package_lookup("sw_main"))
    sw_ref = skypy.deref_json(sw_ret[0])
    return skypy.deref(sw_ref[0])