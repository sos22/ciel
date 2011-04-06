
import ciel
import logging
import threading
from skywriting.runtime.fetcher import fetch_ref_async
from skywriting.runtime.producer import make_local_output
from shared.references import SW2_TombstoneReference
from skywriting.runtime.exceptions import MissingInputException
from skywriting.runtime.block_store import filename_for_ref

class ContextManager:
    def __init__(self, description):
        self.description = description
        self.active_contexts = []

    def add_context(self, new_context):
        ret = new_context.__enter__()
        self.active_contexts.append(ret)
        return ret
    
    def remove_context(self, context):
        self.active_contexts.remove(context)
        context.__exit__(None, None, None)

    def __enter__(self):
        return self

    def __exit__(self, exnt, exnv, exnbt):
        if exnt is not None:
            ciel.log("Context manager for %s exiting with exception %s" % (self.description, repr(exnv)), "EXEC", logging.WARNING)
        else:
            ciel.log("Context manager for %s exiting cleanly" % self.description, "EXEC", logging.INFO)
        for ctx in self.active_contexts:
            ctx.__exit__(exnt, exnv, exnbt)
        return False

class SynchronousTransfer:
        
    def __init__(self, ref):
        self.ref = ref
        self.filename = None
        self.str = None
        self.success = None
        self.finished_event = threading.Event()

    def result(self, success, completed_ref):
        self.success = success
        # Completed ref ignored... for now
        self.finished_event.set()

    def reset(self):
        pass

    def start_filename(self, filename, is_pipe):
        self.filename = filename

    def return_string(self, str):
        self.str = str
        self.success = True
        self.finished_event.set()

    def wait(self):
        self.finished_event.wait()

def retrieve_filenames_for_refs(refs):
        
    ctxs = []
    for ref in refs:
        sync_transfer = SynchronousTransfer(ref)
        ciel.log("Synchronous fetch ref %s" % ref, "BLOCKSTORE", logging.INFO)
        transfer_ctx = fetch_ref_async(ref, sync_transfer.result, sync_transfer.reset, sync_transfer.start_filename)
        ctxs.append(sync_transfer)
            
    for ctx in ctxs:
        ctx.wait()
            
    failed_transfers = filter(lambda x: not x.success, ctxs)
    if len(failed_transfers) > 0:
        raise MissingInputException(dict([(ctx.ref.id, SW2_TombstoneReference(ctx.ref.id, ctx.ref.location_hints)) for ctx in failed_transfers]))
    return [x.filename for x in ctxs]

def retrieve_filename_for_ref(ref):

    return retrieve_filenames_for_refs([ref])[0]

def retrieve_strings_for_refs(refs):

    ctxs = []
    for ref in refs:
        sync_transfer = SynchronousTransfer(ref)
        ciel.log("Synchronous fetch ref %s" % ref, "BLOCKSTORE", logging.INFO)
        transfer_ctx = fetch_ref_async(ref, sync_transfer.result, sync_transfer.reset, sync_transfer.start_filename, string_callback=sync_transfer.return_string)
        ctxs.append(sync_transfer)

    for ctx in ctxs:
        ctx.wait()

    failed_transfers = filter(lambda x: not x.success, ctxs)
    if len(failed_transfers) > 0:
        raise MissingInputException(dict([(ctx.ref.id, SW2_TombstoneReference(ctx.ref.id, ctx.ref.location_hints)) for ctx in failed_transfers]))

    strs = []
    for ctx in ctxs:
        if ctx.str is not None:
            strs.append(ctx.str)
        else:
            with open(ctx.filename, "r") as fp:
                strs.append(fp.read())
    return strs

def retrieve_string_for_ref(ref):
        
    return retrieve_strings_for_refs([ref])[0]

def write_fixed_ref_string(string, fixed_ref):
    output_ctx = make_local_output(id)
    with open(filename_for_ref(fixed_ref), "w") as fp:
        fp.write(string)
    output_ctx.close()

def ref_from_string(string, id):
    output_ctx = make_local_output(id)
    filename, _ = output_ctx.get_filename_or_fd()
    with open(filename, "w") as fp:
        fp.write(string)
    output_ctx.close()
    return output_ctx.get_completed_ref()

# Why not just rename to self.filename(id) and skip this nonsense? Because os.rename() can be non-atomic.
# When it renames between filesystems it does a full copy; therefore I copy/rename to a colocated dot-file,
# then complete the job by linking the proper name in output_ctx.close().
def ref_from_external_file(filename, id):
    output_ctx = make_local_output(id)
    with output_ctx:
        shutil.move(filename, output_ctx.get_filename())
    return output_ctx.get_completed_ref()

def get_ref_for_url(self, url, version, task_id):
    """
    Returns a SW2_ConcreteReference for the data stored at the given URL.
    Currently, the version is ignored, but we imagine using this for e.g.
    HTTP ETags, which would raise an error if the data changed.
    """

    parsed_url = urlparse.urlparse(url)
    if parsed_url.scheme == 'swbs':
        # URL is in a Skywriting Block Store, so we can make a reference
        # for it directly.
        id = parsed_url.path[1:]
        ref = SW2_ConcreteReference(id, None)
        ref.add_location_hint(parsed_url.netloc)
    else:
        # URL is outside the cluster, so we have to fetch it. We use
        # content-based addressing to name the fetched data.
        hash = hashlib.sha1()

        # 1. Fetch URL to a file-like object.
        with contextlib.closing(urllib2.urlopen(url)) as url_file:

            # 2. Hash its contents and write it to disk.
            with tempfile.NamedTemporaryFile('wb', 4096, delete=False) as fetch_file:
                fetch_filename = fetch_file.name
                while True:
                    chunk = url_file.read(4096)
                    if not chunk:
                        break
                    hash.update(chunk)
                    fetch_file.write(chunk)

        # 3. Store the fetched file in the block store, named by the
        #    content hash.
        id = 'urlfetch:%s' % hash.hexdigest()
        ref = ref_from_external_file(fetch_filename, id)

    return ref
