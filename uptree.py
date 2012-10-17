# Copyright (c) 2012 ActiveState Software Inc. All rights reserved.
"""Cache directory traversal and reading larger number of small files

(See ``UpTree`` class comment string)
"""

import os
from os import path as P
import pickle
from datetime import datetime
import logging
from six import PY3
if PY3:
    from io import StringIO
else:
    try:
        from cStringIO import StringIO
    except ImportError:
        from StringIO import StringIO

LOG = logging.getLogger(__name__)


class UpTree:
    """Tree traversal cache

    Keep an updated version of recursive file listing, and contents of selected
    files in cache ... while providing a mechanism to mark directories as
    changed based on dirty files.
    
    Code that is changing files should generally do::
    
        >>> uptree = UpTree('/path/to/repo')
        >>> uptree.mark_dirty('/path/to/repo/the/dir/of/modified/file')
        
    Code processing the directory tree should generally do::
    
        >>> uptree = UpTree('/path/to/repo')
        >>> uptree.update()
        >>> for file in uptree.get_files():
        >>>     ...
        >>>     data = uptree.open(some_file).read()
        >>>     ...
    """

    def __init__(self, root, content_cache_filenames=None, mtime_cache_filenames=None):
        self.root = P.normpath(P.abspath(root))
        self.content_cache_filenames = set(content_cache_filenames or [])
        self.mtime_cache_filenames = set(mtime_cache_filenames or [])
        self._cache_file = P.join(self.root, '.uptree-cache')
        
    def mark_dirty(self, dir):
        """Mark a directory as 'dirty'"""
        assert P.isabs(dir), dir
        if not dir.startswith(self.root):
            raise ValueError("the directory <%s> does not belong to <%s>" % (dir, self.root))
        assert dir.startswith(self.root), (dir, self.root)
        if P.isfile(dir):
            dir = P.dirname(dir)
        dirty_file = P.join(dir, '.dirty')
        LOG.debug('Writing dirty file: %s', dirty_file)
        with open(dirty_file, 'w') as f:
            f.write('UpTree dirty file created at: %s' % datetime.now())
        if P.normpath(dir) != self.root:
            return 1 + self.mark_dirty(parentdir(dir))
        return 0

    def get_files(self):
        """Return a recursive list of files & directories (from cache)

        WARNING: you must `update` the cache first
        """
        return self.cache['files']

    def open(self, filepath):
        """Open the given file (from cache)

        Return a file-like object that would read data from cache

        WARNING: you must `update` the cache first
        """
        return StringIO(self.open_and_read(filepath))
        
    def open_and_read(self, filepath):
        """Read the given file contents (from cache)
        
        WARNING: you must `update` the cache first
        """
        assert P.isabs(filepath)
        try:
            return self.cache['data'][filepath]
        except KeyError as e:
            raise IOError('not found in uptree cache (not in filesystem maybe): %s', e)
            
    def exists(self, filepath):
        """Return True if the given file or directory path exists (in cache)
        
        WARNING: you must `update` the cache first
        """
        assert P.isabs(filepath)
        return filepath in self.cache['files']
        
    def getmtime(self, filepath):
        """Return the modification time
        
        WARNING: you must `update` the cache first
        """
        assert P.isabs(filepath)
        try:
            return self.cache['mtime'][filepath]
        except KeyError as e:
            raise IOError('not found in uptree cache (not in filesystem maybe): %s', e)
   
    def update(self, force=False, counters=None):
        """Traverse the root directory and update the cache

        force -- if True, ignore checking .dirty files and rebuild anyway
        """
        j, r = P.join, self.root
        if counters is None:
            counters = dict(
                directories_processed=0, files_read=0, files_stat=0)
            if force:
                LOG.warn('Performing a force update; may take a while')
        
        # Update must happen when the .dirty file exists or cache file is missing
        if not force and not P.exists(j(r, '.dirty')) and P.exists(self._cache_file):
            self._load_cache()
            return counters
        
        LOG.debug('cache: START: %s', self.root)
        
        self._load_cache(reset=True)
        self.cache.clear()
        
        for fp in _ls(r):
            fp_abs = P.join(r, fp)
            # Add to file listing
            self.cache['files'].add(fp_abs)
            
            # Recurse through sub-directories and add their caches
            if P.isdir(fp_abs):
                subtree = UpTree(
                    fp_abs,
                    content_cache_filenames=self.content_cache_filenames,
                    mtime_cache_filenames=self.mtime_cache_filenames
                )
                subtree.update(force=force, counters=counters)
                self.cache.add_sub_cache(subtree.cache)
                subtree._destroy_cache()  # free up memory
            
            # Add to content caches    
            if P.isfile(fp_abs):
                # Update content cache
                if fp in self.content_cache_filenames:
                    with open(fp_abs) as f:
                        self.cache['data'][fp_abs] = f.read()
                    counters['files_read'] += 1
                # Update mtime cache
                if fp in self.mtime_cache_filenames:
                    self.cache['mtime'][fp_abs] = P.getmtime(fp_abs)
                    counters['files_stat'] += 1
                
        LOG.debug('cache:   END: %s', self.root)
        self.cache.sync()
        if P.exists(j(r, '.dirty')):
            os.remove(j(r, '.dirty'))
            
        counters['directories_processed'] += 1
        return counters
    
    def _destroy_cache(self):
        """Destroy cache from memory"""
        del self.cache  # cache may be big
    
    def _load_cache(self, reset=False):
        """
        reset --- ignore disk cache and load empty cache (next sync will write it)
        """
        if not hasattr(self, 'cache'):
            self.cache = _UpTreeCache(self._cache_file, reset)



def _ls(d):
    """Return non-hidden files and directories in ``d``
    """
    assert P.isabs(d), d
    return [c for c in os.listdir(d) if not c.startswith('.')]
    
def parentdir(p):
    """Return the parent directory of ``p``"""
    return P.abspath(P.join(p, P.pardir))
    
    
class _PersistentDict(dict):
    
    def __init__(self, cache_file, reset, **items):
        super(_PersistentDict, self).__init__(**items)
        self._cache_file = cache_file
        if not reset:
            self._load()
        
    def _load(self):
        if P.exists(self._cache_file):
            # LOG.debug('cache: opening %s', self._cache_file)
            with open(self._cache_file, 'rb') as f:
                try:
                    self.update(pickle.load(f))
                except:
                    LOG.error('error reading from cache: %s', self._cache_file)
                    raise
                
    def sync(self):
        """Sync to disk"""
        _cache_dir = P.dirname(self._cache_file)
        if not P.exists(_cache_dir):
            os.makedirs(_cache_dir)
        # LOG.debug('cache: syncing to %s', self._cache_file)
        with open(self._cache_file, 'wb') as f:
            try:
                pickle.dump(dict(self), f, protocol=pickle.HIGHEST_PROTOCOL)
            except:
                LOG.error('error during syncing cache: %s', self._cache_file)
                raise
        
        
class _UpTreeCache(_PersistentDict):
    """A cache of file listings (``files``) and file contents (``data``)"""
    
    def __init__(self, cache_file, reset):
        super(_UpTreeCache, self).__init__(cache_file, reset, files=set(), data={})
        
    def add_sub_cache(self, sub):
        """Add the subdirectory's cache"""
        b = P.basename(P.dirname(self._cache_file))
        self['files'].update(sub['files'])
        self['data'].update(sub['data'])
        self['mtime'].update(sub['mtime'])
    
    def clear(self):
        super(_UpTreeCache, self).clear()
        self['files'] = set()
        self['data'] = {}
        self['mtime'] = {}
        
        
if __name__ == '__main__':
    import sys
    logging.basicConfig(level=logging.DEBUG)
    action, root = sys.argv[1:3]
    force = False
    if action == 'forceupdate':
        action = 'update'
        force = True
        
    uptree = UpTree(
        root,
        content_cache_filenames=['info.json', 'imports'],
        mtime_cache_filenames=['log'])
    
    if action == 'update':
        counters = uptree.update(force=force)
        files = list(uptree.get_files())
        print('Cache has %d files;\n%s\n...' % (len(files), '\n'.join(files[:5])))
        read_files = uptree.cache['data'].keys()
        print('Cache has %d file data;\n%s\n...' % (len(read_files),
                                                    '\n'.join(read_files[:5])))
        print(counters)
    elif action == 'dirty':
        uptree.mark_dirty(sys.argv[3])
    elif action == 'list':
        uptree._load_cache()
        for f in uptree.get_files():
            print(f)
    else:
        raise SystemExit('unknown action: %s', action)
