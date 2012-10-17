uptree
======

Python module for caching directory traversal and reading of larger number of small files.

Example
-------

UpTree keeps an updated version of recursive file listing, and contents of selected files in cache ... while providing a mechanism to mark directories as changed based on dirty files.
    
Code that is changing files should generally do::
 
    >>> ut = uptree.UpTree('/path/to/repo')
    >>> ut.mark_dirty('/path/to/repo/the/dir/of/modified/file')
        
Code traversing the directory tree should generally do::
    
    >>> ut = uptree.UpTree('/path/to/repo')
    >>> ut.update()
    >>> for file in ut.get_files():
    >>>     ...
    >>>     data = ut.open(some_file).read()
    >>>     ...