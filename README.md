# CTA Frequency Server

This isn't really ready for public consumption, so more detail later. But for the moment, this note: You will not be able to run this software using the standard S-SQL as packaged by Quicklisp, as it contains some bugs relating to window functions. Until/unless my pull request is merged, you'll need to use the patched version on the `window-partition-by` branch at https://github.com/enaeher/Postmodern.