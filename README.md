qworker
=======

[![Build Status](https://api.travis-ci.org/andrasq/node-qworker.svg?branch=master)](https://travis-ci.org/andrasq/node-qworker?branch=master)
[![Coverage Status](https://codecov.io/github/andrasq/node-qworker/coverage.svg?branch=master)](https://codecov.io/github/andrasq/node-qworker?branch=master)


Worker processes to run nodejs scripts.  Each script is run in a separate child process.

    var qworker = require('qworker');

    runnerOptions = {
        maxWorkers: 4,
        timeout: 2000,
        scriptDir: './scripts',
    };
    var runner = qworker(runnerOptions);

    runner.run('ping', { a:1, b:'two' }, function(err, ret) {
        console.log("ping returned", err, ret);
        // => ping returned null { a: 1, b: 'two' }
    })


    # file ./scripts/ping.js:
    module.exports = function ping( payload, callback ) {
        callback(null, payload);
    }


## Api

### runner = qworker( [options] )

Create a job runner.

Options:
- `maxWorkers` - how many workers may run at the same time.  Default 2.
- `timeout` - how long a job may take to finish, in milliseconds.
  The default is `0`, unlimited.
- `scriptDir` - where to search for scripts with relative pathnames.
  The default is ".", the current working directory of the node process.
  Anchored script names (those starting with `/`) are loaded by explicit pathname.
- `maxUseCount` - how many scripts a worker process may run before being retired.
  Default 1, use a new process for each script.
- `niceLevel` - worker process unix priority level, 19 lowest, -19 highest, default 0.
- `idleTimeout` - have worker processes exit after a period of inactivity, in milliseconds
- `exitTimeout` - how many milliseconds to allow for a worker to to exit when told to stop.
  Default 2000 ms.
- `require` - hash of name-path pairs of packages to load for the script.
  The packages are preloaded into the script global environment before it is launched.

### runner.run( script, [payload], callback( err, ret ) )

Run the named script.  Each script is run in a separate child process.  Fork errors
are returned via the callback.  If `script` or `callback` are not provided, an `Error`
is thrown.  If the script is not found or throws while loading, an error is returned
to the callback.

The script may be passed an optional argument (the `payload`).  The `callback` will be
called by the runner to deliver results or errors from the script.

Scripts are defined in separate files as an exported function taking one argument and
a standard callback:

    module.exports = function sampleScript( payload, callback ) {
        callback(null, 'sample script done!');
    }

### runner.runWithOptions( script, options, [payload,] callback( err, ret ) )

Like `run()`, but pass additional options when creating the worker process.

Options:
- `timeout` - how long a job may take to finish, in milliseconds.
  If zero, uses the runner default.
- `niceLevel` - worker process unix priority level, 19 lowest, -19 highest.
  If zero, uses the runner defdault.
- `lockfile` - use the file for a job mutex.  Stores the worker `pid` in the file
  while the job is running, removes the file when the job is done.  Abandoned locks
  are overwritten.  Default is no mutex.
- `idleTimeout` - have worker process exit after so many ms of inactivity
- `require` - hash of name-path pairs of packages to load for the script.
  The packages are preloaded into the script global environment before it is launched.
  This option overrides the qworker option of the same name.
- `eval` - javascript source of script to run, parsed with `eval()`.  If this option
  is present, the `script` argument is used for categorization and scheduling only.

Some options take effect in newly created worker processes, but the script may be run
by an existing worker process.  For predictability always use the same options for the
same script.

### runner.defaults( [options] )

Create a new job runner with the combined settings of both the existing runner and the
new options.  Options are as for `qworker()`.  Currently, both the parent and the new
runner share worker queues.

### runner.close( options )

Shut down all worker processes.  Each worker will be sent a 'stop' message, and if still
running 2 seconds later (`exitTimeout`), killed with SIGKILL.

Options:
- `exitTimeout` - how many milliseconds to allow for a worker to exit when told to stop.
  If specified, this value overrides the `exitTimeout` specified in the constructor.


## ChangeLog

- 0.8.0 - `require` job option, `eval` job option
- 0.7.1 - replace processExists with processNotExists
- 0.7.0 - `close` method, replace MvCache with mv hash functions, document `exitTimeout`
- 0.6.1 - `idleTimeout` option, use own MvCache, clean up after killed processes
- 0.5.0 - simplify package layout, fix duplicate 'done' callbacks, `lockfile` option to set a job mutex
- 0.4.0 - `niceLevel` job runner option, `runWithOptions` method
- 0.3.1 - fix processExists for non-numeric pids on node-v0.10
- 0.3.0 - kill scripts that exceed their timeout, fix worker reuse

## TODO

Future work:
- "qworker" pseudo-script that executes meta-commands (eg purge cache, reload config, etc)
- make it possible for a worker to run multiple types of scripts
- make stopTimeout, exitTimeout configurable
- cap the total number of worker processes
- log a comment if worker is killed (ie, not asked to stop)
- emit `fork`, `exit`, `error` and `trace` events
- option `eval` to parse the script name as a function script, and run the function
