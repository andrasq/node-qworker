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


    # file scripts/ping.js:
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

Options take effect in new worker processes created to run the script, so for
predictability always use the same options for the same script.

### runner.defaults( [options] )

Create a new job runner with the combined settings of both the existing runner and the
new options.  Options are as for `qworker()`.  Currently, both the parent and the new
runner share worker queues.


## ChangeLog

- 0.5.0 - simplify package layout, option to set a job mutex
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
- option `require: []` to jobs to pre-load dependencies eg coffee-script/register