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
- `scriptDir` - where to search for scripts with relative pathnames,
  relative to the process working directory (`process.cwd()`).
  The default is ".", the current working directory of the node process.


### runner.run( script, [payload], callback( err, ret ) )

Run the named script.  Each script is run in a separate child process.  Fork errors
are returned via the callback.  If `script` or `callback` are missing, an `Error` is
thrown.

The script may be passed an optional argument (the `payload`).  The `callback` will be
called by the runner to deliver results or errors from the script.

Scripts are defined in separate files as an exported function taking one argument and
a standard callback:

    module.exports = function sampleScript( payload, callback ) {
        callback(null, 'sample script done!');
    }

### runner.defaults( [options] )

Create a new job runner with the combined settings of both the existing runner and the
new options.  Options are as for `qworker()`.  Currently, both the parent and the new
runner share worker queues.


## TODO

Future work:
- "qworker" pseudo-script that executes meta-commands (eg purge cache, reload config, etc)
- make worker processes reusable (currently disabled)
- make it possible for a worker to run multiple types of scripts
- make stopTimeout, exitTimeout configurable
- cap the total number of worker processes
