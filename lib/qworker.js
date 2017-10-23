/**
 * script runner work queue
 * Script is run by a worker thread in a separate child_process.
 *
 * Copyright (C) 2017 Andras Radics
 * Licensed under the Apache License, Version 2.0
 *
 * 2017-03-28 - AR.
 */

'use strict';

var qhash = require('qhash');
var qinvoke = require('qinvoke');


var _helpers = {
    createWorkerProcess: createWorkerProcess,
};

if (process.env.NODE_KWORKER) {
    // qworker mode, run the script
    runWorker();
}
else {
    // controller mode, create worker and tell it to run the script
    var child_process = require('child_process');
    var quickq = require('quickq');

    module.exports = function buildController(options) {
        if (!options) options = {};
        var self = (this instanceof buildController) ? this : { run: null, defaults: null, _options: null };

        var MAX_WORKERS = options.maxWorkers || 2;
        var SCRIPT_DIR = options.scriptDir || ".";
        var TIMEOUT = options.timeout || 0;

        var workQueue = quickq(jobRunner, MAX_WORKERS);

        self.run = function _queueJob(script, payload, callback) {
            if (!script) throw new Error("qworker: no script");
            if (!callback) { callback = payload; payload = null }
            if (!callback) throw new Error("qworker: callback required");
            // unanchored scripts are script-dir relative
            // ./ and ../ ancored scripts are considered unanchored
            if (script[0] !== '/' && SCRIPT_DIR) {
                script = SCRIPT_DIR + '/' + script;
            }

            // locate the script source file
            try { script = require.resolve(script) }
            catch (err) { return callback(err) }

            var job = { script: script, payload: payload, timeout: TIMEOUT };
            workQueue.push(job, callback);
        };

        self.defaults = function defaults(options) {
            return buildController(qhash.merge(this._options, options));
        };

        self._options = qhash.merge({ _workQueue: null, _helpers: null }, options);

        return self;
    };
}

// worker process is passed the script and payload
function runWorker() {
    sendTo(process, { pid: process.pid, kwType: 'ready' });

    process.on('message', function(parentMessage) {
console.log("AR: controller message", parentMessage);
        if (parentMessage) switch (parentMessage.kwType) {
        case 'job':
            runWorkerJob(parentMessage.job, function(err, result) {
                if (err && err instanceof Error) err = qinvoke.errorToObject(err);
                sendTo(process, { pid: process.pid, kwType: 'done', err: err, result: result });
            })
            break;
        case 'exit':
            setImmediate(process.exit);
            break;
        default:
            // re-emit parent messages eg 'listen', 'stop' for the script as 'kwMessage'
            if (parentMessage.pid === process.pid) {
                process.emit('kwMessage', parentMessage.kwType, parentMessage);
            }
            break;
        }
    })
}

// function to start the worker, pass it the script and payload, and wait for the results
// This function is called with each job to start.
function jobRunner(job, cb) {
    var worker = _helpers.createWorkerProcess();
    if (worker instanceof Error) {
        // TODO: configure whether errors are logged to console
        console.log(new Date().toISOString() + " -- qworker fork error: ", worker);
        return cb(worker);
    }

    var cbCalled = false;
    function cbOnce(err, ret) {
        if (!cbCalled) {
            cbCalled = true;
            cb(err, ret);
        }
    }

    var timeoutTimer;
    if (job.timeout > 0) {
        timeoutTimer = setTimeout(function(){
            sendTo(worker, { pid: worker.pid, kwType: 'exit' });
            worker.emit('error', new Error("job timeout"));
        }, job.timeout).unref();
    }

    worker.on('error', function(err) {
        clearTimeout(timeoutTimer);
        endWorkerProcess(worker);
        cbOnce(err);
    })

    worker.once('exit', function(code) {
        clearTimeout(timeoutTimer);
        endWorkerProcess(worker);
        cbOnce(new Error("worker exited: " + code));
    })

    worker.on('message', function(workerMessage) {
        if (!workerMessage || !workerMessage.kwType) return;
//console.log("AR: worker message", workerMessage);

        switch (workerMessage.kwType) {
        case 'ready':
            // worker is good to go, send it the job
            sendTo(worker, { pid: worker.pid, kwType: 'job', job: job });
            break;
        case 'done':
            // processing finished, returns error or result
            // TODO: allow per-worker job concurrency > 1
            clearTimeout(timeoutTimer);
            endWorkerProcess(worker);
            var err = workerMessage.err ? qinvoke.objectToError(workerMessage.err) : workerMessage.err;
            cbOnce(err, workerMessage.result);
            break;
        }
    })
}

// worker scripts export a function taking a single argument and a callback
function runWorkerJob( job, callback ) {
    try {
        var runner = require(job.script);
        runner(job.payload, callback);
        // TODO: if reusing the runner, periodically uncacheModule(job.script) to pick up changes
        // TODO: else the dead objects linger and tie up memory (eg for programmatically generated scripts)
    }
    catch (err) {
        callback(err);
    }
}

function uncacheModule( path ) {
    // remove cached module from sibling modules list
    var module = require.cache[path];
    var ix = module.parent.children.indexOf(module);
    if (ix >= 0) module.parent.children.splice(ix, 1);

    // remove cached copy from require.cache
    delete require.cache[path];
}

var workers = [];
var nextWorkerId = 1;
function createWorkerProcess( ) {
    process.env.NODE_KWORKER = nextWorkerId++;
    try {
        var worker = child_process.fork(__filename);
        workers.push(worker);
        return worker;
    }
    catch (err) {
        return err;
    }
    // TODO: reuse worker processes, each can read its script in a vm
}

function endWorkerProcess( worker ) {
    if (worker._kstopped) return;
    worker._kstopped = true;

    // tell the worker to stop
    // wait for stopped

    // tell the worker to exit
    sendTo(worker, { pid: worker.pid, kwType: 'exit' });

    // to ensure shutdown kill the worker after 20 seconds
    // TODO: configure stop timeout
    var exitTimer = setTimeout(function(){
        try { worker.kill() } catch(err) { }
    }, 20000).unref();

    worker.once('exit', function() {
        var ix = workers.indexOf(worker);
        if (ix >= 0) workers.splice(ix, 1);
        clearTimeout(exitTimer);
    })

    // TODO: reuse worker processes for a while, time out if inactive too long
}

// non-throwing send, ignores send errors
function sendTo( proc, message ) {
    try {
        proc.send(message);
        return true;
    }
    catch (err) {
        // suppress 'channel closed' errors
        return false;
    }
}
