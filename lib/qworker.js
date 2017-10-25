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
var mvcache = require('qcache/mvcache');


var _helpers = {
    createWorkerProcess: createWorkerProcess,
    endWorkerProcess: endWorkerProcess,
    sendTo: sendTo,
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
        var TIMEOUT = options.timeout || 0;
        var SCRIPT_DIR = options.scriptDir || process.cwd();
        if (SCRIPT_DIR[0] !== '/') SCRIPT_DIR = process.cwd() + '/' + SCRIPT_DIR;

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

            var job = { script: script, payload: payload, timeout: TIMEOUT };
            workQueue.push(job, callback);
        };

        self.defaults = function defaults( options ) {
            return buildController(qhash.merge(this._options, options));
        };

        // save a copy of the options we got
        // assign some objects directly, not a deep copy
        self._options = qhash.merge({ _workQueue: null }, options);
        self._options._workQueue = workQueue;

        return self;
    };
    module.exports._helpers = _helpers;
}

// worker process is passed the script and payload
function runWorker() {
    // tell parent that ready to receive jobs
    sendTo(process, { pid: process.pid, qwType: 'ready' });

    process.on('message', function(parentMessage) {
        switch (parentMessage && parentMessage.qwType) {
        case 'job':
            runWorkerJob(parentMessage.job, function(err, result) {
                if (err && err instanceof Error) err = qinvoke.errorToObject(err);
                sendTo(process, { pid: process.pid, qwType: 'done', err: err, result: result });
            })
            break;
        case 'exit':
            setImmediate(process.exit);
            break;
        default:
            // re-emit parent messages eg 'listen', 'stop' for the script as 'kwMessage'
            // FIXME: test that parentMessage is not falsy
            // if (parentMessage.pid === process.pid):
            process.emit('kwMessage', parentMessage.qwType, parentMessage);
            break;
        }
    })
}

// function to start the worker, pass it the script and payload, and wait for the results
// This function is called with each job to start.
// TODO: make a method, instead of passing params (eg .timeout) in the job
function jobRunner( job, cb ) {
    var worker = _helpers.createWorkerProcess(job.script);
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
            endWorkerProcess(worker);
            // TODO: follow the 'exit' with a kill after exitTimeout
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
        switch (workerMessage && workerMessage.qwType) {
        case 'ready':
            // worker is good to go, send it the job
            sendTo(worker, { pid: worker.pid, qwType: 'job', job: job });
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
        // if reusing workers for different scripts, periodically uncacheModule(job.script)
        var runner = require(job.script);
        runner(job.payload, callback);
    }
    catch (err) {
        callback(err);
    }
}

/**
function uncacheModule( path ) {
    var module = require.cache[path];
    if (module) {
        var ix = module.parent.children.indexOf(module);
        if (ix >= 0) module.parent.children.splice(ix, 1);
        delete require.cache[path];
    }
}
**/

var workers = [];
var workerPool = new mvcache();
var nextWorkerId = 1;
function createWorkerProcess( script ) {
    process.env.NODE_KWORKER = nextWorkerId++;

    var worker = workerPool.get(script);
    if (worker) return worker;

    try {
        var worker = child_process.fork(__filename);
        worker._script = script;
        worker._useCount = 0;
        workers.push(worker);
        return worker;
    }
    catch (err) {
        return err;
    }
}

var exitTimeout = 2000;
// FIXME: maxUseCount=2 breaks the unit tests
var maxUseCount = 1;
function endWorkerProcess( worker, optionalCallback ) {
    if (worker._kstopped) return;
    worker._kstopped = true;

    // tell the worker to stop
    // wait for stopped

    // tell the worker to stop, follow it up with an exit, reinforce with a kill
    sendTo(worker, { pid: worker.pid, qwType: 'stop' });
    setTimeout(sendTo, 20, worker, { pid: worker.pid, qwType: 'exit' });

    // to ensure shutdown, kill the worker after 2 seconds
    // TODO: configure the exit timeout
    // TODO: make a method to have clean linkage for exitTimeout, maxUseCount settings
    var exitTimer = setTimeout(function(){
        try { worker.kill() } catch(err) { }
    }, exitTimeout).unref();

    worker.once('exit', function() {
        // TODO: make pool be per instance, not a global
        clearTimeout(exitTimer);
        if (++worker._useCount < maxUseCount) {
            worker._kstopped = false;
            workerPool.push(worker._script, worker);
        }
        else {
            var ix = workers.indexOf(worker);
            workers.splice(ix, 1);
        }
        if (optionalCallback) optionalCallback(null, worker);
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
