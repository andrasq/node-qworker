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


if (process.env.NODE_QWORKER) {
    // qworker mode, start the worker, listen for work
    var worker = new QwWorker();
    worker.runWorker();
}
else {
    // controller mode, create worker and tell it to run the script
    var child_process = require('child_process');
    var quickq = require('quickq');

    module.exports = QwRunner;
}


function QwWorker( ) {
    // cannot use from prototype unless the worker runs that part of the script first
    this.runWorker = runWorker;
    this.runWorkerJob = runWorkerJob;
    // this.uncacheModule = uncacheModule;
}

// worker:
// worker process is passed the script and payload
function runWorker() {
    // tell parent that ready to receive jobs
    sendTo(process, { pid: process.pid, qwType: 'ready' });

    process.on('message', function(parentMessage) {
        switch (parentMessage && parentMessage.pid === process.pid && parentMessage.qwType) {
        case 'job':             // run the script on payload in "job"
            runWorkerJob(parentMessage.job, function(err, result) {
                if (err && err instanceof Error) err = qinvoke.errorToObject(err);
                sendTo(process, { pid: process.pid, qwType: 'done', err: err, result: result });
            })
            break;
        case 'stop':            // close IPC socket, exit naturally
            // force exit if disconnect doesnt do the job
            setTimeout(process.exit, 2000).unref();
            process.disconnect();
            break;
        default:
            // re-emit parent messages eg 'listen', 'stop' for the script as 'qwMessage'
            process.emit('qwMessage', parentMessage.qwType, parentMessage);
            break;
        }
    })
}

// worker:
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
// worker:
function uncacheModule( path ) {
    var module = require.cache[path];
    if (module) {
        var ix = module.parent.children.indexOf(module);
        if (ix >= 0) module.parent.children.splice(ix, 1);
        delete require.cache[path];
    }
}
**/

function QwRunner( options ) {
    if (!options) options = {};
    if (! (this instanceof QwRunner)) return new QwRunner(options);
    var self = this;

    var MAX_WORKERS = options.maxWorkers || 2;
    var TIMEOUT = options.timeout || 0;
    var SCRIPT_DIR = options.scriptDir || process.cwd();
    if (SCRIPT_DIR[0] !== '/') SCRIPT_DIR = process.cwd() + '/' + SCRIPT_DIR;

    function runJob( job, cb ) {
        self.jobRunner(job, cb);
    }
    self.workQueue = quickq(runJob, MAX_WORKERS);

    this.run = function run( script, payload, callback ) {
        if (!script || typeof script !== 'string') throw new Error("qworker: missing script name");
        if (!callback) { callback = payload; payload = null }
        if (!callback) throw new Error("qworker: callback required");

        // unanchored scripts are script-dir relative
        // ./ and ../ ancored scripts are considered unanchored
        if (script[0] !== '/') script = SCRIPT_DIR + '/' + script;

        var job = { script: script, payload: payload, timeout: TIMEOUT };
        self.workQueue.push(job, callback);
    };

    this.defaults = function defaults( options ) {
        return new QwRunner(qhash.merge(this._options, options));
    };

    // save a copy of the options we got
    // assign some objects directly, not a deep copy
    this._options = qhash.merge({ _workQueue: null }, options);
    this._options._workQueue = self.workQueue;

    return this;
}

// function to start the worker, pass it the script and payload, and wait for the results
// This function is called with each job to start.
// TODO: make a method, instead of passing params (eg .timeout) in the job
QwRunner.prototype.jobRunner = function jobRunner( job, cb ) {
    var worker = this.createWorkerProcess(job.script);
    if (worker instanceof Error) {
        // TODO: configure whether errors are logged to console
        console.log(new Date().toISOString() + " -- qworker fork error: ", worker);
        return cb(worker);
    }

    var timeoutTimer;
    var cbCalled = false;
    function cbOnce(err, ret) {
        clearTimeout(timeoutTimer);
        if (!cbCalled) {
            cbCalled = true;
            cb(err, ret);
        }
    }

    if (job.timeout > 0) {
        timeoutTimer = setTimeout(function(){
            QwRunner.prototype.endWorkerProcess(worker);
            cbOnce(new Error("script timeout"));
        }, job.timeout).unref();
    }

    if (worker._useCount > 0) {
        // worker has already been initialized, send it the job
        sendTo(worker, { pid: worker.pid, qwType: 'job', job: job });
    }
    else {
        worker.on('error', function(err) {
            QwRunner.prototype.killWorkerProcess(worker);
            cbOnce(err || new Error("script error"));
        })

        var returnOnExit;
        worker.once('exit', returnOnExit = function(code) {
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
                worker.removeListener('exit', returnOnExit);
                QwRunner.prototype.endWorkerProcess(worker);
                var err = workerMessage.err ? qinvoke.objectToError(workerMessage.err) : workerMessage.err;
                cbOnce(err, workerMessage.result);
                break;
            }
        })
    }
}

var workers = [];
var workerPool = new mvcache();
var nextWorkerId = 1;
QwRunner.prototype.createWorkerProcess = function createWorkerProcess( script ) {
    process.env.NODE_QWORKER = nextWorkerId++;

    var worker = workerPool.get(script);
    if (worker) return worker;

    try {
        var worker = child_process.fork(__filename);
        worker._qworkerId = process.env.NODE_QWORKER;
        worker._useCount = 0;
        worker._script = script;
        workers.push(worker);
        return worker;
    }
    catch (err) {
        return err;
    }
}

var stopTimeout = 50;
var exitTimeout = 2000;
var maxUseCount = 1;
QwRunner.prototype.endWorkerProcess = function endWorkerProcess( worker, optionalCallback ) {
    if (worker._kstopped) return;
    worker._kstopped = true;

    if (++worker._useCount < maxUseCount) {
        // TODO: reuse worker processes for a while, time out if inactive too long
        worker._kstopped = false;
        workerPool.push(worker._script, worker);
        if (optionalCallback) optionalCallback(null, worker);
    }
    else {
        var ix = workers.indexOf(worker);
        if (ix >= 0) workers.splice(ix, 1);

        if (!this.processExists(worker)) {
            if (optionalCallback) optionalCallback(null, worker);
            return;
        }

        // tell the worker to stop, follow it up with an exit, reinforce with a kill
        // TODO: configure the exit timeout
        // TODO: make it a method to have clean linkage for exitTimeout, maxUseCount settings
        sendTo(worker, { pid: worker.pid, qwType: 'stop' });
        var stopTimer = setTimeout(sendTo, stopTimeout, worker, { pid: worker.pid, qwType: 'exit' }).unref();
        var exitTimer = setTimeout(function(){ try { worker.kill() } catch(err) { } }, exitTimeout).unref();

        worker.once('exit', function(exitcode) {
            // TODO: make pool be per instance, not a global
            clearTimeout(stopTimer);
            clearTimeout(exitTimer);
            if (optionalCallback) optionalCallback(null, worker);
        })
    }
}

// forcibly terminate the worker
QwRunner.prototype.killWorkerProcess = function killWorkerProcess( worker, optionalCallback ) {
    // TODO: this.emit('trace', "worker %d: killed", worker.pid);
    try { worker.kill('SIGKILL') }
    catch (err) { }
    if (optionalCallback) worker.once('exit', function() {
        optionalCallback(null, worker);
    })
}

// return true if the process exists
QwRunner.prototype.processExists = function processExists( proc ) {
    try {
        process.kill(proc.pid, 0);
        return true;
    } catch (err) {
        return err.message.indexOf('ESRCH') < 0;
    }
}

// expose on prototype to tests
QwRunner.prototype.sendTo = sendTo;

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
