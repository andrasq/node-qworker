/**
 * script runner work queue
 * Script is run by a worker thread in a separate child_process.
 *
 * Copyright (C) 2017 Andras Radics
 * Licensed under the Apache License, Version 2.0
 *
 * 2017-03-28 - AR.
 * 2017-10-23 - rewrite.
 */

'use strict';

var qinvoke = require('qinvoke');


if (process.env.NODE_QWORKER) {
    // qworker mode, start the worker, listen for work

    new QwWorker().runScripts();
}
else {
    // controller mode, create worker and tell it to run the script

    var child_process = require('child_process');
    var qhash = require('qhash');
    var mvcache = require('qcache/mvcache');
    var quickq = require('quickq');

    module.exports = QwRunner;
}


function QwWorker( ) {
    // cannot use a prototype unless the worker runs that part of the script first
    this.runScripts = runScripts;
    this.runScriptJob = runScriptJob;
    // this.uncacheModule = uncacheModule;
    this.sendTo = sendTo;
    // TODO: time out worker after a period of inactivity
}

// worker:
// worker process is passed the script and payload
function runScripts() {
    // tell parent that ready to receive jobs
    this.sendTo(process, { pid: process.pid, qwType: 'ready' });

    var self = this;
    process.on('message', function(parentMessage) {
        switch (parentMessage && parentMessage.pid === process.pid && parentMessage.qwType) {
        case 'job':             // run the script on payload in "job"
            runScriptJob(parentMessage.job, function(err, result) {
                if (err && err instanceof Error) err = qinvoke.errorToObject(err);
                self.sendTo(process, { pid: process.pid, qwType: 'done', err: err, result: result });
            })
            break;
        case 'stop':            // close IPC socket, exit naturally
            // force exit if disconnect doesnt do the job
            // TODO: make this stopTimeout configurable
            setTimeout(process.exit, 2000).unref();
            process.disconnect();
            break;
        }
    })
}

// worker:
// worker scripts export a function taking a single argument and a callback
function runScriptJob( job, callback ) {
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
    options = options || {};
    if (! (this instanceof QwRunner)) return new QwRunner(options);
    var self = this;

    this.maxWorkers = options.maxWorkers || 2;                  // num script runners to allow in parallel
    this.jobTimeout = options.timeout || 0;                     // ms before script must be done, 0 means unlimited
    this.scriptDir = options.scriptDir || process.cwd();        // directory containing the scripts to run
    this.exitTimeout = options.workerExitTimeout || 2000;       // ms before runner kills the worker process
    this.maxUseCount = options.maxUseCount || 1;                // num scripts a worker may run before being retired

    function jobRunner( job, cb ) {
        self.jobRunner(job, cb);
    }
    this._workQueue = quickq(jobRunner, this.maxWorkers);
    this._workers = new Array();
    this._workerPool = new mvcache();
    this._nextWorkerId = 1;

    if (this.scriptDir[0] !== '/') this.scriptDir = process.cwd() + '/' + this.scriptDir;

    this.defaults = function defaults( options ) {
        return new QwRunner(qhash.merge(this._options, options));
    };

    // save a copy of the options we got
    // assign some objects directly, not a deep copy
    this._options = qhash.merge({ _workQueue: null }, options);
    this._options._workQueue = self._workQueue;

    return this;
}

// function called by the user to schedule a job
QwRunner.prototype.run = function run( script, payload, callback ) {
    if (!script || typeof script !== 'string') throw new Error("qworker: missing script name");
    if (!callback) { callback = payload; payload = null }
    if (!callback) throw new Error("qworker: callback required");

    // unanchored scripts are script-dir relative
    // ./ and ../ ancored scripts are considered unanchored
    if (script[0] !== '/') script = this.scriptDir + '/' + script;

    var job = { script: script, payload: payload, timeout: this.jobTimeout };
    this._workQueue.push(job, callback);
}

// function to start the worker, pass it the script and payload, and wait for the results
// This function is called with each job to start.
QwRunner.prototype.jobRunner = function jobRunner( job, cb ) {
    var self = this;

    var worker = this.createWorkerProcess(job.script, launchJob);
    if (worker instanceof Error) {
        // TODO: configure whether errors are logged to console
        console.log(new Date().toISOString() + " -- qworker fork error: ", worker);
        return cb(worker);
    }

    // when worker is ready to receive messages, send it the job
    // and wait for the job to finish.  The worker is reusable once the job is done.
    function launchJob( ) {
        var timeoutTimer;
        var cbCalled = false;
        var cbOnce = function cbOnce( err, ret ) {
            clearTimeout(timeoutTimer);
            if (!cbCalled) {
                cbCalled = true;
                cb(err, ret);
            }
        }

        if (job.timeout > 0) {
            timeoutTimer = setTimeout(function(){
                self.killWorkerProcess(worker, function(err) {
                    self.endWorkerProcess(worker, noop);
                });
                cbOnce(new Error("script timeout"));
            }, job.timeout).unref();
        }

        var killOnError;
        worker.on('error', killOnError = function(err) {
            self.killWorkerProcess(worker);
            cbOnce(err || new Error("script error"));
        })

        var returnOnExit;
        worker.once('exit', returnOnExit = function(code) {
            cbOnce(new Error("worker exited: " + code));
        })

        var returnOnDone;
        worker.on('message', returnOnDone = function(workerMessage) {
            if (workerMessage && workerMessage.qwType === 'done') {
                // processing finished, returns error or result
                // TODO: allow per-worker job concurrency > 1
                worker.removeListener('exit', returnOnExit);
                worker.removeListener('done', returnOnDone);
                worker.removeListener('error', killOnError);
                self.endWorkerProcess(worker, noop);
                var err = workerMessage.err ? qinvoke.objectToError(workerMessage.err) : workerMessage.err;
                cbOnce(err, workerMessage.result);
            }
        })

        // once worker is all set up, send it the job
        self.sendTo(worker, { pid: worker.pid, qwType: 'job', job: job });
    }
}

QwRunner.prototype.createWorkerProcess = function createWorkerProcess( script, callback ) {
    // set the qworkerId so the forked worker inherits it
    process.env.NODE_QWORKER = this._nextWorkerId++;

    var worker = this._workerPool.get(script);
    if (worker) {
        if (callback) process.nextTick(function(){ callback(null, worker) });
        return worker;
    }

    try {
        var worker = child_process.fork(__filename);
        worker._qworkerId = process.env.NODE_QWORKER;
        worker._useCount = 0;
        worker._script = script;
        this._workers.push(worker);
        if (callback) worker.once('message', function onMessage(message) {
            // call callback once worker is running and processing messages
            callback(null, worker);
        })
        return worker;
    }
    catch (err) {
        return err;
    }
}

QwRunner.prototype.endWorkerProcess = function endWorkerProcess( worker, callback ) {
    if (worker._kstopped) return callback(null, worker);
    worker._kstopped = true;

    var ix = this._workers.indexOf(worker);
    if (ix >= 0) this._workers.splice(ix, 1);

    if (!this.processExists(worker)) {
        callback(null, worker);
        return;
    }

    if (++worker._useCount < this.maxUseCount) {
        // TODO: reuse worker, but time it out if inactive too long
        worker._kstopped = false;
        this._workerPool.push(worker._script, worker);
        callback(null, worker);
    }
    else {
        // tell the worker to stop, follow it up with an exit, reinforce with a kill
        var self = this;

        this.sendTo(worker, { pid: worker.pid, qwType: 'stop' });
        exitTimer = setTimeout(function(){
            self.killWorkerProcess(worker);
        }, this.exitTimeout).unref();

        var exitTimer;
        worker.once('exit', function(exitcode) {
            clearTimeout(exitTimer);
            callback(null, worker);
        })
    }
}

// forcibly terminate the worker
QwRunner.prototype.killWorkerProcess = function killWorkerProcess( worker, optionalCallback ) {
    // TODO: this.emit('trace', "worker %d: killed", worker.pid);
    if (optionalCallback) worker.once('exit', function() {
        optionalCallback(null, worker);
    })
    try { worker.kill('SIGKILL') }
    catch (err) { }
}

// return true if the process exists and is ours 
QwRunner.prototype.processExists = function processExists( proc ) {
    try {
        process.kill(proc.pid, 0);
        return true;
    } catch (err) {
        return err.message.indexOf('ESRCH') >= 0 ? false
             // a process that exists but is not ours is a sign of a bad pid, ignore it
             // : err.message.indexOf('EPERM') >= 0 ? true
             : false;
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

function noop() {}
