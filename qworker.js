/**
 * script runner work queue
 * Script is run by a worker thread in a separate child_process.
 *
 * Copyright (C) 2017-2018 Andras Radics
 * Licensed under the Apache License, Version 2.0
 *
 * 2017-03-28 - AR.
 * 2017-10-23 - rewrite.
 */

'use strict';


if (process.env.NODE_QWORKER) {
    // qworker mode, start the worker, listen for work

    // TODO: var disrequire = require('disrequire');

    new QwWorker().runScripts();
}
else {
    // controller mode, create worker and tell it to run the script

    var fs = require('fs');
    var qinvoke = require('qinvoke');

    var child_process = require('child_process');
    var quickq = require('quickq');
    var qfslock = require('qfslock');

    module.exports = QwRunner;
}


function QwWorker( ) {
    // cannot use a prototype unless the worker runs that part of the script first
    this.runScripts = runScripts;
    this.runScriptJob = runScriptJob;
    this.sendTo = sendTo;
    this.idleTimer = null;
    this.runningCount = 0;

    this._exitAsap = false;
}

// worker:
// worker process is passed the script and payload
// worker process waits for jobs and runs them
function runScripts() {
    // tell parent that ready to receive jobs
    this.sendTo(process, { pid: process.pid, qwType: 'ready' });

    var self = this;
    process.on('message', function(parentMessage) {
        clearTimeout(self.idleTimer);
        switch (parentMessage && parentMessage.pid === process.pid && parentMessage.qwType) {
        case 'job':             // run the script on payload in "job"
            self.runningCount += 1;
            self.runScriptJob(parentMessage.job, function(err, result) {
                self.runningCount -= 1;
                if (err && err instanceof Error) err = errorToObject(err);
                self.sendTo(process, { pid: process.pid, qwType: 'done', err: err, result: result });
                if ((parentMessage.job.idleTimeout > 0 || self._exitAsap) && self.runningCount <= 0) {
                    // on idle, tell parent to stop us
                    // The indirection via the parent is used to avoid use-vs-exit race conditions.
                    var idleTimeout = self._exitAsap && 1 || parentMessage.job.idleTimeout;
                    self.idleTimer = setTimeout(notifyIdle, +idleTimeout).unref();
                }
            })
            break;
        case 'stop':            // close IPC socket and exit naturally
            exitProcess();
            break;
        }
    })
    process.on('disconnect', exitOnDisconnect);

    function exitOnDisconnect() {
        // exit now, or exit 1 ms after the last job ends, but do not wait longer than 30 sec
        // Note that a disconnected parent cannot receive the job error or results.
        // The 30 sec timeout is to guard against jobs that never call their callback.
        if (self.runningCount <= 0) exitProcess();
        else {
            self._exitAsap = true;
            setTimeout(exitProcess, 30000).unref();
        }
    }

    // copied here from qinvoke.errorToObject:
    function errorToObject( err ) {
        var obj = {};
        var names = Object.getOwnPropertyNames(err);
        for (var i=0; i<names.length; i++) obj[names[i]] = err[names[i]];
        return obj;
    }

    function exitProcess() {
        // paranoia: if the graceful disconnect-exit below does not take effect in 2 sec, brute-force exit
        // TODO: make this stopTimeout configurable
        setTimeout(process.exit, 2000).unref();

        // disconnect, or exit now on "already disconnected" errors
        // If the parent is itself exiting and does not process our 'disconnect',
        // The 'disconnect' listener that is installed will call back here and
        // our second disconnect() will throw, and we will then exit immediately.
        try { process.disconnect() } catch (e) { process.exit() }
    }

    function notifyIdle() {
        // ask parent to dismiss us, or exit if parent not reachable
        // (parent may choose to send a new job instead of disconnecting)
        self.sendTo(process, { pid: process.pid, qwType: 'idleTimeout' }) || exitProcess();
    }
}

// worker:
// worker scripts export a function taking a single argument and a callback
function runScriptJob( job, callback ) {
    try {
        // if reusing workers for different scripts, periodically disrequire(job.script)
        // see the `disrequire` npm package

        // TODO: only require once, and throw if global[k] already set
        if (job.require) {
            for (var k in job.require) {
                global[k] = require(job.require[k]);
            }
        }

        var runner;
        if (job.eval) {
            try { eval("runner = " + job.eval) }
            catch (e) { throw new Error("qworker eval error: " + e.message) }
        } else {
            var runner = require(job.script);
        }
        runner(job.payload, callback);
    }
    catch (err) {
        callback(err);
    }
}

QwWorker.prototype = toStruct(QwWorker.prototype);

// quick-and-easy multi-value hash (non-numeric values only)
// Store holds lists of values by key, empty lists are deleted.
// TODO: add these as pool methods on the runner
function mvPush( store, key, value ) {
    return store[key] ? store[key].push(value) : ((store[key] = new Array(value)), value) }
function mvDelete( store, key, value ) {
    mvRemove(store, key, store[key] && store[key].indexOf(value)) }
function mvRemove( store, key, ix ) {
    if (ix < 0 || !store[key]) return undefined;
    var list = store[key], ret = ix ? list.splice(ix, 1)[0] : list.shift();
    if (!list.length) delete store[key];
    return ret;
}

function QwRunner( options ) {
    if (! (this instanceof QwRunner)) return new QwRunner(options);

    options = options || {};
    var self = this;

    this.maxWorkers = options.maxWorkers || 2;                  // num script runners to allow in parallel
    this.jobTimeout = options.timeout || 0;                     // ms before script must be done, 0 means unlimited
    this.scriptDir = options.scriptDir || process.cwd();        // directory containing the scripts to run
    this.exitTimeout = options.exitTimeout || 2000;             // ms before runner kills the worker process
    this.maxUseCount = options.maxUseCount || 1;                // num scripts a worker may run before being retired
    this.niceLevel = options.niceLevel || 0;                    // unix system priority: 19 is lowest, -19 highest
    this.idleTimeout = options.idleTimeout || 0;                // worker to exit after ms with no work to do
    this.require = options.require || null;                     // packages to preload into global[]

    this._workQueue = quickq(function(job, cb) { self.jobRunner(job, cb) }, this.maxWorkers);
    this._workers = new Array();
    this._workerPool = {};
    this._nextWorkerId = 1;

    if (this.scriptDir[0] !== '/') this.scriptDir = process.cwd() + '/' + this.scriptDir;

    this.defaults = function defaults( options ) {
        return new QwRunner(mergeObjects({}, this._options, options));
    };

    // save a copy of the options we got
    // assign some objects directly, not a deep copy
    this._options = mergeObjects({}, options, { _workQueue: self._workQueue });

    return this;

    function mergeObjects( target /*, VARARGS */ ) {
        for (var i=1; i<arguments.length; i++) {
            var obj = arguments[i];
            for (var k in obj) target[k] = obj[k];
        }
        return target;
    }
}

QwRunner.prototype.close = function close( options ) {
    options = options || {};
    this.exitTimeout = options.exitTimeout || this.exitTimeout;

    var worker;
    var scripts = Object.keys(this._workerPool);

    for (var i=0; i<scripts.length; i++) {
        while ((worker = this.mvRemove(this._workerPool, scripts[i], 0))) {
            this.endWorkerProcess(worker, noop, true);
        }
    }
    for (var i=0; i<this._workers.length; i++) {
        this.endWorkerProcess(this._workers[i], noop, true);
    }
}

// function called by the user to schedule a job
QwRunner.prototype.run = function run( script, payload, callback ) {
    return this.runWithOptions(script, {}, payload, callback);
}

QwRunner.prototype.runWithOptions = function runWithOptions( script, options, payload, callback ) {
    if (!script || typeof script !== 'string') throw new Error("qworker: missing script name");
    if (!options || typeof options !== 'object') throw new Error("qworker: missing options");
    if (!callback) { callback = payload; payload = null }
    if (typeof callback !== 'function') throw new Error("qworker: callback required");

    // unanchored scripts are script-dir relative
    // ./ and ../ ancored scripts are considered unanchored
    if (script[0] !== '/') script = this.scriptDir + '/' + script;

    // pass the job options to the worker
    var jobTimeout = options.timeout || this.jobTimeout;
    var niceLevel = options.niceLevel || this.niceLevel;
    var idleTimeout = options.idleTimeout || this.idleTimeout;
    var lockfile = options.lockfile;
    var requirePackages = options.require || this.require;
    var evalSource = options.eval;
    var job = {
        script: script, payload: payload, timeout: jobTimeout, niceLevel: niceLevel,
        idleTimeout: idleTimeout, lockfile: lockfile, require: requirePackages, eval: evalSource, };
    this._workQueue.push(job, callback);
}

// function to start the worker, pass it the script and payload, and wait for the results
// This function is called with each job to start.
QwRunner.prototype.jobRunner = function jobRunner( job, cb ) {
    var self = this;

    var worker = this.createWorkerProcess(job.script, job, launchJob);
    if (worker instanceof Error) {
        // TODO: configure whether errors are logged to console
        console.log(new Date().toISOString() + " -- qworker fork error: ", worker);
        return cb(worker);
    }

    // set _reserved to not let an idleTimeout kill the process before it starts the job
    worker._reserved = true;

    // when worker is ready to receive messages, send it the job
    // and wait for the job to finish.  The worker is reusable once the job is done.
    function launchJob( err ) {
        if (err) return cb(err);

        var timeoutTimer;
        var cbCalled = false;
        var cbOnce = function cbOnce( err, ret ) {
            clearTimeout(timeoutTimer);
            if (!cbCalled) {
                cbCalled = true;
                cb(err, ret);
            }
        }

        if (job.lockfile) {
            try { self.setLock(job.lockfile, worker.pid) }
            catch (err) { return cbOnce(err) }
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
        worker.once('exit', returnOnExit = function(code, signal) {
            cbOnce(new Error("worker died: " + code + ' / ' + signal));
        })

        var returnOnDone;
        worker.on('message', returnOnDone = function(workerMessage) {
            if (workerMessage && workerMessage.qwType === 'done') {
                worker._runningCount -= 1;
                if (worker._runningCount <= 0) worker._reserved = false;
                // processing finished, returns error or result
                // TODO: allow per-worker job concurrency > 1
                worker.removeListener('exit', returnOnExit);
                worker.removeListener('message', returnOnDone);
                worker.removeListener('error', killOnError);
                self.endWorkerProcess(worker, noop);
                if (job.lockfile) {
                    try { self.clearLock(job.lockfile, worker.pid) }
                    catch (err) { console.log(new Date().toISOString() + " -- qworker clearLock error: " + err) }
                }
                var err = workerMessage.err ? qinvoke.objectToError(workerMessage.err) : workerMessage.err;
                cbOnce(err, workerMessage.result);
            }
        })

        // once worker is all set up, send it the job
        worker._runningCount += 1;
        self.sendTo(worker, { pid: worker.pid, qwType: 'job', job: job });
    }
}

// obtain a worker to run the job, either from the workerPool or create new
QwRunner.prototype.createWorkerProcess = function createWorkerProcess( script, job, callback ) {
    // set the qworkerId so the forked worker inherits it
    process.env.NODE_QWORKER = this._nextWorkerId++;

    do {
        var worker = this.mvRemove(this._workerPool, script, 0);
    } while (worker && this.processNotExists(worker.pid));
    if (worker) {
        // delay callback to after return, to simplify unit tests
        process.nextTick(function(){ callback(null, worker) });
        return worker;
    }

    try {
        var worker = child_process.fork(__filename);
        var pid = worker.pid;
        worker._qworkerId = process.env.NODE_QWORKER;
        worker._useCount = 0;
        worker._script = script;
        worker._runningCount = 0;
        this._workers.push(worker);

        // if process dies or is killed, clean up
        var self = this, onExitCleanup, gotStartedMessage = false;
        worker.once('exit', onExitCleanup = function(code, signal) {
            worker._stopped = true;
            self.endWorkerProcess(worker, noop);
            if (!gotStartedMessage) callback(new Error('process exited with ' + code + ' / ' + signal));
        });

        // listen for and harvest idle workers
        worker.on('message', function(workerMessage) {
            if (workerMessage && workerMessage.qwType === 'idleTimeout') {
                // TODO: if workers allow concurrent jobs, only end worker once all jobs done
                // if (!worker._reserved) ...
                self.endWorkerProcess(worker, noop, true);
            }
        })

        // call callback once worker is running and processing messages
        worker.once('message', function onMessage(message) {
            gotStartedMessage = true;

            if (!job.niceLevel) return callback(null, worker);
            child_process.exec("renice " + +(job.niceLevel) + " " + pid + " 2>&1 #&& ps -l -p " + pid, function(err, stdout, stderr) {
                if (err) console.log("%s -- qworker: failed to renice process %d:", new Date().toISOString(), pid, err);
                callback(null, worker);
            });
        })
        return worker;
    }
    catch (err) {
        return err;
    }
}

// done with worker, recycle for reuse or discard
QwRunner.prototype.endWorkerProcess = function endWorkerProcess( worker, callback, forceQuit ) {
    // remove the worker from our caches
    var ix = this._workers.indexOf(worker);
    if (ix >= 0) ix > 0 ? this._workers.splice(ix, 1) : this._workers.shift();
    // TODO: splice is slow, store undefined and periodically compact the list

    this.mvDelete(this._workerPool, worker._script, worker);

    // only process the worker once
    if (worker._kstopped) return callback(null, worker);
    worker._kstopped = true;

    if (this.processNotExists(worker.pid)) {
        callback(null, worker);
        return;
    }

    // if the process is still usable, cache it for reuse
    if (++worker._useCount < this.maxUseCount && !forceQuit) {
        worker._kstopped = false;
        this.mvPush(this._workerPool, worker._script, worker);
        callback(null, worker);
    }
    else {
        // tell the worker to stop, follow it up with an exit, reinforce with a kill
        var self = this;

        this.sendTo(worker, { pid: worker.pid, qwType: 'stop' });
        var exitTimer = setTimeout(function(){
            self.killWorkerProcess(worker);
        }, this.exitTimeout).unref();

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

// verify that that the process `pid` is not running
QwRunner.prototype.processNotExists = function processNotExists( pid ) {
    return qfslock.processNotExists(pid);
}

// set a job mutex in the filesystem (file containing the lock owner pid)
// If the mutex exists but the that process is gone, override the old lock.
QwRunner.prototype.setLock = function setLock( lockfile, ownerPid ) {
    return qfslock.setLock(lockfile, ownerPid);
}

// clear a job mutex.  The job must not exist or must be ours (contain our lock owner pid).
QwRunner.prototype.clearLock = function clearLock( lockfile, ownerPid ) {
    return qfslock.clearLock(lockfile, ownerPid);
}

// expose on prototype to tests
QwRunner.prototype.sendTo = sendTo;
QwRunner.prototype.mvPush = mvPush;
QwRunner.prototype.mvDelete = mvDelete;
QwRunner.prototype.mvRemove = mvRemove;

// non-throwing send, ignores send errors
function sendTo( proc, message ) {
    try {
        // NOTE: node-v9 and node-v10 log an ERR_IPC_CHANNEL_CLOSED message
        // to the console, because the message is sent on nextTick
        // Test `worker.connected` to not send if connection is closed.
        // NOTE: newer node can throw uncaught EPIPE errors without a callback,
        // but node-v0.x do not support a callback.  Pad with a null handle to make old node ignore it.
        if (proc.connected) proc.send(message, null, function(err) {});
    }
    catch (err) {
        // suppress 'channel closed' errors
    }
}

function noop() {}

QwRunner.prototype = toStruct(QwRunner.prototype)
function toStruct( proto ) { return toStruct.prototype = proto; }
