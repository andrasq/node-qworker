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

var fs = require('fs');
var qinvoke = require('qinvoke');


if (process.env.NODE_QWORKER) {
    // qworker mode, start the worker, listen for work

    // TODO: var disrequire = require('disrequire');

    new QwWorker().runScripts();
}
else {
    // controller mode, create worker and tell it to run the script

    var child_process = require('child_process');
    var qhash = require('qhash');
    var quickq = require('quickq');

    module.exports = QwRunner;
}


function QwWorker( ) {
    // cannot use a prototype unless the worker runs that part of the script first
    this.runScripts = runScripts;
    this.runScriptJob = runScriptJob;
    this.sendTo = sendTo;
    this.idleTimer = null;
}

// worker:
// worker process is passed the script and payload
function runScripts() {
    // tell parent that ready to receive jobs
    this.sendTo(process, { pid: process.pid, qwType: 'ready' });

    var self = this;
    process.on('message', function(parentMessage) {
        clearTimeout(this.idleTimer);
        switch (parentMessage && parentMessage.pid === process.pid && parentMessage.qwType) {
        case 'job':             // run the script on payload in "job"
            runScriptJob(parentMessage.job, function(err, result) {
                if (err && err instanceof Error) err = qinvoke.errorToObject(err);
                self.sendTo(process, { pid: process.pid, qwType: 'done', err: err, result: result });
                if (parentMessage.job.idleTimeout > 0) self.idleTimer = setTimeout(exitProcess, +parentMessage.job.idleTimeout);
            })
            break;
        case 'stop':            // close IPC socket and exit naturally
            exitProcess();
            break;
        }
    })

    function exitProcess() {
        // force exit if disconnect doesnt do the job
        // TODO: make this stopTimeout configurable
        setTimeout(process.exit, 2000).unref();
        // disconnect, but suppress "already disconnected" errors
        try { process.disconnect() } catch (e) { }
    }
}

// worker:
// worker scripts export a function taking a single argument and a callback
function runScriptJob( job, callback ) {
    try {
        // if reusing workers for different scripts, periodically disrequire(job.script)
        // see the `disrequire` npm package
        var runner = require(job.script);
        runner(job.payload, callback);
    }
    catch (err) {
        callback(err);
    }
}

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
    options = options || {};
    if (! (this instanceof QwRunner)) return new QwRunner(options);
    var self = this;

    this.maxWorkers = options.maxWorkers || 2;                  // num script runners to allow in parallel
    this.jobTimeout = options.timeout || 0;                     // ms before script must be done, 0 means unlimited
    this.scriptDir = options.scriptDir || process.cwd();        // directory containing the scripts to run
    this.exitTimeout = options.workerExitTimeout || 2000;       // ms before runner kills the worker process
    this.maxUseCount = options.maxUseCount || 1;                // num scripts a worker may run before being retired
    this.niceLevel = options.niceLevel || 0;                    // unix system priority: 19 is lowest, -19 highest
    this.idleTimeout = options.idleTimeout || 0;                // worker to exit after ms with no work to do

    function jobRunner( job, cb ) {
        self.jobRunner(job, cb);
    }
    this._workQueue = quickq(jobRunner, this.maxWorkers);
    this._workers = new Array();
    this._workerPool = {};
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

QwRunner.prototype.close = function close( ) {
    var worker;
    var scripts = this._workerPool.getKeys();

    for (var i=0; i<scripts.length; i++) {
        while ((worker = this._workerPool.shift(scripts[i]))) {
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
    var job = {
        script: script, payload: payload, timeout: jobTimeout, niceLevel: niceLevel,
        idleTimeout: idleTimeout, lockfile: lockfile, };
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
        self.sendTo(worker, { pid: worker.pid, qwType: 'job', job: job });
    }
}

// obtain a worker to run the job, either from the workerPool or create new
QwRunner.prototype.createWorkerProcess = function createWorkerProcess( script, job, callback ) {
    // set the qworkerId so the forked worker inherits it
    process.env.NODE_QWORKER = this._nextWorkerId++;

    do {
        var worker = this.mvRemove(this._workerPool, script, 0);
    } while (worker && !this.processExists(worker));
    if (worker) {
        // return before callback, to simplify unit tests
        process.nextTick(function(){ callback(null, worker) });
        return worker;
    }

    try {
        var worker = child_process.fork(__filename);
        var pid = worker.pid;
        worker._qworkerId = process.env.NODE_QWORKER;
        worker._useCount = 0;
        worker._script = script;
        this._workers.push(worker);

        // if process dies or is killed, clean up
        var self = this, onExitCleanup, gotStartedMessage = false;
        worker.once('exit', onExitCleanup = function(code, signal) {
            worker._stopped = true;
            self.endWorkerProcess(worker, noop);
            if (!gotStartedMessage) callback(new Error('process exited with ' + code + ' / ' + signal));
        });

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
    if (ix >= 0) this._workers.splice(ix, 1);
    // TODO: splice is slow, store undefined and periodically compact the list

    this.mvDelete(this._workerPool, worker._script, worker);

    // only process the worker once
    if (worker._kstopped) return callback(null, worker);
    worker._kstopped = true;

    // TODO: test with processNotExists
    if (!this.processExists(worker)) {
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
    if (!(proc.pid > 0)) return false;

    try {
        process.kill(proc.pid, 0);
        return true;
    } catch (err) {
        // if the process is not found it does not exist
        // if (err.code === 'ESRCH') >= 0) return false;
        // a process that exists but is not ours is a sign of a bad pid, ignore it
        // if (err.code === 'EPERM') >= 0) return true;
        // on all other errors assume not exist
        return false;
    }
}

// verify that that the process `pid` is not running
QwRunner.prototype.processNotExists = function processNotExists( pid ) {
    try { process.kill(+pid, 0); return false }
    catch (err) { return err.code === 'ESRCH' }
}

// set a job mutex in the filesystem (file containing the lock owner pid)
// If the mutex exists but the that process is gone, override the old lock.
QwRunner.prototype.setLock = function setLock( lockfile, ownerPid ) {
    var fd, self = this;

    try {
        fd = fs.openSync(lockfile, 'wx');
        fs.closeSync(fd);
        fs.writeFileSync(lockfile, String(ownerPid));
    }
    catch (err) {
        if (err.code === 'EEXIST') {
            breakAbandonedLock(lockfile);
            setLock(lockfile, ownerPid);
        }
        else throw err;
    }

    // break the lock if is abandoned, or throw if cannot break
    function breakAbandonedLock( lockfile ) {
        var pid = fs.readFileSync(lockfile);
        if (self.processNotExists(pid)) fs.unlinkSync(lockfile);
        else throw new Error(lockfile + ': cannot break lock, process ' + pid + ' exists');
    }
}

// clear a job mutex.  The job must not exist or must be ours (contain our lock owner pid).
QwRunner.prototype.clearLock = function clearLock( lockfile, ownerPid ) {
    try {
        var pid = String(fs.readFileSync(lockfile));
        if (pid === String(ownerPid) || this.processNotExists(pid)) fs.unlinkSync(lockfile);
        else throw new Error('not our lock');
    }
    catch (err) {
        if (err.code === 'ENOENT') return;
        throw err;
    }
}

// expose on prototype to tests
QwRunner.prototype.sendTo = sendTo;
QwRunner.prototype.mvPush = mvPush;
QwRunner.prototype.mvDelete = mvDelete;
QwRunner.prototype.mvRemove = mvRemove;

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

QwRunner.prototype = toStruct(QwRunner.prototype)
function toStruct( proto ) { return toStruct.prototype = proto; }
