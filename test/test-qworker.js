/**
 * Copyright (C) 2017 Andras Radics
 * Licensed under the Apache License, Version 2.0
 */

'use strict';

var child_process = require('child_process');

var qworker = require('../');
var runner = qworker({
    scriptDir: __dirname + '/scripts',
});

module.exports = {
    'should parse package.json': function(t) {
        require("../package.json");
        t.done();
    },

    'should export a function': function(t) {
        t.equal(typeof qworker, 'function');
        t.done();
    },

    'should build a job runner': function(t) {
        var runner1 = qworker();
        var runner2 = qworker();
        t.ok(runner1);
        t.equal(typeof runner1.run, 'function');
        t.ok(runner1 != runner2);
        t.done();
    },

    'should create a new job runner': function(t) {
        var runner1 = new qworker();
        var runner2 = new qworker();
        t.ok(runner1);
        t.equal(typeof runner1.run, 'function');
        t.ok(runner1 != runner2);
        t.done();
    },

    'options': {
        'should create runner with options': function(t) {
            var runner = qworker({ maxWorkers: 123, timeout: 100 });
            t.equal(runner._options.maxWorkers, 123);
            t.equal(runner._options.timeout, 100);
            t.done();
        },

        'should inherit parent options': function(t) {
            var runner1 = qworker({ maxWorkers: 123, timeout: 1 });
            var runner2 = runner1.defaults({ timeout: 2 });
            t.equal(runner2._options.maxWorkers, 123);
            t.equal(runner2._options.timeout, 2);
            t.done();
        },
    },

    'jobs': {
        'should fork child process to run a job': function(t) {
            var spy = t.stubOnce(child_process, 'fork', function(){ return {} });
            runner.run('fakeping', function(err, ret) {
                t.equal(spy.callCount, 1);
                t.done();
            })
        },

        'should run a job': function(t) {
            runner.run('ping', { x: process.pid }, function(err, ret) {
                t.ifError(err);
                t.deepEqual(ret, { x: process.pid });
                t.done();
            });
        },

        'should run a job by explicit filepath': function(t) {
            runner.run(__dirname + '/scripts_alt/ping', { x: process.pid }, function(err, ret) {
                t.ifError(err);
                t.deepEqual(ret, { x: process.pid, alt: true });
                t.done();
            })
        },

        'should run a job by relative filepath': function(t) {
// FIXME: a new runner reuses the worker, but does not use the right quickq!! (and does not call the callback)
            runner.run('../scripts_alt/ping', { x2: process.pid }, function(err, ret) {
                t.ifError(err);
                t.deepEqual(ret, { x2: process.pid, alt: true });
                t.done();
            })
        },

        'should wait for job to complete': function(t) {
            var startMs = Date.now();
            runner.run('sleep', { ms: 100 }, function(err, ret) {
                t.ifError(err);
                t.ok(Date.now() - startMs >= 100);
                t.done();
            })
        },

        'should run job in a separate process': function(t) {
            runner.run('pid', function(err, ret) {
                t.ifError(err);
                t.equal(typeof ret, 'number');
                t.ok(ret != process.pid);
                t.done();
            })
        },

        'should receive job errors': function(t) {
            runner.run('error', function(err, ret) {
                t.ok(err);
                t.equal(err.message, 'script error');
                t.done();
            })
        },
    },

    'errors': {
        'should throw if no script': function(t) {
            try { runner.run(); t.fail() }
            catch (err) { t.contains(err.message, 'script'); t.done() }
        },

        'should throw if no callback': function(t) {
            try { runner.run('ping'); t.fail() }
            catch (err) { t.contains(err.message, 'callback'); t.done() }
        },

        'should return error if script not found': function(t) {
            runner.run('notfound', function(err, ret) {
                t.ok(err);
                t.contains(err.message, 'find module');
                t.done();
            })
        },

        'should return error if job times out': function(t) {
            var timeoutRunner = runner.defaults({ timeout: 100 });
            timeoutRunner.run('sleep', { ms: 200 }, function(err, ret) {
                t.ok(err);
                t.contains(err.message, 'timeout');
                t.done();
            })
        },

        'should return error if unable to fork worker process': function(t) {
            var stub = t.stub(console, 'log');
            t.stubOnce(child_process, 'fork', function() { throw new Error("fork error " + process.pid) });
            runner.run('otherping', function(err, ret) {
                stub.restore();
                t.ok(err);
                t.equal(err.message, 'fork error ' + process.pid);
                t.done();
            })
        },

        'should return error if script throws when loading': function(t) {
            runner.run('/nonesuch', 123, function(err, ret) {
                t.ok(err);
                t.equal(err.code, 'MODULE_NOT_FOUND');
                t.done();
            })
        },
    },

    'helpers': {
        'createWorkerProcess should fork and return annotated child process': function(t) {
            var worker = {};
            var spy = t.stubOnce(child_process, 'fork', function(){ return worker });
            var worker2 = qworker._helpers.createWorkerProcess('scriptName');
            t.equal(spy.callCount, 1);
            t.equal(worker2._script, 'scriptName');
            t.equal(worker2._useCount, 0);
            t.equal(worker2, worker);
            t.done();
        },

        'killWorkerProcess should cause worker process to exit': function(t) {
            var worker = qworker._helpers.createWorkerProcess('process_to_kill');
            t.equal(worker.exitCode, null);
            var workerPid = worker.pid;
            qworker._helpers.killWorkerProcess(worker, function(err, ret) {
                t.ok(worker.exitCode == 0 || worker.killed || worker.signalCode);
                t.equal(worker.signalCode, 'SIGKILL');
                try { process.kill(workerPid); t.fail() }
                catch (err) { t.contains(err.message, 'ESRCH') }
                t.done();
            })
        },

        'endWorkerProcess should end a killed worker': function(t) {
            var worker = qworker._helpers.createWorkerProcess('sleep');
            worker._useCount = 999999;
            process.kill(worker.pid, 'SIGHUP');
            qworker._helpers.endWorkerProcess(worker, function(err, proc) {
                t.equal(worker.signalCode, 'SIGHUP');
                try { process.kill(proc.pid); t.fail() }
                catch (err) { t.contains(err.message, 'ESRCH') }
                t.done();
            })
        },

        'sendTo should return false on error': function(t) {
            var ret = qworker._helpers.sendTo({}, { qwType: 'test' });
            t.strictEqual(ret, false);
            t.done();
        },
    },
}
