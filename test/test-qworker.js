/**
 * Copyright (C) 2017-2018 Andras Radics
 * Licensed under the Apache License, Version 2.0
 */

'use strict';

var fs = require('fs');
var child_process = require('child_process');
var util = require('util');
var events = require('events');

var qworker = require('../');
var runner = qworker({
    scriptDir: __dirname + '/scripts',
    maxUseCount: 2,
    exitTimeout: 200,
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

    'should anchor scriptDir': function(t) {
        var runner2 = qworker({ scriptDir: 'myDir' });
        t.equal(runner2.scriptDir, process.cwd() + '/myDir');
        t.done();
    },

    'should create a new job runner': function(t) {
        var runner1 = new qworker();
        var runner2 = new qworker();
        t.ok(runner1);
        t.equal(typeof runner1.run, 'function');
        t.equal(typeof runner2.run, 'function');
        t.ok(runner1 != runner2);
        t.done();
    },

    'options': {
        'should create runner with options': function(t) {
            var runner2 = qworker({ maxWorkers: 123, timeout: 100 });
            t.equal(runner2._options.maxWorkers, 123);
            t.equal(runner2._options.timeout, 100);
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
            var spy = t.stubOnce(child_process, 'fork', function(){ return new MockWorker() });
            runner.run('fakeping', function(err, ret) {
                t.ifError(err);
                t.equal(spy.callCount, 1, "expected to fork once");
                t.done();
            })
        },

        'should return from job only once': function(t) {
            var worker = new MockWorker();
            var spy = t.stubOnce(runner, 'createWorkerProcess', function(script, job, launch){ process.nextTick(launch); return worker });
            var callCount = 0;
            runner.run('fakescript', function(err, ret) {
                callCount += 1;
                t.ok(err && err.message === 'deliberate error');
                t.equal(callCount, 1);
                t.done();
            })
            setTimeout(function() {
                worker.emit('error', new Error("deliberate error"));
                worker.emit('error', new Error("other error"));
                worker.emit('error', null);
                worker.emit('exit', 0);
                worker.emit('message', { qwType: 'done' });
            }, 5);
        },

        'should wait for the done message for callback': function(t) {
            var worker = new MockWorker(100);
            var spy = t.stubOnce(child_process, 'fork', function(){ return worker });

            var messages = [];
            worker.on('message', function(m) {
                messages.push(m && m.qwType);
            })
            runner.run('some-other-job', function(err, ret) {
                t.deepEqual(messages, ['ready', 'ready', 'some message', 'some other message', 'done']);
                t.done();
            })
            setTimeout(function() {
                // the mock 'ready' happened before createWorkerProcess listened for it, re-send
                worker.emit('message', { qwType: 'ready' });
                worker.emit('message', { qwType: 'some message' });
                worker.emit('message', { qwType: 'some other message' });
                // 'done' will call the run() callback, which checks the messages seen so far
                worker.emit('message', { qwType: 'done' });
                worker.emit('message', { qwType: 'post-done messages not seen' });
            }, 15);
        },

        'should run a job': function(t) {
            runner.run('ping', { x: process.pid }, function(err, ret) {
                t.ifError(err);
                t.deepEqual(ret, { x: process.pid });
                t.done();
            });
        },

        'should run jobs back to back': function(t) {
            var pid1, pid2, pid3;
            var ncalls = 0;
            runner.run('pid', 1, function(err, ret) {
                ncalls += 1;
                t.ifError(err);
                pid1 = ret;
                runner.run('pid', 2, function(err, ret) {
                    ncalls += 1;
                    t.ifError(err);
                    pid2 = ret;
                    runner.run('ping', 3, function(err, ret) {
                        ncalls += 1;
                        t.ifError(err);
                        pid3 = ret;
                        t.ok(pid1 == pid2 || pid2 == pid3);     // check maxUesCount
                        t.equal(ncalls, 3);                     // callbacks called exactly once each
                        setTimeout(function(){ t.done() }, 20);
                    })
                })
            })
        },

        'run should invoke runWithOptions': function(t) {
            var spy = t.spyOnce(runner, 'runWithOptions');
            runner.run('ping', { x: process.pid }, function(err, ret) {
                t.ifError(err);
                t.ok(spy.called);
                t.deepEqual(spy.args[0][0], 'ping');
                t.deepEqual(spy.args[0][1], {});
                t.deepEqual(spy.args[0][2], { x: process.pid });
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

        'runWithOptions should require options and callback': function(t) {
            t.throws(function() { runner.runWithOptions() }, /missing script name/);
            t.throws(function() { runner.runWithOptions(true) }, /missing script name/);
            t.throws(function() { runner.runWithOptions('ping') }, /missing options/);
            t.throws(function() { runner.runWithOptions('ping', 1.25) }, /missing options/);
            t.throws(function() { runner.runWithOptions('ping', {}, {}) }, /callback required/);
            t.done();
        },

        'runWithOptions should use job timeout': function(t) {
            var runner2 = qworker({ scriptDir: __dirname + '/scripts' });
            var spy = t.spy(runner2, 'createWorkerProcess');
            runner2.runWithOptions('ping', { timeout: 1234 }, 'pong', function(err, ret) {
                t.ok(spy.called);
                t.equal(spy.args[0][1].timeout, 1234);
                t.done();
            })
        },

        'runWithOptions should set nice level': function(t) {
            var runner2 = qworker({ scriptDir: __dirname + '/scripts' });
            var spy = t.spy(runner2, 'createWorkerProcess');
            runner2.runWithOptions('ping', { niceLevel: 1234 }, 'pong', function(err, ret) {
                t.ok(spy.called);
                t.equal(spy.args[0][1].niceLevel, 1234);
                t.done();
            })
        },

        'runWithOptions should preload packages': function(t) {
            var runner2 = qworker({ scriptDir: __dirname + '/scripts', require: { dns: 'dns' }, maxUseCount: 1 });
            runner2.runWithOptions('globals', {}, function(err, ret1) {
                t.ifError(err);
                t.contains(ret1, 'dns');
                runner2.runWithOptions('globals', { require: { os: 'os' } }, function(err, ret2) {
                    t.ifError(err);
                    t.contains(ret2, 'os');
                    t.done();
                })
            })
        },

        'eval should eval script': function(t) {
            var runner2 = qworker({ scriptDir: __dirname + '/scripts', maxUseCount: 1 });
            var src1 = 'function(payload, cb) { cb(null, { retval: 1234 }) }';
            var src2 = 'function(payload, cb) { cb(null, { retval: 2345 }) }';
            runner2.runWithOptions('ping', { eval: src1 }, function(err, ret1) {
                t.ifError(err);
                t.equal(ret1.retval, 1234);
                runner2.runWithOptions('ping', { eval: src2 }, function(err, ret2) {
                    t.ifError(err);
                    t.equal(ret2.retval, 2345);
                    t.done();
                })
            })
        },

        'eval should return parse error': function(t) {
            var runner2 = qworker({ scriptDir: __dirname + '/scripts', maxUseCount: 1 });
            runner2.runWithOptions('ping', { eval: 'function(){}}' }, function(err, ret) {
                t.ok(err);
                t.contains(err.message, 'eval error');
                t.contains(err.message, 'Unexpected token');
                t.done();
            })
        },

        'eval should use script-named worker': function(t) {
            var runner2 = qworker({ scriptDir: __dirname + '/scripts', maxUseCount: 10 });
            var name1 = __dirname + '/scripts/pid';
            var name2 = __dirname + '/scripts/ping';
            runner2.runWithOptions('pid', {}, function(err, ret1) {
                runner2.runWithOptions('ping', {}, function(err, ret2) {
                    t.equal(runner2._workerPool[name1][0]._useCount, 1);
                    t.equal(runner2._workerPool[name2][0]._useCount, 1);
                    runner2.runWithOptions('pid', { eval: 'function(data, cb){ return cb(null, { mypid: process.pid }) }' }, function(err, ret) {
                        t.ifError(err);
                        t.equal(runner2._workerPool[name1][0]._useCount, 2);
                        t.equal(runner2._workerPool[name2][0]._useCount, 1);
                        t.done();
                    })
                })
            })
        },

        'close should terminate all worker processes': function(t) {
            var runner2 = qworker({
                scriptDir: __dirname + '/scripts',
                maxUseCount: 10,
                exitTimeout: 200,
            });

            var spy = t.spy(runner2, 'endWorkerProcess');
            runner2.run('pid', {}, function(err, pidPid) {
                runner2.run('sleep', { ms: 100 }, function(err, sleepInfo) {
                    t.ok(spy.called);
                    setTimeout(function() {
                        t.equal(spy.callCount, 5);
                        t.equal(spy.args[0][2], undefined); // pid call done, recycle process
                        t.equal(spy.args[1][2], true);      // close idle recycled pid process (_workerPool)
                        t.equal(spy.args[2][2], true);      // close running sleep process (_workers)
                        t.equal(spy.args[3][2], undefined); // pid process 'exit' event
                        t.equal(spy.args[4][2], undefined); // sleep process 'exit' event
                        t.done();
                    }, 40);
                })
                setTimeout(function() {
                    runner2.close();
                }, 80);
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

        'should clean up killed idle process': function(t) {
            var runner2 = qworker({ maxUseCount: 10, scriptDir: __dirname + '/scripts' });
            runner2.run('pid', {}, function(err, pid) {
                t.ok(pid > 0);
                t.deepEqual(Object.keys(runner2._workerPool), [ __dirname + '/scripts/pid']);
                process.kill(pid, 'SIGTERM');
                setTimeout(function() {
                    t.ok(runner2.processNotExists(pid));
                    t.equal(runner2._workers.length, 0);
                    t.equal(Object.keys(runner2._workerPool).length, 0);
                    t.done();
                }, 100);
            })
        },

        'disconnected worker should exit once job finishes': function(t) {
            var runner2 = qworker({ idleTimeout: 10000, scriptDir: __dirname + '/scripts' });
            var worker, pid;

            t.expect(4);

            // create the worker process, start a job that will take 1500 ms
            // when run by `nyc` code coverage, it takes 500 ms to create a process
            runner2.run('sleep', { ms: 1500 }, function(err, ret) {
                t.ok(err && /worker died/.test(err.message));
                t.ok(runner2.processNotExists(pid));
            })

            // disconnect 1000 ms later, giving the script plenty of time to start
            setTimeout(function() {
                worker = runner2._workers[0];
                pid = worker.pid;
                worker.disconnect();
                var disconnectTime = Date.now();
                worker.on('exit', function() {
                    // it takes a while for the process to exit after disconnect under `nyc`,
                    // but should be well under the idle timeout
                    t.ok(Date.now() < disconnectTime + 5000);
                    t.done();
                })
            }, 1000)

            // check that the job continued to run after the disconnect
            setTimeout(function() {
                // disconnected job not finished yet, but still running
                var worker = runner2._workers[0];
                t.ok(!runner2.processNotExists(pid));
            }, 1450);
        },
    },

    'helpers': {
        'createWorkerProcess should fork and return annotated child process': function(t) {
            var worker = new MockWorker();
            var spy = t.stubOnce(child_process, 'fork', function(){ return worker });
            var worker2 = runner.createWorkerProcess('scriptName', {}, noop);
            t.equal(spy.callCount, 1);
            t.equal(worker2._script, 'scriptName');
            t.equal(worker2._useCount, 0);
            t.equal(worker2, worker);
            t.done();
        },

        'createWorkerProcess should reuse a recycled process': function(t) {
            var worker = new MockWorker();
            var spy = t.stub(child_process, 'fork', function(){ return worker });
            var worker1 = runner.createWorkerProcess('scriptName', {}, noop);
            var stub = t.stub(runner, 'processNotExists', function(worker){ return false });
            runner.endWorkerProcess(worker1, function(err, endedWorker) {
                spy.restore();
                t.equal(runner._workerPool['scriptName'].length, 1);
                var worker2 = runner.createWorkerProcess('scriptName', {}, noop);
                stub.restore();
                t.equal(worker2, worker1);
                t.equal(spy.callCount, 1);
                t.equal(endedWorker, worker1);
                t.done();
            })
        },

        'createWorkerProcess should create a worker at the configured priority': function(t) {
            var runner2 = qworker({ niceLevel: 12, scriptDir: __dirname + '/scripts' });
            var procs;
            var worker = runner2.run('ps-self', function(err, ret) {
                t.ifError(err);
                var regex = new RegExp("^\\s*" + ret.pid + "\\s* 12$", "m");
                t.ok(regex.test(ret.stdout));
                t.done();
            })
        },

        'createWorkerProcess should return renice error': function(t) {
            var runner2 = qworker({ niceLevel: 'NaN', scriptDir: __dirname + '/scripts' });
            var spy = t.stub(process.stdout, 'write').configure('saveLimit', 10);
            var worker = runner2.run('sleep', { ms: 10 }, function(err, ret) {
                spy.restore();
                var output = concatOutputLines(spy.args, 0);
                t.ifError(err);
                t.ok(spy.called);
                t.contains(output, 'failed to renice process ' + ret.pid);
                t.done();
            })
        },

        'killWorkerProcess should cause worker process to exit': function(t) {
            var worker = runner.createWorkerProcess('process_to_kill', {}, noop);
            t.equal(worker.exitCode, null);
            var workerPid = worker.pid;
            runner.killWorkerProcess(worker, function(err, ret) {
                t.ok(worker.exitCode == 0 || worker.killed || worker.signalCode);
                t.equal(worker.signalCode, 'SIGKILL');
                try { process.kill(workerPid); t.fail() }
                catch (err) { t.contains(err.message, 'ESRCH') }
                t.done();
            })
        },

        'endWorkerProcess should tell the worker to stop': function(t) {
            var worker = runner.createWorkerProcess('sleep', {}, function(err) {
                worker._useCount = 999999;
                runner.endWorkerProcess(worker, function(err, proc) {
                    // the worker process either exited voluntarily or was killed.
                    // Since the worker normally waits forever for more jobs to run,
                    // it has to either be killed or be told to 'stop'.
                    t.ok(runner.processNotExists(worker.pid));
                    t.strictEqual(worker.exitCode, 0);
                    t.strictEqual(worker.killed, false);
                    t.done();
                })
            })
        },

        'endWorkerProcess should end a killed worker': function(t) {
            var worker = runner.createWorkerProcess('sleep', {}, noop);
            worker._useCount = 999999;
            process.kill(worker.pid, 'SIGHUP');
            runner.endWorkerProcess(worker, function(err, proc) {
                t.strictEqual(worker._kstopped, true);
                t.equal(worker.signalCode, 'SIGHUP');
                try { process.kill(proc.pid); t.fail() }
                catch (err) { t.contains(err.message, 'ESRCH') }
                t.done();
            })
        },

        'endWorkerProcess should not end the process twice': function(t) {
            var worker = new MockWorker();
            worker._useCount = 123;
            worker._kstopped = 'yes';
            runner.endWorkerProcess(worker, function(err, stoppedWorker) {
                t.strictEqual(stoppedWorker, worker);
                t.strictEqual(stoppedWorker._useCount, 123);
                t.strictEqual(stoppedWorker._kstopped, 'yes');
                t.done();
            })
        },

        'endWorkerProcess should retire a process that reached maxUseCount': function(t) {
            runner.run('pid', {}, function(err, pid1) {
                t.ifError(err);
                t.ok(pid1 > 0);

            runner.run('pid', function(err, pid2) {
                t.ifError(err);
                t.ok(pid2 > 0);

            runner.run('pid', function(err, pid3) {
                t.ifError(err);
                t.ok(pid3 > 0);
                t.ok(pid3 !== pid1);

            runner.run('pid', function(err, pid4) {
                t.ifError(err);
                t.ok(pid4 > 0);
                t.ok(pid4 != pid2);

            runner.run('pid', function(err, pid5) {
                t.ifError(err);
                t.ok(pid5 > 0);
                t.ok(pid5 != pid3);

                t.done();

            }) }) }) }) })
        },

        'endWorkerProcess should ignore processes that do not exist': function(t) {
            var worker = new MockWorker();
            worker.pid = 'nonesuch';  // invalid id that can not be signaled
            worker._useCount = 999999;
            runner.endWorkerProcess(worker, function(err, stoppedWorker) {
                t.ok(!err);
                t.equal(stoppedWorker, worker);
                t.done();
            })
        },

        'endWorkerProcess should kill the process if it takes too long to exit': function(t) {
            var blockingScript = __dirname + '/scripts/block';
            var worker = runner.createWorkerProcess(blockingScript, {}, noop);
            // make available this worker process for the test by recycling it
            runner.endWorkerProcess(worker, function(err) {
                t.ifError(err);
                var startTime = Date.now();
                var spy = t.spyOnce(runner, 'killWorkerProcess');
                runner.run(blockingScript, { ms: 5000 }, function(err, worker2) {
                    var doneTime = Date.now();
                    t.ok(err);
                    t.contains(err.message, 'worker died');
                    t.ok(doneTime - startTime < 1000);
                    t.done();
                })
                setTimeout(function() {
                    runner.endWorkerProcess(worker, function(err, worker3) {
                        t.ok(worker.killed);
                        t.equal(worker.signalCode, 'SIGKILL');
                    })
                }, 100)
            })
        },

        'idleTimeout should end the worker process': function(t) {
            var runner2 = qworker({ idleTimeout: 2, maxUseCount: 10, scriptDir: __dirname + '/scripts' });
            runner2.run('pid', {}, function(err, ret) {
                t.ok(ret > 0);
                t.ok(ret !== process.pid);
                t.ok(!runner2.processNotExists(ret));
                setTimeout(function() {
                    t.ok(runner2.processNotExists(ret));
                    t.done();
                }, 50);
            })
        },

        'endWorkerProcess should forceQuit': function(t) {
            var runner2 = qworker({ maxUseCount: 10, scriptDir: __dirname + '/scripts' });
            var worker = runner2.createWorkerProcess('pid', {}, noop);
            runner2.endWorkerProcess(worker, function(err) {
                // without forceQuit recycles the worker
                t.equal(runner2._workerPool['pid'].length, 1);
                runner2.endWorkerProcess(worker, function(err) {
                    // with forceQuit terminates the worker
                    t.equal(runner2._workerPool['pid'], undefined);
                    t.done();
                }, true);
            });
        },

        'workerPool MvCache push should set a list property': function(t) {
            var runner2 = qworker();
            var pool = runner2._workerPool;

            t.strictEqual(pool.a, undefined);
            runner2.mvPush(pool, 'a', 'x');
            t.ok(Array.isArray(pool.a));
            t.deepEqual(pool.a, ['x']);
            runner2.mvPush(pool, 'a', 'y');
            t.deepEqual(pool.a, ['x', 'y']);

            runner2.mvPush(pool, 'b', 'z');
            t.deepEqual(pool.a, ['x', 'y']);
            t.deepEqual(pool.b, ['z']);

            t.done();
        },

        'workerPool MvCache shift should return the next item': function(t) {
            var runner2 = qworker();
            var pool = runner2._workerPool;

            t.strictEqual(runner2.mvRemove(pool, 'a', 0), undefined);

            runner2.mvPush(pool, 'a', 'x');
            runner2.mvRemove(pool, 'b', 0);
            t.strictEqual(runner2.mvRemove(pool, 'a', 0), 'x');
            t.strictEqual(runner2.mvRemove(pool, 'a', 0), undefined);
            t.strictEqual(pool.a, undefined);

            runner2.mvPush(pool, 'a', 'y');
            runner2.mvPush(pool, 'a', 'z');
            t.strictEqual(runner2.mvRemove(pool, 'a', 0), 'y');
            t.strictEqual(runner2.mvRemove(pool, 'a', 0), 'z');
            t.strictEqual(runner2.mvRemove(pool, 'a', 0), undefined);
            t.strictEqual(pool.a, undefined);

            t.done();
        },

        'workerPool MvCache delete should remove the item': function(t) {
            var runner2 = qworker();
            var pool = runner2._workerPool;

            runner2.mvDelete(pool, 'a', 'x');
            t.strictEqual(pool.a, undefined);

            runner2.mvPush(pool, 'a', 'x');
            runner2.mvDelete(pool, 'a', 'y');
            t.deepEqual(pool.a, ['x']);
            runner2.mvDelete(pool, 'b', 'x');
            t.deepEqual(pool.a, ['x']);
            runner2.mvDelete(pool, 'a', 'x');
            t.strictEqual(pool.a, undefined);

            runner2.mvPush(pool, 'a', 'x');
            runner2.mvPush(pool, 'a', 'y');
            runner2.mvPush(pool, 'a', 'z');
            runner2.mvDelete(pool, 'a', 'y');
            t.deepEqual(pool.a, ['x', 'z']);
            runner2.mvDelete(pool, 'a', 'z');
            t.deepEqual(pool.a, ['x']);
            runner2.mvDelete(pool, 'a', 'x');
            t.strictEqual(pool.a, undefined);

            t.done();
        },

        'workerPool MvCache getKeys should return currently stored keys': function(t) {
            var runner2 = qworker();
            var pool = runner2._workerPool;

            t.deepEqual(Object.keys(pool), []);

            runner2.mvPush(pool, 'a', 'x');
            t.deepEqual(Object.keys(pool), ['a']);
            runner2.mvPush(pool, 'b', 'y');
            t.deepEqual(Object.keys(pool), ['a', 'b']);
            runner2.mvDelete(pool, 'a', 'x');
            t.deepEqual(Object.keys(pool), ['b']);

            t.done();
        },
    },

    'locking': {
        setUp: function(done) {
            fs.unlink('./sleep.pid', function() {
            fs.unlink('./lock.pid', function() {
            done();
            }) })
        },

        'should set mutex while script is running': function(t) {
            var pid, lockfileName = './sleep.pid';
            runner.runWithOptions('sleep', { lockfile: lockfileName }, { ms: 100 }, function(err, info) {
                t.ok(!err);
                t.equal(+pid, +info.pid);
                t.throws(function(){ fs.readFileSync(lockfileName) }, /ENOENT/);
                t.done();
            })
            setTimeout(function() {
                pid = fs.readFileSync(lockfileName);
            }, 80);
        },

        'should return error if mutex is set': function(t) {
            var lockfileName = './sleep.pid';
            fs.writeFileSync(lockfileName, process.pid);
            runner.runWithOptions('sleep', { lockfile: lockfileName }, { ms: 100 }, function(err, info) {
                t.ok(err);
                fs.unlinkSync(lockfileName);
                t.contains(err.message, 'cannot break');
                t.done();
            })
        },

        'should return lockfile write error': function(t) {
            var lockfileName = '/none/such';
            runner.runWithOptions('sleep', { lockfile: lockfileName }, { ms: 100 }, function(err, info) {
                t.ok(err);
                t.done();
            })
        },

        'should break abandoned lock': function(t) {
            var child = child_process.exec("sleep 1");
            var unusedPid = child.pid;
            process.kill(unusedPid, 'SIGKILL');
            var lockfileName = './sleep.pid';
            fs.writeFileSync(lockfileName, unusedPid);
            runner.runWithOptions('sleep', { lockfile: lockfileName }, { ms: 10 }, function(err, info) {
                t.ok(!err);
                t.equal(info.ms, 10);
                t.done();
            })
        },

        'clearLock should not break a held mutex': function(t) {
            fs.writeFileSync('./lock.pid', process.pid);
            t.throws(function(){ runner.clearLock('./lock.pid', process.pid + 1) }, /not our lock/);
            fs.writeFileSync('./lock.pid', '1');
            t.throws(function(){ runner.clearLock('./lock.pid', '2') }, /not our lock/);
            t.done();
        },

        'clearLock should break abandoned lock': function(t) {
            fs.writeFileSync('./lock.pid', '999999999');
            runner.clearLock('./lock.pid', '1');
            t.throws(function(){ fs.readFileSync('./lock.pid') }, /ENOENT/);
            t.done();
        },

        'clearLock should ignore an already cleared mutex': function(t) {
            runner.clearLock('./lock.pid', process.pid);
            t.done();
        },

        'clearLock should tolerate a forcibly broken lockfile': function(t) {
            var spy = t.spy(process.stdout, 'write');
            runner.runWithOptions('sleep', { lockfile: './sleep.pid' }, { ms: 100 }, function(err, info) {
                fs.unlinkSync('./sleep.pid');
                t.ok(!err);
                t.ok(spy.called);
                var output = concatOutputLines(spy.args, 0);
                t.contains(output, 'clearLock error');
                t.done();
            })
            setTimeout(function() {
                // overwrite the mutex, force a 'not our lock' clearLock error
                fs.writeFileSync('./sleep.pid', '1');
            }, 80);
        },
    },
}

function MockWorker( whenDone ) {
    events.EventEmitter.call(this);

    this._qwId = -1;
    this._useCount = 0;
    this._script = 'no script';

    // send a fake 'done' event in a few
    var self = this;
    process.nextTick(function(){
        self.emit('message', { pid: process.pid, qwType: 'ready' });
    });
    setTimeout(function() {
        self.emit('message', { pid: process.pid, qwType: 'done' });
    }, whenDone || 10);
}
util.inherits(MockWorker, events.EventEmitter);

function noop() {}

function concatOutputLines( spyArgs, index ) {
    var output = '';
    for (var i=0; i<spyArgs.length; i++) output += spyArgs[i][index];
    return output;
}

function wrapPool( runner, pool ) {
    return {
        store: pool,
        push: function(k, v) { runner.mvPush(this.store, k, v) },
        shift: function(k) { runner.mvRemove(this.store, k, 0) },
        delete: function(k, v) { runner.mvDelete(this.store, k, v) },
    }
}
