var child_process = require('child_process');

module.exports = function psSelf( payload, callback ) {
    // procps-ng 3.3.9 command to retrieve the pid and nice level for a process
    child_process.exec("ps -o '%p %n' -p " + process.pid, function(err, stdout, stderr) {
        callback(err, { pid: process.pid, stdout: stdout, stderr: stderr });
    })
}
