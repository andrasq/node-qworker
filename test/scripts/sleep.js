module.exports = function sleep( payload, callback ) {
    setTimeout(callback, payload.ms, null, { ms: payload.ms, pid: process.pid });
}
