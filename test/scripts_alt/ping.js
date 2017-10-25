module.exports = function ping( payload, callback ) {
    if (typeof payload == 'object') payload.alt = true;
    callback(null, payload);
}
