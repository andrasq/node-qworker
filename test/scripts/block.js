module.exports = function block( payload, callback ) {
    // function that blocks the event loop for the given ms
    var until = Date.now() + payload.ms;
    while (Date.now() < until) ;
    callback(payload.ms);
}
