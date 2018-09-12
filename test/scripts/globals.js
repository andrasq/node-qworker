module.exports = function globals( payload, callback ) {
    callback(null, Object.keys(global));
}
