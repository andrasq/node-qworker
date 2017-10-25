module.exports = function error( payload, callback ) {
    callback(new Error("script error"));
}
