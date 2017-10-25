module.exports = function sleep( payload, callback ) {
    setTimeout(callback, payload.ms);
}
