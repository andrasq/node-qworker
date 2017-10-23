/**
 * Copyright (C) 2017 Andras Radics
 * Licensed under the Apache License, Version 2.0
 **/

'use strict';

var qworker = require('../');

module.exports = {
    'should parse package.json': function(t) {
        require("../package.json");
        t.done();
    },
}
