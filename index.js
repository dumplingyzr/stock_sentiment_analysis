// read from redis channel
// update ui in real time

var argv = require('minimist')(process.argv.slice(2)); 
var port = argv['port'];
var redis_host = argv['redis_host'];
var redis_port = argv['redis_port'];

// - setup dependency instances
var express = require('express');
var app = express();
var server = require('http').createServer(app);
var io = require('socket.io')(server);

// - setup webapp routing
app.use(express.static(__dirname + '/public'));
app.use('/jquery', express.static(__dirname + '/node_modules/jquery/dist/'));
app.use('/d3', express.static(__dirname + '/node_modules/d3/'));
app.use('/nvd3', express.static(__dirname + '/node_modules/nvd3/build/'));
app.use('/bootstrap', express.static(__dirname + '/node_modules/bootstrap/dist'));

// - setup redis client
console.log('Creating a redis client');
var redisclient = require('redis').createClient(redis_port, redis_host);

console.log('Subscribing to redis topic tweet');
redisclient.subscribe('tweet');

// - call back function
redisclient.on('message', function (channel, message) {
    console.log("received tweet\n")
    io.sockets.emit(channel, message);
});

server.listen(port, function () {
    console.log('Server started at port %d.', port);
});

// - setup shutdown hooks
var shutdown_hook = function () {
    console.log('Quitting redis client');
    redisclient.quit();
    console.log('Shutting down app');
    process.exit();
};

process.on('SIGTERM', shutdown_hook);
process.on('SIGINT', shutdown_hook);
process.on('exit', shutdown_hook);