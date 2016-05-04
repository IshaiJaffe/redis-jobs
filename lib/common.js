

var redis = require('redis');


var createClient = exports.createClient =  function(port, ip, opts, password,callback,sensitive) {
    var client = redis.createClient(port, ip, opts);
    if (password)
        client.auth(password);

    var pingInterval = setInterval(function() { client.ping(); },1000*60*30);
    client.on('end',function(){
        clearInterval(pingInterval);
    });

    client.on('ready',function(){
        callback(null,client);
        console.log('redis client ready');
    });
    // call this if you wish to initiate reconnect procedure
    client.fail = function(){
        client.emit('error',new Error('Initiated reconnect'));
    };
    client.on('error',function(e){
        console.error('Error with redis client', e.stack);
        if(sensitive)
            process.exit(1);
        if(client._reconnectTO)
            return;
        console.log('will try again in 5 seconds');
        client.quit();
        client._reconnectTO = setTimeout(function(){
            client._reconnectTO = null;
            console.log('reconnecting');
            createClient(port,ip,opts,password,callback)
        },5000);
    });
};