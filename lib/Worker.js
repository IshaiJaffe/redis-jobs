var redis = require('redis');
var EventEmitter = require('events').EventEmitter;
var util = require('util');
var async = require('async');
var uuid = require('node-uuid');

var ip;

function createClient(port, ip, opts, password) {
    var client = redis.createClient(port, ip, opts);
    if (password)
        client.auth(password);
    return client;
}

//MessageWorker listens to the job List and emits works
//The jobs have the function reply which sends messages back to the requester
//
function MessageWorker(opts) {
    var opts = opts || {};
    var redisport = opts.port || null;
    var redishost = opts.host || null;
    var redisopts = opts.opts || null;
    var redispassword = opts.password || null;
    var self = this;
    this.queueName = opts.queueName || '';
    this.timeout = opts.timeout || 5000;
    this.workerId = uuid.v1();
    this.publisher = createClient(redisport, redishost, redisopts, redispassword);
    this.subscriber = createClient(redisport, redishost, redisopts, redispassword);
    this.parallel = opts.parallel || 1;
    this._workingOn = {};
    this._jobs = {};
}
util.inherits(MessageWorker, EventEmitter);

MessageWorker.prototype.listen = function () {
    var self = this;
    if(self.stopped)
        return;
    self.subscriber.brpoplpush(self.queueName + 'JobQueue', self.queueName + 'WorkQueue', '0',
        function (err, evt) {
            if (evt) {
                //Create a job from the event, wrap queue delete logic on job
                var job = self.createJob(evt);
                self.emit('job', job);
            }
            else
                self.listen();
        });
};

MessageWorker.prototype.createJob = function (jobString) {
    var event = null;
    var self = this;
    //I always wrap code coming off the redis bus with this type of boilerplate
    //Its not needed after I remember to stringify on bot sides, but the error condition
    //should be handled better
    try {
        event = JSON.parse(jobString);
    } catch (e) {
        console.log('parsing error... do you even stringify?');
    }
    this._workingOn[event.id] = jobString;
    var job = new Job(event);
	this._jobs[event.id] = job;
    //create the clear work function for later so we don't have to store
    //the jobString anywhere else
    job.clearWorkQueue = function () {
        self.publisher.lrem([self.queueName + "WorkQueue", "-1", jobString], function (e, r) {
            if (e)
                console.error('error clearing job from queue', e);
        });
    }
    //Create the reply closure
    job.reply = function (res,cbk) {
        var jobRes = { "reply":res,
            "id":event.id,
            "time":event.time,
            "requester":event.requester };
        var jobThis = this;
        delete self._workingOn[event.id];
		delete self._jobs[event.id];
        //Send response over the wire
        self.publisher.publish(event.requester, JSON.stringify(jobRes), function () {
            //Clear the worker Queue using the jobs this;
            jobThis.clearWorkQueue();
			if(cbk)
				cbk();
        });
        var mem = process.memoryUsage();
        if (mem.rss > (1000 * 1000 * 500)) {
            console.error('memory overload, reducing worker parallel');
            self.parallel--;
            if (!self.parallel) {
                console.error('exiting process due to low memory, bye');
                setTimeout(function () {
                    process.exit(0);
                }, 500);
            }
        }
        else
        // ready to take another job
            self.listen();
    }
    return job;
}

MessageWorker.prototype.pause = function(){
    this.stopped = true;
};

MessageWorker.prototype.close = function(cbk){
	
	var jobs = [];	
    for(var eventId in this._jobs){
		jobs.push(this._jobs[eventId]);
	}
	if(jobs.length){
		console.log('Redis-Queue killing jobs');
		var self = this;
		async.each(jobs,function(job,cbk){
			job.reply('killed',cbk);
		},function(){
			console.log('Redis-Queue closing worker');
		    self.publisher.quit();
			self.subscriber.quit();
			if(cbk)
				cbk();
		});
	}
	else {
		console.log('Redis-Queue closing worker');
	    this.publisher.quit();
		this.subscriber.quit();
		if(cbk)
			cbk();
	}
}

MessageWorker.prototype.recycleJobs = function(cbk){
}

function Job(request) {
    this.data = request.request;
}

module.exports = MessageWorker;


