var redis = require('redis');
var EventEmitter = require('events').EventEmitter;
var util = require('util');
var async = require('async');
var uuid = require('node-uuid')
    ,createClient = require('./common').createClient;

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
    createClient(redisport, redishost, redisopts, redispassword,function(err,client){
        self.publisher = client;
    });
    this.parallel = opts.parallel || 1;
    this._workingOn = {};
    this._jobs = {};
    createClient(redisport, redishost, redisopts, redispassword,function(err,client){
        self.subscriber = client;
        if(self._listening)
            self.listen();
    },true);
}
util.inherits(MessageWorker, EventEmitter);

MessageWorker.prototype.listen = function () {
    var self = this;
    self._listening = true;
    if(!self.subscriber)
        return;
    if(self.stopped)
        return;
    self.subscriber.brpoplpush(self.queueName + 'JobQueue', self.queueName + 'WorkQueue', '0',
        function (err, evt) {
            if (evt) {
                //Create a job from the event, wrap queue delete logic on job
                var job = self.createJob(evt);
                if(!job)
                    return;
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
    if(!event || !event.id)
        return console.error('Error parsing message');
    this._workingOn[event.id] = jobString;
    var job = new Job(event);
	this._jobs[event.id] = job;
    //create the clear work function for later so we don't have to store
    //the jobString anywhere else
    job.clearWorkQueue = function (cbk) {
        self.publisher.lrem([self.queueName + "WorkQueue", "-1", jobString], function (e, r) {
            if (e)
                console.error('error clearing job from queue', e);
			if(cbk)
				cbk();
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
            jobThis.clearWorkQueue(cbk);
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
    var self = this;
	if(jobs.length){
		console.log('Redis-Queue killing jobs');
		async.each(jobs,function(job,cbk){
			job.reply('killed',cbk);
		},function(){
			console.log('Redis-Queue closing worker');
		    self.publisher && self.publisher.quit();
			self.subscriber && self.subscriber.quit();
			if(cbk)
				cbk();
		});
	}
	else {
		console.log('Redis-Queue closing worker');
        self.publisher && self.publisher.quit();
        self.subscriber && self.subscriber.quit();
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


