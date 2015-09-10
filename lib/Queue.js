var redis = require('redis');
var uuid = require('node-uuid');
var util = require('util');
var EventEmitter = require('events').EventEmitter,
    async = require('async')
    ,createClient = require('./common').createClient;




//MessageQueue manages the jobs on the job queue, and their timeouts
//it can also give feedback on the performance of the worker cluster
function MessageQueue(opts) {
    var opts = opts || {};
    var redisport = opts.port || null;
    var redishost = opts.host || null;
    var redisopts = opts.opts || null;
    var password = opts.password || null;
    var self = this;
    this.queueName = opts.queueName || '';
    this.requestTimers = {};
    this.jobs = {};
    this.timeout = opts.timeout || 5000;
    this.requesterId = uuid.v1();
    createClient(redisport, redishost, redisopts, password,function(err,client){
        self.publisher = client;
    });
    createClient(redisport, redishost, redisopts, password,function(err,client){
        self.subscriber = client;
        //Replies come over a redis PUB/SUB
        client.on('message', function (channel, message) {
            var res = null;
            try {
                res = JSON.parse(message);
            } catch (e) {
                console.log('parsing error, do you even stringify?');
            }
            if (self.requestTimers[res.id]) {
                clearTimeout(self.requestTimers[res.requestId]);
                delete self.requestTimers[res.requestId];
            }
            //remove redis request/reply/event wrapper and emit
            if (self.jobs[res.id]) {
                self.jobs[res.id].reply(res.reply);
                delete self.jobs[res.id];
            } else {
                console.log('unknown job id');
            }

        });
        //Replies come back over the requesterId channel of the instance of mq
        //This is a less reliable interface than BRPOPLPUSH but its just a message and shouldn't blow up anything.
        client.subscribe(self.requesterId);

    });
}
//Creates a job and returns a promise like Object for listening to events from
//message bus and for starting the job
MessageQueue.prototype.createJob = function (jobData,timeout) {

    // check if job is already in queue
    var jobDataStr = JSON.stringify(jobData);
    for(var key in this.jobs){
        var otherJob = this.jobs[key];
        if(jobDataStr == otherJob.jobDataStr)
            return otherJob;
    }

    var id = uuid.v1();
    var time = new Date().getTime();
    var req = {id:id, request:jobData,
        time:time, requester:this.requesterId};
    var job = new Job(req);
    job.jobDataStr = jobDataStr;
    var self = this;
    job.start = function (isPriority) {
        if (job._started)
            return;
        job._started = true;
        var stringRequest = JSON.stringify(this.req);
        self.requestTimers[id] = setTimeout(function () {
            job.timeout();
            self.publisher.lrem([self.queueName + "WorkQueue", "-1", stringRequest], function (e, r) {
                if (e)
                    console.error('error removing timedout request from worker queue', e);
            });
        }, timeout || self.timeout);        
        if(isPriority)
            self.publisher.lpush(self.queueName + 'JobQueue', stringRequest, function () {});
        else
            self.publisher.rpush(self.queueName + 'JobQueue', stringRequest, function () {});
    }
    this.jobs[id] = job;
    return job;
}

MessageQueue.prototype.getWaitingCount = function (cbk) {
    this.publisher.llen(this.queueName + 'JobQueue', cbk);
}

MessageQueue.prototype.getWorkingCount = function (cbk) {
    this.publisher.llen(this.queueName + 'WorkQueue', cbk);
}

/**
 * Check for items in worker queue
 * @param cbk
 */
MessageQueue.prototype.checkWorkingJobs = function () {
    var self = this;
    this.publisher.lrange(this.queueName + 'WorkQueue', 0, -1, function (err, elements) {
        if (err) {
            console.error('error getting elements from worker queue', err);
            return;
        }
        var keepFrom = elements.length;
        for (var i = 0; i < elements.length; i++) {
            var json;
            try {
                json = JSON.parse(elements[i]);
            } catch (e) {
            }
            if (json && (Date.now() - json.time < self.timeout)) {
                keepFrom = i;
                break;
            }
        }
        if (keepFrom > 0) {
            self.publisher.ltrim([self.queueName + 'WorkQueue', keepFrom, -1], function (err) {
                if (err)
                    console.error('error trimming work queue', err);
            })
        }
    });
}

MessageQueue.prototype.pollWorkingJobs = function (interval) {
    setInterval(this.checkWorkingJobs.bind(this), interval);
};

/**
 * Remove from queue all jobs the are qualified by func
 * @param func
 * Qualifier
 * @param cbk
 */
MessageQueue.prototype.cleanJobs = function (func, cbk) {
    var self = this;
    async.parallel([
        function(cbk){
            self.clearJobsFromQueue(self.queueName + 'WorkQueue',func,cbk);
        },
        function(cbk){
            self.clearJobsFromQueue(self.queueName + 'JobQueue',func,cbk);
        }
    ],cbk);

    var toDel = [];
    for (var id in this.jobs) {
        if (this.jobs[id].req && func(this.jobs[id].req.request)) {
            this.jobs[id].destroy();
            toDel.push(id);
        }
    }
    toDel.forEach(function (id) {
        delete self.jobs[id];
    });
}

MessageQueue.prototype.clearJobsFromQueue = function(queue,qualifier,cbk){
    var self = this;
    this.publisher.lrange(queue, 0, -1, function (err, elements) {
        if (err)
            return cbk(err)

        var toRemove = elements.filter(function (elm) {
            try {
                var json = JSON.parse(elm);
                return json && json.request && qualifier(json.request);
            } catch (e) {
                return false;
            }
        });
        async.each(toRemove, function (job, cbk) {
            self.publisher.lrem([queue, 1, job ], cbk);
        }, cbk);
    });
}

/**
 * Go over all jobs and check whether one is qualified
 * @param func
 * @return {Boolean}
 */
MessageQueue.prototype.hasJob = function(func){
    for(var id in this.jobs){
        if(func(this.jobs[id]))
            return true;
    }
    return false;
}

MessageQueue.prototype.close = function () {
    this.publisher.quit();
    this.subscriber.quit();
}

function Job(request) {
    this.req = request;
}
util.inherits(Job, EventEmitter);

Job.prototype.timeout = function () {
    this.emit('timeout');
    //Clear any other listeners to avoid a mem leak.
    this.removeAllListeners();
}

Job.prototype.reply = function (msg) {
    this.emit('reply', msg);
    //Clear any other listeners to avoid a mem leak.
    this.removeAllListeners();
}
Job.prototype.destroy = function () {
    this.emit('destroy');
    this.removeAllListeners();
}
module.exports = MessageQueue;


