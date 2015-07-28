'use strict';
var AWS = require('aws-sdk');
var log = require('winston');
var async = require('async');
/**
 * Service constuctor to create services from Amazon SQS
 * @param {Object} sqs aws-sdk.SQS or mocked version for testing
 */
function Service(sqs) {
  var self = this;
  self.meta = {}; // contains meta information
  self.options = {}; // contains sqs options
  self.processes = []; // contains all the logics for processing a message from the queue
  self.registeredDone = null; // function called when done
  self.registeredError = null; // function called when error happens
  self.interval = null; // future setInterval for polling

  // Dependency injection available if needed, but defaults to amazons sqs sdk
  sqs = sqs || new AWS.SQS();


  ///////////////////
  // Chain methods //
  ///////////////////

  /**
   * Sets the name and queue name of the service
   * @param  {String} name Service name/queue name
   * @return {Object}      Service Object Chain
   */
  self.name = function (name) {
    self.meta.name = name;
    return self;
  };

  /**
   * Sets the description and queue description of the service
   * @param  {String} description Service description/queue description
   * @return {Object}      Service object chain
   */
  self.description = function (description) {
    self.meta.description = description;
    return self;
  };

  /**
   * Sets the version of the service
   * @param  {String} version version number in x.x.x
   * @return {Object}         Service object chain
   */
  self.version = function (version) {
    self.meta.version = version;
    return self;
  };

  /**
   * Sets the polling interval for the service
   * @param  {Int} numberOfSeconds Number of seconds
   * @return {Object}                 Service object chain
   */
  self.pollInterval = function (numberOfSeconds) {
    self.options.pollInterval = numberOfSeconds;
    return self;
  };

  /**
   * Load Amazon Credentials
   * @param  {String} jsonPath Path to the json declartion. If left null, uses the cwd + sqs-arch.json. This contains any AWS credentials or SQL credentials
   * @return {Void}
   */
  self.loadConfig = function (jsonPath) {
    if (jsonPath) {
      sqs.config.loadFromPath(jsonPath);
    } else {
      sqs.config.loadFromPath('./sqs-arch.json');
    }
    return self;
  };

  self.process = function (useCase, validation, callback) {
    // TODO check params
    // TODO check already established use cases
    self.processes.push(function (message) {
      // Turn the body into an object
      try {
        message.Body = JSON.parse(message.Body);
        for (var prop in validation) {
          if (validation.hasOwnProperty(prop) && message.Body.hasOwnProperty(prop)) {
            switch (validation[prop]) {
              case String:
                if (typeof message.Body[prop] !== 'string') {
                  return false;
                }
                break;
              case Array:
                if (message.Body[prop].constructor !== Array) {
                  return false;
                }
                break;
              case Object:
                if (message.Body[prop].constructor !== Object) {
                  return false;
                }
                break;
              case Number:
                if (message.Body[prop].constructor !== Number) {
                  return false;
                }
                break;
              case Boolean:
                if (message.Body[prop].constructor !== Boolean) {
                  return false;
                }
                break;
              default:
                throw 'Object Constructor Type not supported.';
            }
            // if (message.Body[prop] && message.Body[prop].constuctor !== validation[prop]) {
            //   return false;
            // }
          } else {
            return false;
          }
        }
        // use this process
        callback(message.Body, function (err, output) {
          if (err) {
            self.registeredError(err);
            self.report(err);
          } else {
            self.registeredDone(output);
            self.report(null, output);
          }
          // remove from queue either way
          self.removeMessage(message);
        });
        return true;
      } catch (e) {
        return false; // we only permit json strings in the body
      }
    });
  };

  self.report = function (err, output) {
    // TODO
    if (err) {
      console.log(err);
    } else {
      console.log(output);
    }
  };

  /**
   * Completion function when a message is finished processing (without error).
   * @param  {Function} callback Callback function called with the returned value from the process function
   * @return {Object}          Service object chain
   */
  self.done = function (callback) {
    self.registeredDone = callback;
    return self;
  };

  /**
   * Completion function when a message is finished processing with an error, or an error is unhandled.
   * @param  {Function} callback Callback function called with the returened error value
   * @return {Object}            Service object chain
   */
  self.error = function (callback) {
    self.registeredError = callback;
    return self;
  };

  self.removeMessage = function (message) {
    sqs.deleteMessage({
      QueueUrl: self.options.queue,
      ReceiptHandle: message.ReceiptHandle
    }, function (err, data) {
      if (err) {
        log.error(err, data);
      } else {
        log.info('Removed', data);
      }
    });
  };

  /**
   * Exposes the winston logger
   * @param  {Function} callback Function given the winston instance to modify
   * @return {Object}            Service chain object
   */
  self.winston = function (callback) {
    callback(log);
    return self;
  };

  self.stop = function () {
    if (self.interval) {
      clearInterval(self.interval);
    }
  };

  /**
   * Starts the service, including all bootstrapping necessary
   * @return {Void}
   */
  self.start = function () {
    // TODO check if items have been created
    // create queues if necessary
    async.waterfall([
      function (cb) {
        sqs.createQueue({
          QueueName: 'sqs-arch-' + self.name
        }, cb);
      },
      function (q1, cb) {
        self.options.QueueUrl = q1;
        sqs.createQueue({
          QueueName: 'sqs-arch-' + self.name + '-report'
        }, cb);
      }
    ], function (err, q2) {
      if (err) {
        log.error(err);
      } else {
        self.options.ReportQueueUrl = q2;
        // start polling
        self.interval = setInterval(function () {
          // Iterate through processes for each message for first match
          sqs.receiveMessage({
            QueueUrl: self.QueueUrl,
            MaxNumberOfMessages: 10
          }, function(err, data) {
            if (err) {
              log.error(err);
            } else {
              if (data.Messages) {
                for (var j = 0; j < data.Messages.length; j++) {
                  var message = data.Messages[j];
                  for (var i = 0; i < self.processes.length; i++) {
                    if (self.processes[i](message)) {
                      break;
                    }
                  }
                }
              }
            }
          });
        }, self.options.pollInterval);
      }
    });
  };
  return this;
}

module.exports = Service;
