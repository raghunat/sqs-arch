'use strict';
var log = require('winston');
var async = require('async');
var Sequelize = require('sequelize');
var sequelize;
var semver = require('semver');
var fs = require('fs');
var path = require('path');
/**
 * Service constuctor to create services from Amazon SQS
 * @param {Object} sqs aws-sdk.SQS or mocked version for testing
 */
function Service(sqs) {
  var self = this;
  self.meta = {
    processes: [],
    queueBase: null
  }; // contains meta information
  self.options = {
    pollInterval: 10
  }; // contains sqs options
  self.processes = []; // contains all the logics for processing a message from the queue
  self.registeredDone = null; // function called when done
  self.registeredError = null; // function called when error happenszz
  self.interval = null; // future setInterval for polling
  self.configLoaded = false; // Stores if the config has been loaded
  self.Record = null; // Stores the ORM Model
  // Dependency injection available if needed, but defaults to amazons sqs sdk
  var AWS = sqs || require('aws-sdk');

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
    if (semver.valid(version)) {
      self.meta.version = version;
      return self;
    } else {
      throw new Error('You use semver format for your version. Example \'1.9.4\'');
    }
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

  self.getSQLTypeLength = function (type) {
    switch (type) {
      case 'string':
        return (self.dbOptions && self.dbOptions.dialect === 'mssql') ? 'max' : 255;
      default:
        throw new Error('SQL Type UNKNOWN');
    }
  };

  /**
   * Sets up the DB ORM
   * @param {String} uri Connection string for sql db types
   */
  self.DB = function (db, user, pass, opts) {
    self.dbOptions = opts || {};
    if (db && user && pass) {
      sequelize = new Sequelize(db, user, pass, self.dbOptions);
      self.customDB = true;
    } else {
      sequelize = new Sequelize(null, null, null, {
        dialect: 'sqlite',
        storage: './sqs-arch.sqlite',
        logging: false
      });
    }
    return self;
  };

  /**
   * Load Amazon Credentials
   * @param  {String} jsonPath Path to the json declartion. If left null, uses the cwd + sqs-arch.json. This contains any AWS credentials or SQL credentials
   * @return {Void}
   */
  self.loadConfig = function (jsonPath) {
    if (jsonPath) {
      AWS.config.loadFromPath(jsonPath);
    } else {
      AWS.config.loadFromPath('./sqs-arch.json');
    }
    self.configLoaded = true;
    return self;
  };

  /**
   * Adds a process use case for incoming messages
   * @param  {String}   useCase    String Identifier for the case this logic applies
   * @param  {Object}   validation KVP's where the value are data types to match inputs against
   * @param  {Function} callback   Logic function to run when matched, gets the input, and done callbacks
   * @return {Void}
   */
  self.process = function (useCase, validation, callback) {
    // check params
    if (!useCase && typeof useCase !== 'string') {
      throw new Error('The argument you supplied for the use case is improper. It must be a string.');
    } else if (typeof validation !== 'object') {
      throw new Error('The argument you supplied for the validation is improper. It must be an object.');
    } else if (typeof callback !== 'function') {
      throw new Error('The argument you supplied for the callback is improper. It must be a function.');
    }
    // TODO check already established use cases
    self.meta.processes.push({
      useCase: useCase,
      validation: JSON.stringify(validation),
      logic: callback.toString()
    });
    self.processes.push(function (message) {
      // Turn the body into an object
      try {
        if (typeof message.Body !== 'object') {
          message.Body = JSON.parse(message.Body);
        }
      } catch (e) {
        return false;
      }
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
            case Date:
              if (new Date(message.Body[prop]) === 'Invalid Date') {
                return false;
              }
              break;
            default:
              throw new Error('Object Constructor Type not supported.');
          }
        } else {
          return false;
        }
      }
      // use this process
      callback(message.Body, function (err, output, group, count) {
        if (err) {
          self.registeredError(err);
          self.report(err, null, message, group, count);
        } else {
          self.registeredDone({
            output: output,
            message: message
          });
          self.report(null, output, message, group, count);
        }
        // remove from queue either way
        self.removeMessage(message);
      });
      return true;
    });
    return self;
  };

  /**
   * Stores service result into the DB
   * @param  {Object} err     Error if occured
   * @param  {Variant} output  Logic Results
   * @param  {Object} message SQS Object
   * @param  {String} group   Reference Value
   * @param  {Integer} count   Number of results associated
   * @return {Void}
   */
  self.report = function (err, output, message, group, count) {
    var status = 1;
    var val = null;
    if (err) {
      status = 2;
      val = JSON.stringify(err);
    } else {
      val = JSON.stringify(output);
    }

    self.Record.create({
      messageId: message.MessageId,
      originalMessageId: message.Body.originalMessageId || message.MessageId,
      reportStatusId: status,
      referenceType: message.Body.by,
      referenceValue: message.Body[message.Body.by],
      group: group,
      numberAffected: count,
      result: val
    }).then(function () {});
  };

  /**
   * Stores a local asset based on the fully qualified path name
   * @param {String} filePath Fully qualified path to file asset.
   */
  self.asset = function (filePath) {
    fs.readFile(filePath, 'utf8', function (err, data) {
      if (err) {
        log.error(err);
      } else {
        self.assets.push({
          name: path.basename(filePath),
          type: path.extname(filePath),
          data: data
        });
      }
    });
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

  /**
   * Removes a message from the sqs queue
   * @param  {Object} message SQS Message object or proxy object with .ReceiptHandle
   * @return {Object}         Service object chain
   */
  self.removeMessage = function (message) {
    sqs.deleteMessage({
      QueueUrl: self.options.QueueUrl,
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

  /**
   * Stops the polling interval for SQS
   * @return {[type]} [description]
   */
  self.stop = function () {
    if (self.interval) {
      clearInterval(self.interval);
    }
    return self;
  };

  /**
   * Pushes a message to another queue
   * @param  {String}   queue   FullQueue Name
   * @param  {Variant}   message Message body
   * @param  {Function} cb      Callback with err,data
   * @return {Void}
   */
  self.pushMessage = function (queue, message, cb) {
    cb = cb || function () {}; //callback is not necessary
    sqs.sendMessage({
      QueueUrl: queue,
      MessageBody: message
    }, function (err, data) {
      cb(err, data);
    });
  };

  /**
   * Starts the service, including all bootstrapping necessary
   * @return {Void}
   */
  self.start = function () {
    if (!self.configLoaded) {
      self.loadConfig();
    }
    if (!self.customDB) {
      self.DB();
    }
    sqs = sqs || new AWS.SQS();
    // check if items have been created
    if (!self.meta.name || !self.meta.description || !self.meta.version) {
      throw new Error('Not all required items present. Each service needs a name, a description, and a version');
    }
    // create queues if necessary
    async.waterfall([
      // Set up sqs-arch Meta DB and insert
      function (cb) {
        self.Meta = sequelize.define('sqs-arch-service', {
          name: Sequelize.STRING,
          description: Sequelize.STRING(self.getSQLTypeLength('string')),
          version: Sequelize.STRING,
          pollInterval: Sequelize.STRING,
          processes: Sequelize.STRING(self.getSQLTypeLength('string')),
          assets: Sequelize.STRING(self.getSQLTypeLength('string'))
        });
        self.Meta.sync().then(function () {
          // Create meta if doesnt exist or version is updated
          self.Meta.findOrCreate({
            where: {
              name: self.meta.name,
              description: self.meta.description,
              version: self.meta.version,
              pollInterval: self.options.pollInterval.toString(),
              processes: JSON.stringify(self.meta.processes),
              assets: JSON.stringify(self.meta.assets)
            }
          }).then(function () {
            cb();
          }).catch(function (e) {
            cb(e);
          });
        });
      },
      // Set up Service DB
      function (cb) {
        self.Record = sequelize.define(self.meta.name, {
          messageId: Sequelize.STRING,
          originalMessageId: Sequelize.STRING,
          reportStatusId: Sequelize.INTEGER,
          referenceType: Sequelize.STRING(self.getSQLTypeLength('string')),
          referenceValue: Sequelize.STRING(self.getSQLTypeLength('string')),
          group: Sequelize.STRING,
          result: Sequelize.STRING(self.getSQLTypeLength('string')),
          numberAffected: Sequelize.INTEGER
        });

        self.Record.sync().then(function () {
          cb();
        }).catch(function (e) {
          cb(e);
        });
      },
      function (cb) {
        sqs.createQueue({
          QueueName: 'sqs-arch-' + self.meta.name
        }, cb);
      }
    ], function (err, q) {
      if (err) {
        log.error(err);
      } else {
        self.options.QueueUrl = q.QueueUrl;
        var base = q.QueueUrl.split('/');
        base.pop();
        self.meta.queueBase = base.join('/') + '/';
        // start polling
        self.interval = setInterval(function () {
          // Iterate through processes for each message for first match
          sqs.receiveMessage({
            QueueUrl: self.options.QueueUrl,
            MaxNumberOfMessages: 10
          }, function (err, data) {
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
        }, self.options.pollInterval * 1000);
      }
    });
  };
  return this;
}

module.exports = Service;
