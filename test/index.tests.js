'use strict';
/* exported should */
var should = require('should');
var Service = require('../index.js');
var fs = require('fs');
var testService;

describe('sqs-arch', function () {
  before(function () {
    fs.writeFileSync('./sqs-arch.json', '{}');
  });
  after(function () {
    if (fs.existsSync('./sqs-arch.json')) {
      fs.unlinkSync('./sqs-arch.json');
    }
    if (fs.existsSync('./sqs-arch.sqlite')) {
      fs.unlinkSync('./sqs-arch.sqlite');
    }
  });
  beforeEach(function () {
    var data = {
      Messages: [{
        Body: '{"Name": "Stephen"}'
      }]
    };
    testService = new Service({
      deleteMessage: function () {

      },
      createQueue: function (params, cb) {
        cb(null, {QueueUrl: params.QueueName});
      },
      receiveMessage: function (params, cb) {
        cb(null, data);
      },
      config: {
        loadFromPath: function () {
          //do nothing
        }
      }
    });
  });

  it('should expose a constuctor', function (done) {
    Service.should.be.a.Function; //jshint ignore:line
    testService.should.be.an.Object; //jshint ignore:line
    done();
  });

  it('#name', function (done) {
    testService
      .name('test-name')
      .meta.name.should.equal('test-name');
    done();
  });

  it('#description', function (done) {
    testService
      .description('some description about what I\'m doing')
      .meta.description.should.equal('some description about what I\'m doing');
    done();
  });

  it('#version', function (done) {
    testService
      .version('0.1.2')
      .meta.version.should.equal('0.1.2');
    done();
  });

  it('#pollInterval', function (done) {
    testService
      .pollInterval(3)
      .options.pollInterval.should.equal(3);
    done();
  });

  it('#loadConfig', function (done) {
    testService.loadConfig().should.be.an.object; //jshint ignore:line
    testService.loadConfig('./sqs-arch.json').should.be.an.object; //jshint ignore:line
    done();
  });

  it('#process', function (done) {
    testService.process('case1', {
      name: String
    }, function () {});
    testService.processes.length.should.equal(1);
    done();
  });

  it('#report'
    /*, function (done) {
        testService.report.should.be.a.Function; //jshint ignore:line

        done();
      }*/
  );

  it('#asset'
    /*, function (done) {
        testService.report.should.be.a.Function; //jshint ignore:line

        done();
      }*/
  );

  it('#done', function (done) {
    testService.done(function () {})
      .registeredDone.should.be.a.Function; //jshint ignore:line
    done();
  });

  it('#error', function (done) {
    testService.error(function () {})
      .registeredError.should.be.a.Function; //jshint ignore:line
    done();
  });

  it('#removeMessage', function (done) {
    testService.removeMessage.should.be.a.Function; //jshint ignore:line
    (function () {
      testService.removeMessage({
        ReceiptHandle: 1
      });
    }).should.not.throw();
    done();
  });

  it('#winston', function (done) {
    testService.winston(function (log) {
      log.info.should.exist; //jshint ignore:line
      log.error.should.exist; //jshint ignore:line
      done();
    });
  });

  it('#getSQLTypeLength', function (done) {
    testService.getSQLTypeLength('string').should.equal(255);
    testService.dbOptions = {dialect:'mssql'};
    testService.getSQLTypeLength('string').should.equal('max');
    (function () {
      testService.getSQLTypeLength();
    }).should.throw();
    done();
  });

  it('#start', function (done) {
    this.timeout(3000);
    this.slow(3000);
    testService
      .name('test')
      .description('a')
      .version('0.0.1')
      .pollInterval(0.25)
      .process('case1', {
        Name: String
      }, function (input, done) {
        done(null, input);
      })
      .done(function () {
        //all done
      })
      .error(function (err) {
        console.log(err);
      }).start();
    setTimeout(function () {
      testService.stop();
      done();
    }, 1000);
  });
});
