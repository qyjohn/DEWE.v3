exports.googleHandler = function googleHandler (event, callback) {
var gcs = require('@google-cloud/storage')();
var bucket = gcs.bucket('research-331982');
var file = bucket.file('list.txt');
file.download({destination: 'list.txt'}, function(err) {});
var exec = require('child_process').exec;
var cmd = 'ls -l';

exec(cmd, function(error, stdout, stderr) {
    console.log(stdout);
    console.log(stderr);
});
  callback();
};
