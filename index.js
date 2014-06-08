'use strict';

var exec = require('child_process').exec
  , spawn = require('child_process').spawn
  , path = require('path');

/**
 * log
 *
 * Logs a message to the console with a tag.
 *
 * @param message  the message to log
 * @param tag      (optional) the tag to log with.
 */
function log(message, tag) {
  var util = require('util')
    , color = require('cli-color')
    , tags, currentTag;

  tag = tag || 'info';

  tags = {
    error: color.red.bold,
    warn: color.yellow,
    info: color.cyanBright
  };

  currentTag = tags[tag] || function(str) { return str; };
  util.log((currentTag("[" + tag + "] ") + message).replace(/(\n|\r|\r\n)$/, ''));
}

/**
 * getArchiveName
 *
 * Returns the archive name in database_YYYY_MM_DD.tar.gz format.
 *
 * @param databaseName   The name of the database
 */
function getArchiveName(databaseName) {
  var date = new Date()
    , datestring;

  datestring = [
    databaseName,
    date.getFullYear(),
    date.getMonth() + 1,
    date.getDate(),
    date.getTime()
  ];

  return datestring.join('_') + '.tar.gz';
}

/* removeRF
 *
 * Remove a file or directory. (Recursive, forced)
 *
 * @param target       path to the file or directory
 * @param callback     callback(error)
 */
function removeRF(target, callback) {
  var fs = require('fs');

  callback = callback || function() { };

  fs.exists(target, function(exists) {
    if (!exists) {
      return callback(null);
    }
    log("Removing " + target, 'warn');
    exec( 'rm -rf ' + target, callback);
  });
}

/**
 * mongoDump
 *
 * Calls mongodump on a specified database.
 *
 * @param options    MongoDB connection options [host, port, username, password, db]
 * @param directory  Directory to dump the database to
 * @param callback   callback(err)
 */
function mongoDump(options, directory, callback) {
  var mongodump
    , mongoOptions;

  callback = callback || function() { };

  mongoOptions= [
    '-h', options.host + ':' + options.port,
    '-d', options.db,
    '-o', directory
  ];

  if(options.username && options.password) {
    mongoOptions.push('-u');
    mongoOptions.push(options.username);

    mongoOptions.push('-p');
    mongoOptions.push(options.password);
  }

  log('Starting mongodump of ' + options.db, 'info');
  mongodump = spawn('mongodump', mongoOptions);

  mongodump.stdout.on('data', function (data) {
    log(data);
  });

  mongodump.stderr.on('data', function (data) {
    log(data, 'error');
  });

  mongodump.on('exit', function (code) {
    if(code === 0) {
      log('mongodump executed successfully', 'info');
      callback(null);
    } else {
      callback(new Error("Mongodump exited with code " + code));
    }
  });
}

/**
 * compressDirectory
 *
 * Compressed the directory so we can upload it to S3.
 *
 * @param directory  current working directory
 * @param input     path to input file or directory
 * @param output     path to output archive
 * @param callback   callback(err)
 */
function compressDirectory(directory, input, output, callback) {
  var tar
    , tarOptions;

  callback = callback || function() { };

  tarOptions = [
    '-zcf',
    output,
    input
  ];

  log('Starting compression of ' + input + ' into ' + output, 'info');
  tar = spawn('tar', tarOptions, { cwd: directory });

  tar.stderr.on('data', function (data) {
    log(data, 'error');
  });

  tar.on('exit', function (code) {
    if(code === 0) {
      log('successfully compress directory', 'info');
      callback(null);
    } else {
      callback(new Error("Tar exited with code " + code));
    }
  });
}

/**
 * sendToS3
 *
 * Sends a file or directory to S3.
 *
 * @param options   s3 options [key, secret, bucket]
 * @param directory directory containing file or directory to upload
 * @param target    file or directory to upload
 * @param callback  callback(err)
 */
function sendToS3(options, directory, target, callback) {
  var knox = require('knox')
    , sourceFile = path.join(directory, target)
    , s3client
    , destination = options.destination || '/';

  callback = callback || function() { };

  s3client = knox.createClient({
    key: options.key,
    secret: options.secret,
    bucket: options.bucket
  });

  log('Attemping to upload ' + target + ' to the ' + options.bucket + ' s3 bucket');
  s3client.putFile(sourceFile, path.join(destination, target),  function(err, res){
    if(err) {
      return callback(err);
    }

    res.setEncoding('utf8');

    res.on('data', function(chunk){
      if(res.statusCode !== 200) {
        log(chunk, 'error');
      } else {
        log(chunk);
      }
    });

    res.on('end', function(chunk) {
      if (res.statusCode !== 200) {
        return callback(new Error('Expected a 200 response from S3, got ' + res.statusCode));
      }
      log('Successfully uploaded to s3');
      return callback();
    });
  });
}

/**
 * sync
 *
 * Performs a mongodump on a specified database, gzips the data,
 * and uploads it to s3.
 *
 * @param mongodbConfig   mongodb config [host, port, username, password, db]
 * @param s3Config        s3 config [key, secret, bucket]
 * @param callback        callback(err)
 */
function sync(mongodbConfig, s3Config, callback) {
  var tmpDir = path.join(require('os').tmpDir(), 'mongodb_s3_backup')
    , backupDir = path.join(tmpDir, mongodbConfig.db)
    , archiveName = getArchiveName(mongodbConfig.db)
    , async = require('async');

  callback = callback || function() { };

  async.series([
    async.apply(removeRF, backupDir),
    async.apply(removeRF, path.join(tmpDir, archiveName)),
    async.apply(mongoDump, mongodbConfig, tmpDir),
    async.apply(compressDirectory, tmpDir, mongodbConfig.db, archiveName),
    async.apply(sendToS3, s3Config, tmpDir, archiveName)
  ], function(err) {
    if(err) {
      log(err, 'error');
    } else {
      log('Successfully backed up ' + mongodbConfig.db);
    }
    return callback(err);
  });
}

/**
 * getFromS3
 *
 * get a file from S3.
 *
 * @param s3Config   s3 config [key, secret, bucket]
 * @param target    file  to download
 * @param destination   path to local file to save archive
 * @param callback  callback(err)
 */
function getFromS3(s3config, target, destination, callback) {
    var knox = require('knox')
    , fs = require("fs")
    , s3client;


    callback = callback || function() { };

    s3client = knox.createClient({
        key: s3config.key,
        secret: s3config.secret,
        bucket: s3config.bucket
    });

    log("Attempting to download "+ target + " from " + " bucket " + s3config.bucket);
    s3client.getFile(target, function(err, res) {
        if (err) {
            log((err));
            return callback(new Error('Error when downloading', err));
        }
        log("Status ", res.statusCode);

        log("SAVING FILE to"+ destination);

        res.on("data", function(chunk) {
            log("bytes received ", chunk.length);
        });

        fs.mkdir(destination.replace(target, ''), function(err) {
            if (err) {
                log("Error in getFromS3: "+err, 'error');
            }
        });
        var saveFile = fs.createWriteStream(destination);
        res.pipe(saveFile);

        res.on("end", function(chunk) {
            if (res.statusCode !== 200) {
                return callback(new Error('Expected a 200 response from S3, got ' + res.statusCode));
            }
            log("FILE FULLY DOWNLOADED");
        });

        saveFile.on("close", function(chunk) {
             log("FILE FULLY WRITTEN TO DISK");
            saveFile.close();
            return callback(null);
        });
    });
}

/**
 * uncompressArchive
 *
 * Uncompress the archive downloaded from S3.
 *
 * @param input     path to input archive
 * @param output     path to output directory
 * @param callback   callback(err)
 */
function uncompressArchive(input, output, callback) {
    var tar
    , tarOptions;

    callback = callback || function() { };

    tarOptions = [
        '-zxvf',
        input
    ];

    log('Starting decompression of ' + input + ' into ' + output, 'info');
    tar = spawn('tar', tarOptions, {cwd: output});


    tar.stdout.on('data', function(data) {
        log(data);
    });

    tar.stderr.on('data', function (data) {
        log(data, 'error');
    });

    tar.on('exit', function (code) {
        if(code === 0) {
            log('successfully uncompressed archive', 'info');
            callback(null);
        } else {
            callback(new Error("Tar exited with code " + code));
        }
    });
}



/**
 * mongoRestore
 *
 * Calls mongoRestore on a specified backup.
 *
 * @param options    MongoDB connection options [host, port, username, password, db]
 * @param directory  Directory to dump the database to
 * @param callback   callback(err)
 */
function mongoRestore(options, backupDir, callback) {
    var mongodump
    , mongoOptions;

    callback = callback || function() { };

    mongoOptions= [
        '-h', options.host + ':' + options.port,
        '-d', options.db,
        '--drop',
        backupDir
    ];
    if(options.username && options.password) {
        mongoOptions.push('-u');
        mongoOptions.push(options.username);

        mongoOptions.push('-p');
        mongoOptions.push(options.password);
    }

    log('Starting mongorestore of ' + options.db, 'info');
    mongodump = spawn('mongorestore', mongoOptions);

    mongodump.stdout.on('data', function (data) {
        log(data);
    });

    mongodump.stderr.on('data', function (data) {
        log(data, 'error');
    });

    mongodump.on('exit', function (code) {
        if(code === 0) {
            log('mongorestore executed successfully', 'info');
            callback(null);
        } else {
            callback(new Error("Mongorestore exited with code " + code));
        }
    });
}

/**
 * sync
 *
 * Performs a mongodump on a specified database, gzips the data,
 * and uploads it to s3.
 *
 * @param mongodbConfig   mongodb config [host, port, username, password, db]
 * @param s3Config        s3 config [key, secret, bucket]
 * @param callback        callback(err)
 */
function restoreFromS3(mongodbConfig, s3Config, remoteFilename, callback) {
    var tmpDir = path.join(require('os').tmpDir(), 'mongodb_s3_backup')
    , backupDir = path.join(tmpDir, mongodbConfig.db)
    , uncompressedDirName = mongodbConfig.db
    , async = require('async');

    callback = callback || function() { };

    async.series([
        async.apply(removeRF, tmpDir),
        async.apply(getFromS3, s3Config, remoteFilename, path.join(tmpDir, remoteFilename)),
        async.apply(uncompressArchive, path.join(tmpDir, remoteFilename), tmpDir ),
        async.apply(mongoRestore, mongodbConfig, backupDir)
    ], function(err) {
        if(err) {
            log(err, 'error');
        } else {
            log('Successfully Restored ' + mongodbConfig.db);
        }
        return callback(err);
    });
}

module.exports = { sync: sync, log: log, restore: restoreFromS3 };
