var fs = require('fs');
var MongoClient = require('mongodb').MongoClient;
var async = require('async');

module.exports = {
    process: process
};

var outstandingInserts = 0;

function printHelp(scriptName) {
    console.log("To import one or more files:");
    console.log("  " + scriptName + "[options] file1 [file2 [file3 [...]]]");
    console.log("To import from stdin:");
    console.log("  " + scriptName + " - [id_prefix]");
    console.log("The document IDs will be the filename plus the line number.");
    console.log("If importing from stdin, it will be id_prefix plus the line number.");
    console.log("If id_prefix is not provided, it will be a random string plus the line number.");
    console.log("Options:");
    console.log("  --host       MongoDB host (standard format)");
    console.log("  --db         MongoDB database name");
    console.log("  --collection MongoDB collection name");
}

function process(argv, stdin, lineProcessor, doneCallback) {
    if(argv.length < 2) {
        throw "There needs to be at least two elements in argv";
    }
    // The node executable is the first element in the array, even if the script is marked
    // executable and run directly.
    var scriptName = argv[1];
    var files = [];
    var hostName = false;
    var dbName = false;
    var collectionName = false;

    for(argIndex = 2; argIndex < argv.length; ++argIndex) {
        var arg = argv[argIndex];
        if(arg === "--help") {
            printHelp(scriptName);
            return;
        } else if(arg === "--host") {
            ++argIndex;
            hostName = argv[argIndex];
        } else if(arg === "--db") {
            ++argIndex;
            dbName = argv[argIndex];
        } else if(arg === "--collection") {
            ++argIndex;
            collectionName = argv[argIndex];
        } else {
            files.push(arg);
        }
    }

    if(!hostName) {
        throw "MongoDB host must be specified with '--host hostname[:port]";
    }
    if(!dbName) {
        throw "MongoDB database name must be specified with '--db dbName'";
    }
    if(!collectionName) {
        throw "MongoDB collection name must be specified with '--collection collName";
    }
    if(files.length < 1) {
        throw "The file(s) to import must be specified (use '-' to read from standard input)";
    }

    MongoClient.connect('mongodb://' + hostName + '/' + dbName, function(err, db) {
        if(err) throw err;

        var collection = db.collection(collectionName);
        //bulk = collection.initializeUnorderedBulkOp();
        //console.log(bulk.toString());

        readFiles(collection, files, lineProcessor, stdin, doneCallback);
    });
}

function readFiles(collection, filenames, lineProcessor, stdin, doneCallback) {
    var filename;
    var input;
    var waitForOutstandingInserts = function() {
        // Wait for any outstanding inserts to finish
        if (outstandingInserts > 0) {
            setTimeout(waitForOutstandingInserts, 10000);
        } else {
            doneCallback();
        }
    };

    if(filenames[0] === '-') {
        if(filenames.length > 1) {
            filename = filenames[1];
        } else {
            filename = randomId();
        }
        console.log("Processing stdin with id: " + filename);
        input = stdin;
        readFile(collection, input, filename, lineProcessor, waitForOutstandingInserts);
    } else {
        async.eachSeries(filenames, function(filename, fileFinished) {
            console.log("Processing " + filename);
            input = fs.createReadStream(filename);
            readFile(collection, input, filename, lineProcessor, fileFinished);
        }, waitForOutstandingInserts);
    }
}

function randomId() {
    // from http://stackoverflow.com/a/19964557/575982
    var N = 10;
    return (Math.random().toString(36)+'00000000000000000').slice(2, N+2);
}

function readFile(collection, input, filename, lineProcessor, finishedCallback) {
    var remaining = '';
    var lineNumber = 1;

    input.on('data', function(data) {
        remaining += data;
        var index = remaining.indexOf('\n');
        var last = 0;
        while(index > -1) {
            var line = remaining.substring(last, index);
            last = index + 1;
            insertRecord(collection, line, filename, lineNumber, lineProcessor);
            lineNumber++;
            index = remaining.indexOf('\n', last);
        }
        remaining = remaining.substring(last);
    });

    input.on('end', function() {
        // If the last line didn't end with a new line, process it now
        if(remaining.length > 0) {
            insertRecord(collection, remaining, filename, lineNumber, lineProcessor);
        }
        finishedCallback();
    });
}

function insertRecord(collection, line, filename, lineNumber, lineProcessor) {
    var recordId = filename + '-' + zeroPad(lineNumber, 12);
    var doc = lineProcessor({text: line, filename: filename, lineNumber: lineNumber, recordId: recordId});
    if (doc) {
        ++outstandingInserts;
        collection.insert(doc, {w: 1}, function(err, doc) {
            if(err) {
                console.log(err);
                console.log(filename + ":" + lineNumber + " " + line);
            }
            --outstandingInserts;
        });
    }
}

// Convert number to a string and pad with preceding zeros until its length is len
function zeroPad(number, len) {
    var s = number + "";
    while(s.length < len) {
        s = "0" + s;
    }
    return s;
}
