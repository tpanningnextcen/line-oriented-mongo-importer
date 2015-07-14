This Node module takes care of most of the routine parts of importing a file into a MongoDB
collection, so long as each record is on its own line. It also handles parsing command line
parameters.

Example:

    #!/usr/bin/env node
    var lomi = require('line-oriented-mongo-importer');

    function processLine(lineInfo) {
        // This method will be called once for each line in the file(s). The return value is
        // inserted into the MongoDB collection. lineInfo has the following structure:
        // {
        //     text: 'your,data', // the line that was read from the file
        //     filename: 'data.csv', // the name of the file that text came from (or the id assigned to stdin)
        //     lineNumber: 42, // the line number of the file that was passed in
        //     recordId: 'data.csv-42' // a suggested value for _id that will be reproducible if
        //                             // this file is imported again
        // }


        if (/*some error condition*/) {
            return null; // this will cause no document to be inserted for this line
        }
        var doc = { /* create the document from lineInfo.text */};
        return doc;
    }

    lomi.process(process.argv, process.stdin, processLine, function() {
        console.log("Finished parsing");
        process.exit(0);
    });
