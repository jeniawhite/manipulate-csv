'use-strict';
const fs = require('fs');
const csv_parse = require('csv-parse');
const csv_writer = require('csv-writer');
const _ = require('lodash');
const { finished } = require('stream/promises');
const { v1: uuidv1 } = require('uuid');

// Class that allows to pipe operations on csv files
// Every instance has it's own uid and uses it in order to differentiate between different executions of pipes
// Currently I've decided to use instance per single execution of pipes for simplicity, but the code can be easily edited
// With the initialization of _op_queue upon end of execution and generation of new uid for next execution pipes
// For each pipe there will be created an output csv with the result of the operation on the previous csv
// The resulting csv will be used as an input source for the next operation in the queue
// I've decided to leave the resulting csv's after every operation in the queue
// This allows to easily debug and even take up a csv from any stage input it to a different queue if needed in future
// The left overs of csv's for every stage can be easily cleaned up if not needed
// I've decided not to use folders for every queue and just output the csv's directly into the folder with prefix of uid
// The last stage csv is copied with prefix "output-" for simplicity in order to see the actual result instead of taking last stage csv
// If somewhere between the stages we encounter an empty csv (no output after operation on csv), we break the chain for efficiency
// I've assumed that there is no point to continue running the queue on empty csv from one of the stages, but it can be changes easily
// Coded with the assumption that the origin csv file is immutable
class ManipulateCSV {
    
    // file - path to the csv file (String)
    constructor(file) {
      // Generate custom id for manipulated stages csv
      this._uid = uuidv1();
      // Assign the origin csv file
      this._file = file;
      // Queue for the requested operations to perform on csv
      // Example: [
      //    { func: 'max', args: [] },
      //    { func: Function, args: [1, 2] }   
      // ]
      this._op_queue = [];
    }

    // func - operation to perform (Function for custom functions OR String for existing operations)
    // args - arguments for the operation (Array<any> or Empty)
    pipe(func, args = []) {
        // Push the relevant operation with the arguments into the op_queue
        this._op_queue.push({ func: _.isFunction(func) ? func : this[func], args: _.isArray(args) ? args : [args] });
        return this;
    }

    // Execute the operations queue
    async exec() {
        // source csv file to read from (this is changing with every stage)
        let src_csv;
        // target csv file to write to (this is changing with every stage)
        let target_csv;

        // Loop on the queue of operations
        for (let index = 0; index < this._op_queue.length; index++) {     
            // Get the operation object (includes the operation and the arguments)  
            const op_obj = this._op_queue[index];
            // Generate dynamic source csv for the stage
            src_csv = `${!index ? this._file : `${this._uid}-${index - 1}.csv`}`;
            // Generate dynamic target csv for the stage
            target_csv = `${this._uid}-${index}.csv`;

            // Create a reader for the source csv 
            let reader = fs.createReadStream(src_csv, { encoding: 'utf-8' })
                .pipe(csv_parse({ delimiter: ',', cast: true }));

            // Create a writer for the destination csv 
            let writer = csv_writer.createArrayCsvWriter({
                path: target_csv,
                append: true,
            });

            // Scope for the invoked operations
            let scope = {};
            // Promise for the writer to the destination csv
            let writer_promise;

            // Listen to readable event from the reader and perform invocation
            reader.on('readable', async function() {
                // While there are rows to read
                while (scope.input = reader.read()) {
                    // Make sure that the writer finished writing
                    if (writer_promise) await writer_promise;
                    // Invoke the operation
                    scope = op_obj.func.call(scope, ...op_obj.args);
                    // If operation is non accumulative
                    if (!scope.input && scope.output) {
                        // Write the output to the destination csv
                        writer_promise = writer.writeRecords([scope.output]);
                        scope.output = undefined;
                    }
                }
            });

            // Wait for the reader and writer to finish
            await finished(reader);
            if (writer_promise) await writer_promise;

            // Write accumulative operation result to csv
            if (scope.output) await writer.writeRecords([scope.output]);

            // Make sure to create the csv for empty stage
            if (!fs.existsSync(target_csv)) {
                fs.closeSync(fs.openSync(target_csv, 'w'));
                // Break the queue on empty stage for efficiency
                break;
            }
        }

        // Copy the last stage csv to the output csv
        fs.copyFileSync(target_csv, `output-${this._uid}.csv`);
    }

    // Accumulative function that outputs the sum of the first column of the rows
    sum() {
        const input_value = this.input && this.input[0];
        if (_.isNumber(input_value)) {
            this.output = this.output ? [this.output[0] + input_value] : [input_value];
        }
        return this;
    }

    // Accumulative function that outputs the minimum of the first column of the rows
    min() {
        const input_value = this.input && this.input[0];
        if (_.isNumber(input_value)) {
            this.output = [Math.min((this.output ? this.output[0] : Infinity), input_value)];
        }
        return this;
    }

    // Accumulative function that outputs the maximum of the first column of the rows
    max() {
        const input_value = this.input && this.input[0];
        if (_.isNumber(input_value)) {
            this.output = [Math.max((this.output ? this.output[0] : -Infinity), input_value)];
        }
        return this;
    }

    // Accumulative function that outputs the average of the first column of the rows
    avg() {
        const input_value = this.input && this.input[0];
        if (_.isNumber(input_value)) {
            this.output = this.output ? [(this.output[0] + input_value) / 2] : [input_value];
        }
        return this;
    }

    // Function that outputs the n’th field (zero indexed) as row from every row
    pluck(n) {
        const input_value = this.input && this.input[n];
        if (input_value) {
            this.output = [input_value];
        }
        // Notice that non accumulative functions remove the input from context
        delete this.input;
        return this;
    } 

    // Function that outputs all the rows where n’th field is equal to x
    filter(n, x) {
        const input_value = this.input && this.input[n];
        // Very simplistic check using toString
        // Can be improved to something more complex with typing and etc...
        if (input_value && (input_value.toString() === x.toString())) {
            this.output = this.input;
        }
        // Notice that non accumulative functions remove the input from context
        delete this.input;
        return this;
    }

    // Function that outputs the ceiling (round up) of the first column in the data
    ceil() {
        const input_value = this.input && this.input[0];
        if (_.isNumber(input_value)) {
            this.output = [Math.ceil(input_value)];
        }
        // Notice that non accumulative functions remove the input from context
        delete this.input;
        return this;
    }

}

exports.ManipulateCSV = ManipulateCSV;