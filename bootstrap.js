const ManipulateCSV = require('./manipulate_csv').ManipulateCSV;
const _ = require('lodash');
const DEMO_CSV = 'demo.csv';

(async () => {
    // ManipulateCSV object that we re-use for every demo case
    let manipulate_csv_object;
    // Timing string that we re-use for every demo case
    let time_str;

    // Cutting the pipe of operators due to empty output from pipe demo case  
    // This shows that we cut the pipe chain if we have no data to manipulate
    // After the first ceil we shouldn't get anything and we don't create csvs for next operations
    manipulate_csv_object = new ManipulateCSV(DEMO_CSV);
    time_str = `ceil -> max -> min -> sum - HASH(${manipulate_csv_object._uid})`;
    console.time(time_str);
    await manipulate_csv_object
        .pipe('ceil')
        .pipe('max')
        .pipe('min')
        .pipe('sum')
        .exec();
    console.timeEnd(time_str);

    // Custom function that decides randomly if to return the input row
    const random_func = function() {
        const should_return = Math.random() < 0.5;
        if (should_return) {
            this.output = this.input;
        }
        // Notice that non accumulative functions remove the input from context
        delete this.input;
        return this;
    };

    // Custom non accumulative function demo case
    manipulate_csv_object = new ManipulateCSV(DEMO_CSV);
    time_str = `pluck(10) -> random_func - HASH(${manipulate_csv_object._uid})`;
    console.time(time_str);
    await manipulate_csv_object
        .pipe('pluck', 10)
        .pipe(random_func)
        .exec();
    console.timeEnd(time_str);

    // Custom accumulative function that outputs sum of the first absoluted column in the data
    const sum_absolute = function() {
        let input_value = this.input && this.input[0];
        if (_.isNumber(input_value)) {
            input_value = Math.abs(input_value);
            this.output = this.output ? [this.output[0] + input_value] : [input_value];
        }
        return this;
    };

    // Custom non accumulative function demo case
    manipulate_csv_object = new ManipulateCSV(DEMO_CSV);
    time_str = `pluck(10) -> sum_absolute - HASH(${manipulate_csv_object._uid})`;
    console.time(time_str);
    await manipulate_csv_object
        .pipe('pluck', 10)
        .pipe(sum_absolute)
        .exec();
    console.timeEnd(time_str);

    // Mix of several operators demo case
    manipulate_csv_object = new ManipulateCSV(DEMO_CSV);
    time_str = `pluck(10) -> sum -> avg - HASH(${manipulate_csv_object._uid})`;
    console.time(time_str);
    await manipulate_csv_object
        .pipe('pluck', 10)
        .pipe('sum')
        .pipe('avg')
        .exec();
    console.timeEnd(time_str);
    
    // Mix of several operators demo case
    manipulate_csv_object = new ManipulateCSV(DEMO_CSV);
    time_str = `pluck(10) -> filter(0, 26) - HASH(${manipulate_csv_object._uid})`;
    console.time(time_str);
    await manipulate_csv_object
        .pipe('pluck', 10)
        .pipe('filter', [0, 26])
        .exec();
    console.timeEnd(time_str);

})()