-- load into Pig
raw = LOAD 's3n://uw-cse-344-oregon.aws.amazon.com/btc-2010-chunk-*' USING TextLoader as (line:chararray);

-- parse each line into n-triples
ntriples = FOREACH raw GENERATE FLATTEN(myudfs.RDFSplit3(line)) as (subject:chararray,predicate:chararray,object:chararray);

-- group the n-triples by subject column
subjects = group ntriples by (subject) PARALLEL 50;

-- count the tuples associated with each subject
count_by_subject = foreach subjects generate flatten($0), COUNT($1) as count PARALLEL 50;

-- group the results by counts (x-axis values) 
counts_group = group count_by_subject by (count) PARALLEL 50;

-- compute the final counts (y-axis values)
num_points = FOREACH counts_group GENERATE FLATTEN($0), COUNT($1) PARALLEL 50;

-- store the results 
STORE num_points INTO '/user/hadoop/histogram' USING PigStorage();

-- read number of lines in the file
lines = LOAD '/user/hadoop/histogram';
lines_group = GROUP lines ALL;
lines_count = FOREACH lines_group GENERATE COUNT(lines);

-- print the result
DUMP lines_count;


