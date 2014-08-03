-- load into Pig
raw = LOAD 's3n://uw-cse-344-oregon.aws.amazon.com/btc-2010-chunk-000' USING TextLoader as (line:chararray); 

-- parse each line into n-triples
ntriples = FOREACH raw GENERATE FLATTEN(myudfs.RDFSplit3(line)) as (subject:chararray,predicate:chararray,object:chararray);

-- filter to get only 'rdfabout.com'
subject_filter = FILTER ntriples BY subject MATCHES '.*rdfabout\\.com.*';

-- another copy of the filtered collection
copy_subject_filter = FOREACH subject_filter GENERATE subject as subject2, predicate as predicate2, object as object2;

-- join the two copies
collection_join = JOIN subject_filter BY object, copy__subject_filter by subject2;

-- remove duplicate tuples
collection = DISTINCT collection_join;

-- store the results 
STORE collection INTO '/user/hadoop/collection' USING PigStorage();

-- read number of lines in the file
lines = LOAD '/user/hadoop/collection';
lines_group = GROUP lines ALL;
lines_count = FOREACH lines_group GENERATE COUNT(lines);

-- print the result
DUMP lines_count;

