# tnydb #

A little tool for interactive analysis of big data

## Why bother? ##

Analysing big data is more time consuming than it is difficult.  The key challenge to productivity is being able to run your analysis quickly, so that you can adjust your code accordingly.  When working with small data sets, this process is interactive, with each run only taking seconds.  When the data gets big, you can code just as quickly, but now each test run takes hours or days.
A key way of combating this is to slice up your data set, but this is fraught with danger.  Most of the time when conducting analysis, you are looking for features within the dataset.  Thus, when you slice up your data to make a test set which you can work with interactively, you need to make sure your slice contains a feature (for obvious reasons).  As you develop your algorithm, there will be real a danger of over-fitting your model to that one, previously selected feature – which is not a good outcome.
Computing power is cheap, and most analysts have access to clusters (or can use EC2).  So a logical idea is to use lots of computers to get things done faster.  Note that this is a very different requirement to most databases where the name of the game is supporting lot of users.  Here we want to use lots of computers to do one thing very quickly.
 

## What is the use-case this is for? ##

Let’s say we have a single table of data with the following schema (in SQL)

```sql
CREATE TABLE read (
	id INT,
	pair_id INT,
	quality INT,
	pos BIGINT,
	is_reverse BIT
	is_first BIT
)
```

I want to draw a histogram of the distance between the first read in the pair and the second, broken into buckets of 50.   That is want to get the number of read-pairs with difference in position of 0, 50, 100, 150, 200, etc.  I also want to do this only where the quality is more than 10, and they are both in the same orientation.
I can express this as the following set of commands:

```c
// Only include high quality reads
passed_qc = read[quality > 10]

// Join pairs on pair_id and id, outputing the different in 
// size (isize) and the position of each of the reads
pairs := (passed_qc[is_first=1]*passed_qc[is_first=0])
	[
		1.id=2.pair_id 					// Same pair of data
	&&	2.pos - 1.pos < 0 				// Negative insert size
	&& 	(1.is_reverse = 2.is_reverse)	// Same orientation
	]
	-> 2.pos - 1.pos "isize", 1.pos "a_pos", 2.pos "b_pos"

// Get me a histogram of the different isizes, in 50 base pair buckets
pairs->50*(isize/50), count() 
```


The key things here are:

 *	The syntax is terse, much more so than SQL but still very SQL like

 * 	Everything is a table

 * 	WHERE conditions are placed in [] (square brackets)

 * 	JOINS are done using the * operator, with tables being aliased with 1,2... depending on the order they are joined on.

 *	GROUP BY is redundant, if a query contains an aggregation, then any non-aggregated columns will automatically be handled like a GROUP BY.

 * 	Columns / Expressions are aliased by placing the alias in double quotes.

 * 	"->" replaces the SELECT command and it occurs at the end of the query definition.

 *	Queries can be aliased easily without being realized by using the "=" operator.  That is, we can use "passed_qc" wherever we would normally use a table without actually executing and storing it anywhere.  This is basically like a VIEW in SQL databases, but without the hassle of persistence

 *	We can actually create a new table from a query by using the ":=" operator, which will actually execute the query and allow it to be referenced by the variable name.  This is like inserting into a table in SQL, and means that a separate copy of the table will be created.  This is particularly useful for when you create expensive aggregations where the overhead of storing the output in memory is less than the overhead of constantly re-running the query.  


## What is the status of this project ##

Its in active development.  We have built up the low level data access aspects of the system, and are working on the network layer currently.  It is hoped that we will have an alpha release ready in September 2012 - but this depends on many things and may blow out. 

 
## Why not use one of these tools? ##

### Hadoop ###

Hadoop is an awesome tool, but its elephant logo is fitting – its big and cumbersome to set up.  Writing a map-reduce query for every query is similarly cumbersome.  Hive is headed in the right direction, but everything is read from HDFS, which means networks and disk access, which makes interactive analysis difficult.  Besides, I wasn’t smart enough to get Hadoop-On-Demand up and running with Hive on my cluster.

### MongoDB, Riak, Redis, CouchDB, etc ###

These tools are awesome at scaling to support a lot of users concurrently.  However they are pretty awful at supporting complex queries from a single user.  The workloads are completely different.  These NoSQL solutions are wonderful for scaling a web app, but trying to do complex queries involving aggregations, joins and sub queries and generating generalized linear models from the output would kind of be tricky even in Redis.


## So how is all of this implemented? ##

tnydb is written in a combination of C and golang.  The division of duties between languages is simple, anything that does data access on a _Page_ of data is written in C, anything higher up is written in golang.  Thus we get the full control of memory allocation and access from C, with the speed of development that golang provides.

## Whats the License? ##

GPL at the moment, but I am not sure how this will work moving forward

## Who is organizing this? ##

Terence Siganakis of Growing Data http://www.growingdata.com.au

