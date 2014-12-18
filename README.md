##Cloud Computing HW3

Working on this assignment:
- vf2221 victor Ferrand
- pwn2107 Peter Wakahiu Njenga


This is a big-data analytic application to determine the sentiment of various topics from the tweet data. 
We determined topics using their \#hashtags and then calculated a sentiment score for each topic.



#####ELASTIC MAP-REDUCE
We configured and created a cluster of nodes to ran our map-reduce applicatioon. Our application can scale to very large 
datasets because the cluster size is variable and our application in implemented on AWS EMR.
The number of nodes (mapper and reducers) provisioned can be adjusted to match the size of
dataset. In addition, the dataset is based on HDFS, a distributed file system that supports
high aggregate bandwith to nodes. The application was written in Java and AWS API. The **mapper** function takes a
line of text with the tweet and calculates it's sentiment. It outputs a \<key, value\> pair for
every topic. The **reducer** performs a sum reduction.



#####CLOUD STORAGE
We utilized Amazon's simple storage services - S3 - for both out input and output files. The tweet analytics were 
performed on the [dataset](https://s3.amazonaws.com/ColumbiaCloud/final/Assignment3Tweets-2) provided by the course staff.
Similarly output files were stored on s3 buckets. This method of implementation proved efficient since s3 is a native 
storage system designed for interoperability among tools.



#####SENTIMENT ANALYSIS
A naive Sentiment analysis was performed by counting the frequency of positive/negative words. 
We implemented a dictionary of positive/negative words sourced from the internet. Each mapper task
receives as input a line of text containing the body of the tweet, we analysed only those tweets with
\#hashtags in them. The mapper emits a {Key,value} pair consisting of {Hashtag,sentimentScore}. The 
reducer then performs a sum-reduction of {Key,value} pairs with the same hashtags. We attempted to 
perform more sophisticated sentiment analysis with open source software such as stanford NLP library
by were not successful. Ultimately our approach is modular therefore the mapper task can be updated
to incorporate more sophisticated sentiment analysis.




Github source code:
- https://github.com/viclf/CCBD-HW3
- Keys have been removed from the source code for security.
