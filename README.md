##Cloud Computing HW3

Working on this assignment:
vf2221 victor Ferrand
pwn2107 Peter Wakahiu Njenga


This is a big-data analytic application to determine the sentiment of various topics from the tweet data. 
We determined which topic tweets were associated by extracting their \#hashtags. We then calculated a sentiment
score for each topic.



#####ELASTIC MAP-REDUCE
We created a cluster of nodes and ran map-reduce on ir. Our application can scale to very large 
datasets because the cluster size is variable and our application in inplmented on AWS EMR. 
The application was written in Java and AWS API.



#####CLOUD STORAGE
We utilized Amazon's simple storage services - S3 - for both out input and output files. The tweet analytics were 
performed on the [dataset](https://s3.amazonaws.com/ColumbiaCloud/final/Assignment3Tweets-2) provided by the course staff.
Similary output files were stored on s3 buckets. This method of implementation proved efficient since s3 is a native 
storage system designed for interoperability among tools.



#####SENTIMENT ANALYSIS
A naive Sentiment analysis was performed by counting the frequency of positive/negative words. 
We implemented a dictictionary of positive/negative words sourced from the internet. Each mapper task
receives as input a line of text containing the body of the tweet, we analysed only those tweets with
\#hashtags in them. The mapper emits a {Key,value} pair constiting of {Hashtag,sentimentScore}. The 
reducer then performs a sum-reduction of {Key,value} pairs with the same hashtags. We attempted to 
perform more sophisticated sentiment analysis with opensource software such as stanford NLP library
by were not successfull. Utilimately our approach is modular therefore the mapper task can be updated
to incorporate more sophisticated sentiment analysis.




Github source code:
https://github.com/viclf/CCBD-HW3
Keys have been removed from the source code for security.
