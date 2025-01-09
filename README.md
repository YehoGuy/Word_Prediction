## Overview
The Hadoop Word Prediction Project is a distributed system designed to process textual BIG-DATA and provide word predictions based on google hebrew 3-grams. It leverages Hadoop's MapReduce framework for efficient data processing and is scalable to handle MASSIVE datasets.

## Features
- **Word Prediction**: Predicts the most probable word(s) given a context.
- **Distributed Processing**: Utilizes Hadoop MapReduce for efficient computation.
- **Scalable Architecture**: Handles large-scale datasets across multiple nodes.
- **Advanced Sorting**: The convenience of calculation of each Step is Achieved by the compareTo & hash methods of the custom processed keys.
- **Memory efficiency**: Stores only 1 Global variable for the entire calculation, and due to the advanced sorting, is able to produce a relatively low number of key-value pairs!
- **Stop Word Filtering!** - The first step make sure to filter an y Bad Stop-Words for more accuracy and readability.

## Mathematic Background
my implementation is based on the Thede & Harper formula for calculating the probability of the trigram (W1,W2,W3) given the presence of the bigram (W1,W2). As presented in the Article "A second-order Hidden Markov Model for part-of-speech tagging":

__P(W3|W1,W2) = K3*(N3/C2) + (1-K3)*K2*(N2/C1) + (1-K3)*(1-K2)*(N1/C0)__

where __K3__ = (log(N3+1)+1)/((log(N3+1)+2)) , __K2__ = (log(N2+1)+1)/((log(N2+1)+2))
      ▪ __N1__ is the number of times w3 occurs.
      ▪ __N2__ is the number of times sequence (w2,w3) occurs.
      ▪ __N3__ is the number of times sequence (w1,w2,w3) occurs.
      ▪ __C0__ is the total number of word instances in the corpus.
      ▪ __C1__ is the number of times w2 occurs.
      ▪ __C2__ is the number of times sequence (w1,w2) occurs.


## System Architecture
▪ note: when we want to reference a trigram we use (W1,W2,W3), for bigram we use (W1,W2,*) and for single word (W1,*,*)
▪ Steps are pipelined, meaning that Step i's output, is Step i+1's input.


# Step1 - Word Count
relevant files: Step1, Key1
The first Step is responsible for loading processing the google hebrew 3-grams file from AWS S3 to the Hadoop File System.
Its output is a lexicographically Ordered (by W1-->W2-->W3, '*' is prior to every char) file, where each key represents a word/bigram/trigram and each value is its number of appearences in the corpus.

# Step2 - Calculating K3*(N3/C2) for each Trigram
relevant files: Step2, Key1, Key2
The second Step is responsible for Calculating K3*(N3/C2) for each Trigram, which is conveniet given that the input is lexicographically ordered thanks to the custom Key1 class.
Its output is then lexicographically Ordered by W2-->W3-->W1 ('*' is prior to every char), for convenience in the next Step.

# Step3 - Adding (1-K3)*K2*(N2/C1) to each Trigram
relevant files: Step3, Key2, Key3
The third Step is responsible for Calculating (1-K3)*K2*(N2/C1) for each Trigram, which is conveniet thanks to the input ordering by Key2 class.
Its output is then lexicographically Ordered by W3-->W1-->W2 ('*' is prior to every char), for convenience in the next Step. It also discards the Bigrams, which are no more useful for the rest of the calculatuion, which adds to the efficiency.

# Step4 - Adding (1-K3)*(1-K2)*(N1/C0) to each Trigram
relevant files: Step4, Key3, Key4
The fourth Step is responsible for Calculating 1-K3)*(1-K2)*(N1/C0) for each Trigram, which is conveniet thanks to the input ordering by Key2 class.
Its output is then lexicographically Ordered by W3-->W1-->W2 ('*' is prior to every char), for convenience in the next Step. It also discards the Single Words, which are no more useful for the rest of the calculatuion (final sorting).

# Step5 - Final Sorting
relevant files: Step5, Key4
The fifth step is responsible for the final sorting (based on Key4's compareTo method).
The sorting goes by: W1-->W2 lexicographically ascending, Probability for W3 descending.

# How To Run:
1. Make sure you have an AWS Account with credentials set-up at local env.
2. go over to the Helpers.Consts file- there you can see all of the relevant S3 Paths used. you can modify them to your liking,
   just make sure that the bucket exists and that within it the jars/ folder exists.
3. compile and upload the Step1 - Step5 JARs to the  Jars/ folder.
4. Run The App.java file and track progress on AWS Emr console, according to the locally printed Jobflow-Id.


## Some Runtime Stats:

# Without local aggregation (8 Mappers):
▪ Number of Map Output Records was: 56,963,480.
▪ Number of Reduce Input Records: 56,963,480.
▪ Time Elapsed: 26 minutes, 33 seconds.

# With local aggregation - Combiner1 (8 Mappers):
▪ Number of Map Output Records was: 56,963,480.
▪ Number of Reduce Input Records: 812,839.
▪ Time Elapsed: 24 min 29 sec.
* We can see that local aggregation saved a ton of data communication between the Hadoop nodes,
* Yet the time did not improve drastically - __Amdahl's Law__ - local aggregation was needed and used only at step 1.

# With local aggregation - Combiner1 (5 Mappers):
▪ Number of Map Output Records was: 56,963,480.
▪ Number of Reduce Input Records: .
▪ Time Elapsed: .


Name: Guy Yehoshua
Id: 326529229

