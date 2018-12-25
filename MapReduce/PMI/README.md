# PMI

Pointwise Mutual Information (PMI) is a measure of association used in information theory and statistics. For more information on PMI see *https://en.wikipedia.org/wiki/Pointwise_mutual_information*

Both the *PairsPMI.Java* and *StripesPMI.Java* program compute the PMI of all co-occuring pairs of words in a collection.

The *PairsPMI.Java* variant writes the co-occuring word pair *(A,B)* as the key along with the PMI and word count *(PMI, Count)* as the value in the final output.

The *StripesPMI.Java* variant writes a word *(A)* as the key and a map containing co-occuring words along with their PMI and word count *(B, (PMI, Count))* as the value in the final output.

The stripes version is preffered over the the pairs version due to a cleaner output and less data being shuffled to reducers during execution.
