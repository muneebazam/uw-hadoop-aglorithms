# Bigram

*A bigram is a sequence of two adjacent words from a string of words (tokens). A bigram is an n-gram for n=2. For more information on bigrams see https://en.wikipedia.org/wiki/Bigram#Bigram_frequency_in_the_English_language*

Both programs compute the relative frequencies of all bigrams in a text collection.

The *ComputeBigramRelativeFrequencyPairs.scala* variant writes the co-occuring word pair *(A,B)* as the key along with the frequency as the value in the final output.

The *ComputeBigramRelativeFrequencyStripes.scala* variant writes a word *(A)* as the key and a map containing co-occuring words along with their frequencies *(B, freq)* as the value in the final output.
