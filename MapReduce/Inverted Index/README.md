# Inverted Index

An inverted index is a data structure which maps content, such as words or numbers, to its locations in set of documents.

The *BuildInvertedIndexCompressed.java* program takes in a collection and builds an inverted index structure which uses variable integer compression and buffered postings to avoid overflow.

The *BooleanRetrievalCompressed.java* program takes in a boolean query and returns the article along with the sentences that match the specified query.

For example the boolean query *"waterloo stanford OR cheriton AND"* corresponds to all article + sentences which contains either the word *waterloo* or *stanford* along with the word *cheriton*
