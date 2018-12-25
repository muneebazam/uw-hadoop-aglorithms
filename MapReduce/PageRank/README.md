# PageRank

An implementation of multi-source personalized pagerank. The main difference between pagerank and its personalized counterpart are:
- All computations are made with respect to a source node *(personalization aspect)*
- Initially, the source node holds all the mass as opposed to an equal mass distribution 
- Random jumps are always made back to the source node as opposed to an equal probability over nodes
- All lost mass is put back into the source node as opposed to being equally distributed

*For more information on PageRank and the diferrence between the variants check out the following links:*
*- https://computer.howstuffworks.com/google-algorithm1.htm*
*- https://www.r-bloggers.com/from-random-walks-to-personalized-pagerank/*

