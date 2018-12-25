# Spam Classifier

A spam classifier trained using stochastic gradient descent. The classifier closely replicates work done by Cormack, Smucker, and Clarke which can be found here: *https://arxiv.org/abs/1004.5168*

The *TrainSpamClassifier.Java* program reads in all the training instances, runs stochastic gradient descent, and outputs a model.

The *ApplySpamClassifier.Java* program applies the trained spam classifier to each test instance. It does this by reading in each input instance, computing its spamminess (score) and makes a prediction of either spam or ham based on the score.

The *ApplyEnsembleSpamClassifier.Java* program applies multiple classifiers to each instance and computes the average score over all models to make a prediction of spam or ham. 
