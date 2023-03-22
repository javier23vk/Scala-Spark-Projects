# Scala Spark Projects

## Travel Trends
In this project, an analysis of the nationality of the tourists who visit a certain city will be carried out, with the aim of determining which is the nationality that visits that city the most and what is its percentage in relation to the total number of tourists.

The data used for the analysis were obtained from a csv file that can be obtained from the following link: https://www.kaggle.com/datasets/rkiattisak/traveler-trip-data.

## Penguin Size
This project deals with a prediction classification problem whose test data is obtained through Spark Structured Streaming.

The dataset is obtained from the page: https://www.kaggle.com/datasets/parulpandey/palmer-archipelago-antarctica-penguin-data.

This project predicts what kind of penguin it is through some variables of the penguin's physiological structure.

To simulate that the test data is streamed, first the model is created and trained with some data but a certain amount is reserved and divided into several files and placed in a project directory. This directory is used to read by streams the data to test the model.
