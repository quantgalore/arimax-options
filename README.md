# S&P 500 ARIMAX Option Trading Strategy
System for using ARIMAX models to trade options on the S&amp;P 500.

Original Source w/ Methodology - "Pub-Stomping Option Markets with ARIMAX" - [The Quant's Playbook](https://quantgalore.substack.com/)

The workflow for this is as follows:

1. Run the “sp500-dataset-builder” file. 

    * This will create the training dataset in about 30 seconds.

2. Run the “sp500-dataset-daily-production” file.

    * This will create the data points that will be used for getting the next day’s prediction.

3. Run the “SP500_Training” notebook on Google Colab.

    * This will train the model on the dataset. 

    * When complete, this will create a download which contains the .”pkl” model file, upload this to your Google Drive.

4. Run the “SP500_Production” notebook on Google Colab.

    * This will return the prediction for the next day’s return. 

5. Track the prediction in OptionStrat

    * If you do not wish to subscribe to the service, the free plan’s data is delayed by 15 minutes, so you will have to manually enter the real market ask price.

## The above numbered methodology has been updated, please refer to "Turbo-Charging The ARIMAX Option System" for instructions on the new process. 
