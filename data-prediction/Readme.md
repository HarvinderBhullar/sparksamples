### Data Prediction

Linear Regression is used for predicting the value of current based on the environment data. It is the done in the following steps.

  *  Read the data from lathe.csv, which is created in the data munging step
  *  Generate a LabeledPoint by selecting the current col as label and other 3 as features.
  *  Build the model using Apache ML Linear Regression Package
  *  Get the predicted values
  *  Find the RMSE
  *  Save the model
 
Saved model can be used for predicting the value of current for lathe machines. This can be used for any preventive measure. 

