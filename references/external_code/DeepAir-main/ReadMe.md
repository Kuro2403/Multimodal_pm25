# Data Storage
`./data/DeepAir/STData`  
The most critical data storage path, which can be directly utilized for pre-training GBM.
Pay attention to the `dataPath` defined at the beginning of each program, as it specifies the dynamic data for all time periods.

# baseline_models 

## RF
Random forest model with situ data.

## GBM
GBM model with situ data.

## DCNN
DCNN model, using CNN + MLP to predict PM2.5.




# CA_PM25_prediction

## data_preprocess

- `daily_dataPrepare_universalCNNembed.py`  
  Processes all grid data in California using the pre-trained CNN model. Encodes Elevation and LandCover information and adds it to the original data to generate new data for prediction.  
    - Depends on the pre-trained models in `preTrain_and_mergeData/preTrainCNN/ElevaLand.preTrain.shapeNoDrop/saved_earlyStop_weights_yearsG8`.  
    - Uses the pre-trained lightGBM model for subsequent operations.  

- `daily_dataPrepare_universalDataMerge.py`  
  Merges encoded LandCover and Elevation data with previous data to replace features [this step is not actually required].  
    - Depends on data from the original `SituData`.  
    - Each day’s records are merged with the corresponding encoded LandCover and Elevation data for each grid.  

- `daily_CA_PM25_mergeGBMPred_batch.py`  
  Predicts PM2.5 values for each grid using the trained lightGBM model, with additional data merging operations.  

- `CA_ElevaLand.preTrain.waterDropLet`  
  Stores prediction results, visualizations, and data:  
    - Organized by year and date.  
    - `CA_allGrid_PM25_pred.csv`: Stores prediction results for specific dates.  
    - `CA_daily_gridPM.pkl`: Formats predicted values for each grid (CA_lon, CA_lat, CA_grid_value).  
    - `CA_PM_plot.png`: Visualizes prediction results.  


# DeepAir
## preTrainCNN_and_mergeData

### preTrainCNN
Pre-train a CNN model to directly predict PM2.5.  

The model structure used is `situNet_ElevaLand`.  

- Use CNN to encode Elevation and LandCover information.
- Use embedding encoding for date and basinID data, which will serve as input for the subsequent DNN model.  

After grouped training, store the trained models in `ElevaLand.preTrain.shapeNoDrop`, which will later be used to directly read data.

### CNN_embed_and_merge_data
Based on the pre-trained CNN PM2.5 prediction model:
- Encode data from the original dataset containing PM2.5 labels.
- Integrate Elevation and LandCover information to generate new data.  
This new data is used for subsequent lightGBM model training.

## DeepAir_GBM 
Train a lightGBM model for prediction based on the Elevation and LandCover data encoded by the pre-trained CNN model.

This model is used for subsequent predictions across the entire California region.

Running the `py` files that start with `batch_` to train model.
