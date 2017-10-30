# Discovering Foodborne Illness in Online Restaurant Reviews

This repo contains the results for the publication "Discovering Foodborne Illness in Online Restaurant Reviews", to appear in JAMIA 2017.

## Contents
 * `Official Results.ipynb` contains the reproducible experiments from the paper. (To actually be able to run the experiments you need the data, which can be requested from [Tom Effland](mailto:teffland.cs.columbia.edu).)
 * `sick_results.csv` and `mult_results.csv` contain the final performance metrics from `Official Results.ipynb` (These correspond to Tables 1 and 2 in the paper, respectively)
 * `experiments`  contains all of the supporting experiment code, model definitions and the preliminary experiments. (Check out the [README](experiments/README.md) in that directory for further info)
 * `figures` contains model performance reports from `Official Results`
 * `final_models` contains the final models from the paper.
 * `experiments/best_results` contains the best models from training.
 * `figures`, `final_models`,  `experiments/best_results` are symlinked and can be extracted from `twitter-results.tar.gz` in [google drive folder](https://drive.google.com/open?id=0B2sTl8wocuJocEVTalZ0UzFhd3c)

## How To Use `final_models` in practice:

- You'll need `scikit-learn 18.2` and `numpy`
- To load them, use:
  ```python
  from sklearn.externals import joblib
  yelp_sick_classifier = joblib.load(‘final_yelp_models/final_yelp_sick_model.gz’)
  yelp_mult_classifier = joblib.load(‘final_yelp_models/final_yelp_mult_model.gz’)
  ```
- To predict with them, use:
  ```python
  sick_preds = yelp_sick_classifier.predict(numpy_array_of_utf8_or_str) 
  mult_preds = yelp_mult_classifier.predict(numpy_array_of_utf8_or_str)
  ```
- And to get the prediction probabilities for the positive class, use:
  ```python
  sick_pred_pos_probs  = yelp_sick_classifier.predict_proba(numpy_array_of_utf8_or_str)[:,1]
  mult_pred_pos_probs = yelp_mult_classifier.predict_proba(numpy_array_of_utf8_or_str)[:,1]
  ```
