"""
Test hyperparameters for regression.
We want to maximize likelihood.
"""
from argparse import ArgumentParser
import logging
import os
from statsmodels.discrete.discrete_model import Logit
from model_helpers import load_clean_data, run_regularized_regression_raw_data
import numpy as np
import pandas as pd

def logit_cdf(X):
    return 1 / (1 + np.exp(-X))

## log likelihood
def compute_log_likelihood(params, X, Y):
    q = 2 * Y - 1
    ll = np.sum(np.log(logit_cdf(q * np.dot(X, params))))
    return ll

def convert_to_raw_data(data, dep_var, scalar_vars, cat_vars, cat_var_treatment_vals):
    """
    Convert structured data to raw data, 
    e.g. categorical variable to dummies.
    """
    scalar_var_str = ' + '.join(scalar_vars)
    cat_var_str = ' + '.join(list(map(lambda x: 'C(%s, Treatment("%s"))'%(x[0], x[1]), zip(cat_vars, cat_var_treatment_vals))))
    var_str_list = list(filter(lambda x: x != '', [scalar_var_str, cat_var_str]))
    independent_var_str = ' + '.join(var_str_list)
    formula = f'{dep_var} ~ {independent_var_str}'
#     print(formula)
    model = Logit.from_formula(formula, data)
    return model.exog, model.endog

def main():
    parser = ArgumentParser()
    parser.add_argument('data')
    parser.add_argument('--data_type', default='loanword')
    parser.add_argument('--L2_weights', nargs='+', default=[])
    parser.add_argument('--scalar_vars', nargs='+', default=[])
    parser.add_argument('--cat_vars', nargs='+', default=[])
    parser.add_argument('--cat_var_treatment_vals', nargs='+', default=[])
    parser.add_argument('--fixed_effect_vars', nargs='+', default=[])
    parser.add_argument('--out_dir', default='../../output/regression_results/')
    args = vars(parser.parse_args())
    logging_file = '../../output/test_L2_weights_regression.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    ## load data
    scalar_vars = args['scalar_vars']
    cat_vars = args['cat_vars']
    cat_var_treatment_vals = args['cat_var_treatment_vals']
    fixed_effect_vars = args['fixed_effect_vars']
    data_type = args['data_type']
    scalar_var_str = '+'.join(scalar_vars)
    cat_var_str = '+'.join(list(map(lambda x: f'C({x})', cat_vars)))
    ind_var_str = '+'.join(list(filter(lambda x: x!='', [scalar_var_str, cat_var_str])))
    dep_var = 'has_light_verb'
    full_var_str = f'{dep_var}~{ind_var_str}'
    model_name_parts = [f'DATA={data_type}', f'PRED={full_var_str}']
    model_name = '_'.join(model_name_parts)
    if(data_type == 'loanword'):
        min_counts = [5,]*len(fixed_effect_vars)
    elif(data_type == 'native_verb'):
        min_count_lookup = {
            'screen_name' : 50,
            'native_word_type' : 5,
        }
        min_counts = list(map(min_count_lookup.get, fixed_effect_vars))
    filter_vars = scalar_vars + cat_vars
    data_file = args['data']
    data = load_clean_data(data_file, filter_vars=filter_vars, rare_control_vars=fixed_effect_vars, scalar_vars=scalar_vars, data_type=data_type, min_counts=min_counts)
    # clean up variables
    # get treatment values for fixed effect variables
    clean_fixed_effect_vars = list(map(lambda x: f'freq_{x}', fixed_effect_vars))
    fixed_effect_treatment_vals = []
    rare_val = 'RARE'
    for fixed_effect_var in clean_fixed_effect_vars:
        if(rare_val not in data.loc[:, fixed_effect_var].unique()):
            treatment_val = data.loc[:, fixed_effect_var].value_counts().index[0]
        else:
            treatment_val = rare_val
        fixed_effect_treatment_vals.append(treatment_val)
    combined_cat_var_treatment_vals = cat_var_treatment_vals + fixed_effect_treatment_vals
    combined_cat_vars = cat_vars + clean_fixed_effect_vars
    
    ## compute likelihood for each hyperparameter
    # convert to raw data to guarantee consistency in train/test
    np.random.seed(123)
    np.random.shuffle(data.values)
    X_raw_data, Y_raw_data = convert_to_raw_data(data, dep_var, scalar_vars, combined_cat_vars, combined_cat_var_treatment_vals)
    # split on train/test data
    N = data.shape[0]
    train_pct = 0.9
    N_train = int(N * train_pct)
    N_test = N - N_train
    X_train_data = X_raw_data[:N_train, :]
    X_test_data = X_raw_data[N_train:, :]
    Y_train_data = Y_raw_data[:N_train]
    Y_test_data = Y_raw_data[N_train:]
    log_likelihoods = []
    L2_weights = list(map(float, args['L2_weights']))
    for L2_weight in L2_weights:
        model = run_regularized_regression_raw_data(X_train_data, Y_train_data, l2_weight=L2_weight, max_iter=1000, verbose=False)
        log_likelihood = compute_log_likelihood(model.params, X_test_data, Y_test_data)
        # normalize by data size
        norm_log_likelihood = log_likelihood / N_test
        log_likelihoods.append(norm_log_likelihood)
    log_likelihoods = pd.DataFrame([L2_weights, log_likelihoods], index=['L2_weight', 'log_likelihood']).transpose()
    
    ## save to file
    out_dir = args['out_dir']
    out_file = os.path.join(out_dir, f'{model_name}_hyperparameter_likelihoods.tsv')
    log_likelihoods.to_csv(out_file, sep='\t', index=False)
    
if __name__ == '__main__':
    main()