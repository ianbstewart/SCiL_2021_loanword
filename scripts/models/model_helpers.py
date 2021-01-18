"""
Helper methods for modeling.
"""
import numpy as np
from statsmodels.discrete.discrete_model import Logit
from statsmodels.stats.outliers_influence import variance_inflation_factor
from statsmodels.api import GLM
from statsmodels.formula.api import glm
import statsmodels.formula.api
from statsmodels.genmod.families.family import Binomial
from statsmodels.genmod.families.links import logit
from sklearn.model_selection import KFold
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from scipy.sparse import csr_matrix, diags, linalg
from scipy.stats import norm, chi
from datetime import datetime
import pandas as pd
import re
import logging

def get_regression_output(results):
    """
    Organize regression output parameters.
    """
    pvals = results.pvalues
    coeff = results.params
    conf_lower = results.conf_int()[0]
    conf_higher = results.conf_int()[1]

    results_df = pd.DataFrame({"p":pvals,
                               "coeff":coeff,
                               "CI_low":conf_lower,
                               "CI_high":conf_higher
                                })
    results_df = results_df.loc[:, ["coeff","p","CI_low","CI_high"]]
    return results_df

def bin_data_var(data, bin_ranges, bin_names, bin_var='es'):
    bin_vals = list(map(bin_names.get, np.digitize(data.loc[:, bin_var], bin_ranges)))
    data = data.assign(**{
        f'{bin_var}_bin' : bin_vals
    })
    return data

def filter_valid_data(data, filter_vars=['es_bin', 'latin_american_media_pct', 'description_location_region', 'integrated_verb_pct'], scalar_vars = ['latin_american_media_pct', 'integrated_verb_pct']):
    valid_data = data.copy()
    for var_name in filter_vars:
#         print(f'filtering {var_name}')
        valid_data = valid_data[valid_data.loc[:, var_name] != '']
#         print(valid_data.loc[:, 'es_bin'].value_counts())
    # fix scalar vars
    for scalar_var in scalar_vars:
        valid_data = valid_data.assign(**{
            scalar_var : valid_data.loc[:, scalar_var].astype(float)
        })
    return valid_data

def label_rare_data_var(data, data_var='screen_name', min_count=5):
    """
    Label the long tail of variable values with RARE.
    """
    data_var_counts = data.loc[:, data_var].value_counts()
    freq_data_vals = set(data_var_counts[data_var_counts >= min_count].index.tolist())
    freq_data_var = f'freq_{data_var}'
    data = data.assign(**{
        freq_data_var : data.loc[:, data_var].apply(lambda x: x if x in freq_data_vals else 'RARE')
    })
    return data

def load_clean_data(data_file, rare_control_vars=['loanword_verb', 'screen_name'], filter_vars=['es_bin', 'latin_american_media_pct', 'description_location_region', 'integrated_verb_pct'], scalar_vars=['latin_american_media_pct', 'integrated_verb_pct'], cat_vars=[], data_type='loanword', min_counts=[5]):
    """
    Load regression data and clean:
    - bin scalar vars: lang, media
    - normalize scalar values
    """
    data = pd.read_csv(data_file, sep='\t')
    # cleanup
    data.fillna('', inplace=True)
    # optional: add random value for baseline
    if('rand' in scalar_vars):
        data = data.assign(**{
            'rand' : np.random.randn(data.shape[0])
        })
    # filter to valid data
    data = filter_valid_data(data, filter_vars=filter_vars, scalar_vars=scalar_vars)
    # add RARE for rare control vars
    for control_var, min_count in zip(rare_control_vars, min_counts):
        data = label_rare_data_var(data, data_var=control_var, min_count=min_count)
    # normalize scalar values
#     log_vars = ['integrated_verb_pct', 'latin_american_media_pct', 'es', 'post_pct', 'URL_share_pct', 'RT_pct']
    for scalar_var in scalar_vars:
#             data = data.assign(**{
#                 f'log_{log_var}' : data.loc[:, log_var].apply(lambda x: np.log(x) if x!='' else x)
#             })
        var_vals = data[data.loc[:, scalar_var] != ''].loc[:, scalar_var].values
        var_mu = np.mean(var_vals)
        var_sigma = np.std(var_vals)
        data = data.assign(**{
            scalar_var : data.loc[:, scalar_var].apply(lambda x: (x-var_mu)/var_sigma if x!='' else x)
        })
    # fix cat vars
    int_var_types = [int, np.int64]
    for cat_var in cat_vars:
        cat_var_type = data.loc[:, cat_var].dtype.type
        if(cat_var_type in int_var_types):
            data = data.assign(**{
                cat_var : data.loc[:, cat_var].astype(str)
            })
    # add dependent var
    dep_var = 'has_light_verb'
    if(data_type == 'loanword'):
        data = data.assign(**{
            dep_var : (data.loc[:, 'loanword_type']=='light_verb_loanword').astype(int)
        })
    elif(data_type == 'native_verb'):
        data = data.assign(**{
            dep_var : (data.loc[:, 'native_word_category']=='native_light_verb').astype(int)
        })
    return data

## actual modeling

def run_logistic_regression(data, dep_var, scalar_vars=[], cat_vars=[], cat_var_treatment_vals=[], max_iter=100, verbose=False):
    """
    Run basic logistic regression.
    """
    scalar_var_str = ' + '.join(scalar_vars)
    cat_var_str = ' + '.join(list(map(lambda x: 'C(%s, Treatment("%s"))'%(x[0], x[1]), zip(cat_vars, cat_var_treatment_vals))))
    ## TODO: interaction?
    var_str_list = list(filter(lambda x: x != '', [scalar_var_str, cat_var_str]))
    independent_var_str = ' + '.join(var_str_list)
    formula = f'{dep_var} ~ {independent_var_str}'
    if(verbose):
        print(f'formula = {formula}')
    model = Logit.from_formula(formula, data=data)
    fit_model = model.fit(method='bfgs', maxiter=max_iter)
    # convert to useful data
    fit_model_output = get_regression_output(fit_model)
    return fit_model, fit_model_output

## regularized regression => reduce fixed effect coefficient bias
# compute err matrix
def compute_err_data(model_results):
    """
    Compute error data for regularized regression.
    """
    exog_names = model_results.model.exog_names
    design_mat = model_results.model.exog
    pred_probs = model_results.model.predict(model_results.params)
    # need sparse matrix! to avoid memory explosion
    prob_mat = diags(pred_probs, 0).tocsr()
    design_mat = csr_matrix(design_mat)
    cov_mat = linalg.inv(design_mat.T.dot(prob_mat).dot(design_mat))
    param_err = np.sqrt(np.diag(cov_mat.todense()))
    model_err_data = pd.DataFrame(model_results.params, columns=['mean'])
    model_err_data = model_err_data.assign(**{'SE' : param_err})
    # compute test stat, p-val for two-sided test
    # https://stats.stackexchange.com/questions/60074/wald-test-for-logistic-regression
    model_err_data = model_err_data.assign(**{'Z' : model_err_data.loc[:, 'mean'] / model_err_data.loc[:, 'SE']})
    # use Wald test
    model_err_data = model_err_data.assign(**{'p' : model_err_data.loc[:, 'Z'].apply(lambda x: 1-chi.cdf(x**2, 1))})
    # confidence intervals
    alpha = 0.05
    Z_alpha = norm.ppf(1-alpha/2)
    model_err_data = model_err_data.assign(**{'CI_lower' : model_err_data.loc[:, 'mean'] - Z_alpha*model_err_data.loc[:, 'SE']})
    model_err_data = model_err_data.assign(**{'CI_upper' : model_err_data.loc[:, 'mean'] + Z_alpha*model_err_data.loc[:, 'SE']})
    return model_err_data

def normalize_cat_vars_from_model(model):
    """
    Normalize categorical variables from the model.
    """
    model_exog_data = pd.DataFrame(model.exog, columns=model.exog_names)
    cat_var_matcher = re.compile('C\(.+\)')
    model_exog_cat_params = list(filter(lambda x: cat_var_matcher.search(x) is not None, model.exog_names))
    for model_exog_cat_param in model_exog_cat_params:
        scaler = StandardScaler()
        model_exog_data = model_exog_data.assign(**{model_exog_cat_param : scaler.fit_transform(model_exog_data.loc[:, model_exog_cat_param].values.reshape(-1,1))})
    model.exog = model_exog_data.values
    return model

# regularized regression: elastic net
def run_regularized_regression(data, dep_var, scalar_vars=[], cat_vars=[], cat_var_treatment_lookup={}, interaction_var_pairs=[], l2_weight=0.1, max_iter=100, verbose=False):
    """
    Run regularized regression on data.
    """
    scalar_var_str = ' + '.join(scalar_vars)
    cat_var_str = ' + '.join(list(map(lambda x: 'C(%s, Treatment("%s"))'%(x, cat_var_treatment_lookup[x]), cat_vars)))
    # add interaction
    interaction_var_str = ''
    if(len(interaction_var_pairs) > 0):
        interaction_var_pairs_str = []
        for var_1, var_2, var_type in interaction_var_pairs:
            var_1_treatment = cat_var_treatment_lookup[var_1]
            var_2_treatment = cat_var_treatment_lookup[var_2]
            if(var_type == 'categorical'):
                interaction_var_pairs_str.append(f'C({var_1}, Treatment("{var_1_treatment}")):C({var_2}, Treatment("{var_2_treatment}"))')
            elif(var_type == 'scalar'):
                interaction_var_pairs_str.append(f'{var_1}*{var_2}')
        interaction_var_str = ' + '.join(interaction_var_pairs_str)
    var_str_list = list(filter(lambda x: x != '', [scalar_var_str, cat_var_str, interaction_var_str]))
    independent_var_str = ' + '.join(var_str_list)
    formula = f'{dep_var} ~ {independent_var_str}'
#     print(formula)
    model = GLM.from_formula(formula, data, family=Binomial(link=logit()))
    # normalize categorical variables
    # actually: don't do this because it's hard to interpret standard deviations from 0
#     model = normalize_cat_vars_from_model(model)
    fit_model = model.fit_regularized(maxiter=max_iter, method='elastic_net', alpha=l2_weight, L1_wt=0.0)
#     return fit_model
    fit_model_results = compute_err_data(fit_model)
    return fit_model, fit_model_results

def run_regularized_regression_raw_data(X_data, Y_data, l2_weight=0.1, max_iter=100, verbose=False):
    """
    Run regularized regression on data 
    with raw data.
    """
    model = GLM(endog=Y_data, exog=X_data, family=Binomial(link=logit()))
    # normalize categorical variables
    # actually: don't do this because it's hard to interpret standard deviations from 0
#     model = normalize_cat_vars_from_model(model)
    fit_model = model.fit_regularized(maxiter=max_iter, method='elastic_net', alpha=l2_weight, L1_wt=0.0)
#     return fit_model
#     fit_model_results = compute_err_data(fit_model)
    return fit_model

# model diagnostics

def compute_VIF(model_results, var_name):
    """
    Compute variance inflation factor for a given variable.
    https://stats.idre.ucla.edu/stata/webbooks/reg/chapter2/stata-webbooksregressionwith-statachapter-2-regression-diagnostics/
    """
    exog = model_results.model.exog
    var_idx = np.where(model_results.params.index == var_name)[0][0]
    VIF = variance_inflation_factor(exog, var_idx)
    return VIF
def clean_var_name(x):
    """
    Clean variable name, as produced by statsmodels regression.
    """
    return x.replace('-','_').replace('/', '_').replace("'", '_')

def fit_evaluate_lr_model(data, ind_vars, dep_var, test=0.1, k=10, max_iter=1000, balance=False):
    """
    Fit and evaluate LR model based on ability
    to predict dep_var. 
    We are interested in (1) predictive power and (2) deviance from null model.
    
    :param data: prediction data
    :param ind_vars: independent vars
    :param dep_var: dependent var
    :param test: test percent
    :param k: k_fold classification count
    :param max_iter: maximum number of iterations for model convergence
    :param balance: balance data by dependent variable class
    """
    np.random.seed(123)
    formula = '%s ~ %s'%(dep_var, ' + '.join(ind_vars))
    logging.info('formula: %s'%(formula))
#     print(data.loc[:, 'NE_fixed'].head())
    ## regular fit/statistics
    model = glm(formula=formula, data=data, family=Binomial(logit()))
    model_results = model.fit()
    logging.info('model summary:')
    logging.info(model_results.summary())
    # balance data by class
    if(balance):
        data.loc[:, dep_var] = data.loc[:, dep_var].astype(int)
        dep_var_counts = data.loc[:, dep_var].value_counts()
        N_min_class = dep_var_counts.iloc[-1]
        data_balanced = pd.concat([data_c.loc[np.random.choice(data_c.index, N_min_class, replace=False), :] for c, data_c in data.groupby(dep_var)], axis=0)
        data = data_balanced.copy()
#     print(data.loc[:, 'NE_fixed'].head())
    
    ## k-fold cross validation
    # convert categorical vars to usable format
    reg_data = data.copy()
    cat_var_matcher = re.compile('C\((.+)\)')
    ind_vars_cat = [cat_var_matcher.search(x).group(1) for x in ind_vars if cat_var_matcher.search(x) is not None]
    if(len(ind_vars_cat) > 0):
        ind_var_cat_vals = []
    #     print(reg_data.loc[:, ind_vars_cat].head())
        for ind_var_cat in ind_vars_cat:
            ind_var_unique_vals = list(reg_data.loc[:, ind_var_cat].unique())
    #             print(unique_val)
            reg_data = reg_data.assign(**{clean_var_name(x):(reg_data.loc[:, ind_var_cat]==x).astype(int) for x in ind_var_unique_vals})
            # fix bad strings
            ind_var_unique_vals = [clean_var_name(x) for x in ind_var_unique_vals]
            ind_var_cat_vals += ind_var_unique_vals
            reg_data.drop(ind_var_cat, axis=1, inplace=True)
    #     print('data cols %s'%(str(reg_data.columns)))
        ind_vars_full = (set(ind_vars) - set(['C(%s)'%(x) for x in ind_vars_cat])) | set(ind_var_cat_vals)
        formula_full = '%s ~ %s'%(dep_var, ' + '.join(ind_vars_full))
    else:
        formula_full = '%s ~ %s'%(dep_var, ' + '.join(ind_vars))
    logging.info(f'full formula for prediction = {formula_full}')
    kfold = KFold(n_splits=k, shuffle=True)
    predict_acc = []
    reg_data.loc[:, dep_var] = reg_data.loc[:, dep_var].astype(int)
    for train_idx, test_idx in kfold.split(reg_data):
        data_train = reg_data.iloc[train_idx, :]
        data_test = reg_data.iloc[test_idx, :]
#         print('train data %s'%(str(data_train.columns)))
        model_i = statsmodels.formula.api.logit(formula=formula_full, data=data_train)
#         model_i = logit(endog=train_data.loc[:, dep_var], exog=train_data.loc[:, ind_vars])
        model_i_results = model_i.fit(full_output=False, disp=True, maxiter=max_iter)
        model_i_results.predict(data_test)
        pred_vals_i = np.array([int(x > 0.5) for x in model_i_results.predict(data_test)])
        y = data_test.loc[:, dep_var].astype(int)
#         predict_results_i = 1 - ((y - pred_vals_i) / len(y))
        predict_results_i = (y == pred_vals_i)
        predict_acc_i = np.mean(predict_results_i)
        predict_acc.append(predict_acc_i)
    predict_acc = pd.Series(predict_acc)
    return model_results, predict_acc

def fit_evaluate_model(data, scalar_vars, cat_vars, dep_var, model_type='random_forest', test=0.1, k=10, balance=False, model_params={}):
    """
    Fit and evaluate arbitrary model for predicting
    binary dependent variable.
    """
    np.random.seed(123)
    # balance data by class
    if(balance):
        data.loc[:, dep_var] = data.loc[:, dep_var].astype(int)
        dep_var_counts = data.loc[:, dep_var].value_counts()
        N_min_class = dep_var_counts.iloc[-1]
        data_balanced = pd.concat([data_c.loc[np.random.choice(data_c.index, N_min_class, replace=False), :] for c, data_c in data.groupby(dep_var)], axis=0)
        data = data_balanced.copy()
#     print(data.loc[:, 'NE_fixed'].head())
    
    ## k-fold cross validation
    # convert categorical vars to dummies
    reg_data = []
    clean_cat_vars = []
    if(len(cat_vars) > 0):
        for cat_var in cat_vars:
            dummy_data = pd.get_dummies(data.loc[:, cat_var])
            clean_cat_vars += list(dummy_data.columns)
            reg_data.append(dummy_data)
        reg_data = pd.concat(reg_data, axis=1)
    else:
        reg_data = pd.DataFrame()
    reg_data = pd.concat([reg_data, data.loc[:, scalar_vars+[dep_var]]], axis=1)
    ind_vars = clean_cat_vars + scalar_vars
    kfold = KFold(n_splits=k, shuffle=True)
    predict_acc = []
    reg_data.loc[:, dep_var] = reg_data.loc[:, dep_var].astype(int)
    for train_idx, test_idx in kfold.split(reg_data):
        data_train = reg_data.iloc[train_idx, :]
        data_test = reg_data.iloc[test_idx, :]
        if(model_type == 'random_forest'):
            model_i = RandomForestClassifier(**model_params)
        model_i.fit(data_train.loc[:, ind_vars], data_train.loc[:, dep_var])
        pred_vals_i = model_i.predict(data_test.loc[:, ind_vars])
        y = data_test.loc[:, dep_var].astype(int)
        predict_results_i = (y == pred_vals_i)
        predict_acc_i = np.mean(predict_results_i)
        predict_acc.append(predict_acc_i)
    predict_acc = pd.Series(predict_acc)
    return predict_acc

## integration rate methods
def compute_integrated_rate(data):
    integrated_rate = data.loc[:, 'integrated_verb'].sum() / data.loc[:, ['integrated_verb', 'light_verb']].sum(axis=1).sum()
    return integrated_rate

def load_verb_count_data(data_file):
    """
    Load verb count data extracted from 
    archive files.
    """
    data = pd.read_csv(data_file, sep=',', header=None)
    data.columns= ['loanword', 'integrated_verb', 'light_verb', 'file_name']
    data.dropna(axis=0, inplace=True)
    # get date
    date_fmt = '%b-%d-%y'
    data = data.assign(**{
        'date' : data.loc[:, 'file_name'].apply(lambda x: datetime.strptime('-'.join(x.split('-')[1:4]), date_fmt))
    })
    # get year for more coarse-grained analysis
    data = data.assign(**{
        'date_year' : data.loc[:, 'date'].apply(lambda x: x.year)
    })
    return data