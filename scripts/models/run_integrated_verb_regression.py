"""
Run regression on light verb use.
"""
from argparse import ArgumentParser
import logging
import os
import re
from model_helpers import load_clean_data, run_regularized_regression, fit_evaluate_lr_model, fit_evaluate_model, compute_VIF
import numpy as np
import pandas as pd

def run_regression(data, dep_var, scalar_vars, cat_vars, cat_var_treatment_lookup, fixed_effect_vars, interaction_var_pairs, l2_weight=0.1):
    """
    Run regression with all variables.
    """
    clean_fixed_effect_vars = list(map(lambda x: f'freq_{x}', fixed_effect_vars))
    for fixed_effect_var in clean_fixed_effect_vars:
        logging.info('%d unique vals for var=%s'%(data.loc[:, fixed_effect_var].nunique(), fixed_effect_var))
    combined_cat_vars = cat_vars + clean_fixed_effect_vars
    # get treatment val for fixed effect vars
    fixed_effect_treatment_vals = []
    rare_val = 'RARE'
    for fixed_effect_var in clean_fixed_effect_vars:
        if(rare_val not in data.loc[:, fixed_effect_var].unique()):
            treatment_val = data.loc[:, fixed_effect_var].value_counts().index[0]
        else:
            treatment_val = rare_val
#         fixed_effect_treatment_vals.append(treatment_val)
    # update cat var treatment with fixed effect treatment
        cat_var_treatment_lookup[fixed_effect_var] = treatment_val
    reg_scalar_vars = list(scalar_vars)
    reg_cat_vars = list(combined_cat_vars)
#     reg_cat_var_treatment_vals = list(cat_var_treatment_vals)
    # remove variables that are reference in interaction terms to avoid collinearity?
    # they will be included by default through interaction
    for var_1, var_2, var_type in interaction_var_pairs:
        combined_interaction_vars = [var_1, var_2]
        if(var_type=='categorical'):
            for var_i in combined_interaction_vars:
                match_idx_i = np.where(np.array(reg_cat_vars)==var_i)[0][0]
                reg_cat_vars.pop(match_idx_i)
#                 reg_cat_var_treatment_vals.pop(match_idx_i)
        elif(var_type=='scalar'):
            reg_scalar_vars = list(filter(lambda x: x not in combined_interaction_vars, reg_scalar_vars))
    max_iter = 100000
#     l2_weight = 0.1
    model, model_results = run_regularized_regression(data, dep_var, scalar_vars=reg_scalar_vars, cat_vars=reg_cat_vars, cat_var_treatment_lookup=cat_var_treatment_lookup, interaction_var_pairs=interaction_var_pairs, l2_weight=l2_weight, max_iter=max_iter, verbose=False)
    # remove fixed effect vars from output
    if(len(fixed_effect_vars) > 0):
        fixed_effect_var_matcher = re.compile('|'.join(clean_fixed_effect_vars))
        model_results = model_results.loc[list(filter(lambda x: fixed_effect_var_matcher.search(x) is None, model_results.index)), :]
    return model, model_results

def fix_cat_var_name(name):
    """
    Clean up categorical variable name.
    "C(description_location_region, Treatment("other"))[T.latin_america]" => "description_location_region==latin_america"
    """
    cat_var_matcher = re.compile('(?<=^C\()[a-zA-Z_0-9\-]+|(?<=:C\()[a-zA-Z_0-9\-]+')
    cat_var_val_matcher = re.compile('(?<=\[T\.)[a-zA-Z_0-9\-]+(?=\])|(?<=\[)[a-zA-Z_0-9\-]+(?=\])')
    # process all variables in name (in case interaction)
    var_str = []
    for name_i in name.split(':'):
        cat_var_match = cat_var_matcher.search(name_i)
        if(cat_var_match is not None):
            cat_var = cat_var_match.group(0)
            cat_var_val = cat_var_val_matcher.search(name_i).group(0)
            name_i = f'{cat_var}=={cat_var_val}'
        var_str.append(name_i)
    clean_name = ':'.join(var_str)
    return clean_name

def main():
    parser = ArgumentParser()
    parser.add_argument('data')
    parser.add_argument('--scalar_vars', nargs='+', default=[])
    parser.add_argument('--cat_vars', nargs='+', default=[])
    parser.add_argument('--cat_var_treatment_vals', nargs='+', default=[])
    parser.add_argument('--fixed_effect_vars', nargs='+', default=[])
    parser.add_argument('--interaction_vars', nargs='+', default=[])
    parser.add_argument('--data_filter_vars', nargs='+', default=[])
    parser.add_argument('--data_filter_var_ranges', nargs='+', default=[])
    parser.add_argument('--data_type', default='loanword')
    parser.add_argument('--out_dir', default='../../output/regression_results/')
    parser.add_argument('--model_type', default='logistic_regression')
    parser.add_argument('--l2_weight', type=float, default=0.1)
    parser.add_argument('--regression_name', default='')
    args = vars(parser.parse_args())
    ## specify model name for logging purposes UGH
    scalar_vars = args['scalar_vars']
    cat_vars = args['cat_vars']
    cat_var_treatment_vals = args['cat_var_treatment_vals']
    fixed_effect_vars = args['fixed_effect_vars']
    interaction_vars = args['interaction_vars']
    data_type = args['data_type']
    data_filter_vars = args['data_filter_vars']
    l2_weight = args['l2_weight']
    data_filter_var_ranges = list(map(lambda x: x.split(':'), args['data_filter_var_ranges']))
    # remove null vars
    scalar_vars = list(filter(lambda x: x!='', scalar_vars))
    cat_vars = list(filter(lambda x: x!='', cat_vars))
    cat_var_treatment_vals = list(filter(lambda x: x!='', cat_var_treatment_vals))
    cat_var_treatment_lookup = dict(zip(cat_vars, cat_var_treatment_vals))
    fixed_effect_vars = list(filter(lambda x: x!='', fixed_effect_vars))
    interaction_vars = list(filter(lambda x: x!='', interaction_vars))
    data_filter_vars = list(filter(lambda x: x!='', data_filter_vars))
    # get interaction var pairs
    interaction_var_pairs = list(map(lambda x: x.split('*'), interaction_vars))
    # get interaction var as str
    interaction_var_str = ''
    # TODO: specify treatment vals for interactions on categorical variables
    if(len(interaction_var_pairs) > 0):
        interaction_var_pair_str_combined = []
        for var_1, var_2, var_type in interaction_var_pairs:
            if(var_type=='categorical'):
                interaction_var_pair_str = f'{var_1}:{var_2}'
            elif(var_type=='scalar'):
                interaction_var_pair_str = f'{var_1}*{var_2}'
            interaction_var_pair_str_combined.append(interaction_var_pair_str)
        interaction_var_str = '+'.join(interaction_var_pair_str_combined)
    scalar_var_str = '+'.join(scalar_vars)
    cat_var_str = '+'.join(list(map(lambda x: f'C({x})', cat_vars)))
    ind_var_str = '+'.join(list(filter(lambda x: x!='', [scalar_var_str, cat_var_str, interaction_var_str])))
#     dep_var = 'has_light_verb'
    dep_var = 'has_integrated_verb'
    full_var_str = f'{dep_var}~{ind_var_str}'
    model_name_parts = [f'DATA={data_type}', f'PRED={full_var_str}', f'L2={l2_weight}']
    if(len(data_filter_vars) > 0):
        data_filter_var_str = ','.join(list(map(lambda x: '%s={%s:%s}'%(x[0], x[1][0], x[1][0]), zip(data_filter_vars, data_filter_var_ranges))))
        data_filter_var_str_full = f'FILTER={data_filter_var_str}'
        model_name_parts.append(data_filter_var_str_full)
    model_name = '_'.join(model_name_parts)
    logging_file = f'../../output/regression_results/regression_{model_name}.txt'
    # if file name is too long, use the back up name
    MAX_FILE_LEN=255
    if(len(logging_file) > MAX_FILE_LEN):
        model_name = args['regression_name']
        logging_file = f'../../output/regression_results/regression_{model_name}.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    ## load data
    data_file = args['data']
    # get interaction var pairs
    # specify min count for rare vals
    data_type = args['data_type']
    if('loanword' in data_type):
        min_counts = [5,]*len(fixed_effect_vars)
    elif('native_verb' in data_type):
        min_count_lookup = {
            'screen_name' : 50,
            'native_word_type' : 5,
        }
        min_counts = list(map(min_count_lookup.get, fixed_effect_vars))
    filter_vars = scalar_vars + cat_vars
    data = load_clean_data(data_file, filter_vars=filter_vars, rare_control_vars=fixed_effect_vars, scalar_vars=scalar_vars, cat_vars=cat_vars, data_type=data_type, min_counts=min_counts)
    # sanity check: categorical distributions
#     for cat_var in cat_vars:
#         print(data.loc[:, cat_var].value_counts())
    logging.info('extracted %d data'%(data.shape[0]))
    # optional: filter data by value range
    # filtering is inclusive (i.e. [min, max])
    if(len(data_filter_vars) > 0):
        # range format: "MIN_VAL:MAX_VAL" (scalar) OR "VAL1:VAL2" (categorical)
        for data_filter_var, data_filter_var_range in zip(data_filter_vars, data_filter_var_ranges):
            # tmp debugging
#             print(f'filter var={data_filter_var} range={data_filter_var_range}')
            if(data_filter_var in cat_vars):
                data = data[data.loc[:, data_filter_var].isin(data_filter_var_range)]
                print('filter var %s has counts %s'%(data_filter_var, data.loc[:, data_filter_var].value_counts()))
                # remove filter variables from categorical variables 
                # or else we'll end up with another intercept term (e.g. all samples have language=spanish_high)
                if(len(data_filter_var_range) == 1):
                    cat_var_idx = np.where(np.array(cat_vars)==data_filter_var)[0][0]
                    cat_vars.pop(cat_var_idx)
#                     cat_var_treatment_vals.pop(cat_var_idx)
            elif(data_filter_var in scalar_vars):
                data_filter_var_min, data_filter_var_max = list(map(float, data_filter_var_range))
                data = data[(data.loc[:, data_filter_var] >= data_filter_var_min) & 
                            (data.loc[:, data_filter_var] <= data_filter_var_max)]
    logging.info('filtered data (N=%d)'%(data.shape[0]))
    # tmp debugging
#     bin_vars = ['es_bin', 'latin_american_media_pct_bin']
#     for bin_var in bin_vars:
#         print(data.loc[:, bin_var].value_counts())
    
    ## TODO: output 0: data distributions
    out_dir = args['out_dir']
    data_dist_out_file = os.path.join(out_dir, f'{model_name}_data.tsv')
#     with open(data_dist_out_file, 'w') as data_dist_out:
    scalar_var_mean = data.loc[:, scalar_vars].mean(axis=0)
    scalar_var_sd = data.loc[:, scalar_vars].std(axis=0)
    scalar_var_stats = pd.concat([scalar_var_mean, scalar_var_sd], axis=1).reset_index().rename(columns={'index' : 'var_name', 0 : 'mean', 1 : 'SD'})
    cat_var_stats = pd.concat(list(map(lambda x: data.loc[:, x].value_counts().reset_index().rename(columns={x:'var_count', 'index':'var_val'}).assign(**{'var_name':x}), cat_vars)))
    cat_var_stats = cat_var_stats.assign(**{'var_count' : cat_var_stats.loc[:, 'var_count'].astype(int)})
    var_stats = pd.concat([scalar_var_stats, cat_var_stats], axis=0)
    var_stats.fillna('', inplace=True)
    var_stats.to_csv(data_dist_out_file, sep='\t')
    
    ## output 1: regression coefficients
    # fit model on all data
    dep_var = 'has_integrated_verb'
    # tmp debugging
#     print('dep var %s has values %s'%(dep_var, data.loc[:, dep_var].value_counts()))
    l2_weight = args['l2_weight']
    model, model_results = run_regression(data, dep_var, scalar_vars, cat_vars, cat_var_treatment_lookup, fixed_effect_vars, interaction_var_pairs, l2_weight=l2_weight)
    # add p significance using Bonferroni correction: dumb but simple
    coef_count = len(model.params)-1
    p_val_cutoffs = np.array([0.001, 0.01, 0.05])
    p_val_cutoffs = p_val_cutoffs / coef_count
    p_val_cutoff_str = {0: '***', 1 : '**', 2 : '*', 3 : '-'}
    model_results = model_results.assign(**{
        'p_significant' : list(map(p_val_cutoff_str.get, np.digitize(model_results.loc[:, 'p'].values, p_val_cutoffs)))
    })
    # write coefficients and summary to file
    out_dir = args['out_dir']
    model_coef_out_file = os.path.join(out_dir, f'{model_name}_coef.tsv')
    model_results.to_csv(model_coef_out_file, sep='\t', index=True)
    
    ## output 2: cross-validation accuracy
    # leave out fixed effects for better generalization? yes
    # logistic regression => lame
    model_type = args['model_type']
    k_fold = 10
    if(model_type == 'logistic_regression'):
        pred_model_type = model_type
        reg_format_cat_vars = list(map(lambda x: f'C({x})', cat_vars))
        ind_vars = scalar_vars + reg_format_cat_vars
        max_iter = 1000
        full_model_results, pred_acc = fit_evaluate_lr_model(data, ind_vars, dep_var, balance=True, k=k_fold, max_iter=max_iter)
    else:
        scalar_var_str = '+'.join(scalar_vars)
        cat_var_str = '+'.join(cat_vars)
        ind_var_str = '+'.join(list(filter(lambda x: x!='', [scalar_var_str, cat_var_str])))
        full_var_str = f'{dep_var}~{ind_var_str}'
        pred_model_type = f'DATA={data_type}_PRED={full_var_str}'
        model_param_lookup = {
            'random_forest' : {'n_estimators' : 100}
        }
        model_params = model_param_lookup[model_type]
#         print(f'cat vars = {cat_vars}')
        pred_acc = fit_evaluate_model(data, scalar_vars, cat_vars, dep_var, model_type=model_type, k=k_fold, model_params=model_params, balance=True)
    # add mean, std to pred accuracy
    pred_acc.loc['acc_mean'] = pred_acc.iloc[:k_fold].mean()
    pred_acc.loc['acc_sd'] = pred_acc.iloc[:k_fold].std()
    # write accuracy to file
    model_acc_out_file = os.path.join(out_dir, f'MODEL={model_type}_{pred_model_type}_acc.tsv')
    # change directories if name too long
    if(len(model_acc_out_file) > MAX_FILE_LEN):
        regression_name = args['regression_name']
        model_acc_out_file = os.path.join(out_dir, f'{regression_name}_acc.tsv')
    pred_acc.to_csv(model_acc_out_file, sep='\t')
#     MAX_FILE_NAME_LEN = 255
#     if(len(model_acc_out_file) > MAX_FILE_NAME_LEN):
#         script_dir = os.getcwd()
#         os.chdir(out_dir)
#         model_acc_out_file = f'MODEL={model_type}_{pred_model_type}_acc.tsv' # 
#         pred_acc.to_csv(model_acc_out_file, sep='\t')
#         os.chdir(script_dir)
    
    ## output 3: model diagnostic
    # VIF: do any of the variables contribute to collinearity?
    diagnosis_vars = model_results.index
    vif_data = []
    for diagnosis_var in diagnosis_vars:
        vif = compute_VIF(model, diagnosis_var)
        vif_data.append([diagnosis_var, vif])
    vif_data = pd.DataFrame(vif_data, columns=['var_name', 'VIF'])
    vif_out_file = os.path.join(out_dir, f'{model_name}_vif.tsv')
    vif_data.to_csv(vif_out_file, sep='\t')
    
    ## output 4: pretty print everything for easier copying
    # variable count/mean, coefficient, SE, p-val, VIF
    var_stats = var_stats.assign(**{
        'var_count_combined' : var_stats.apply(lambda x: x.loc['mean'] if x.loc['mean']!='' else x.loc['var_count'], axis=1)
    })
    model_results = model_results.assign(**{
        'var_name' : model_results.index
    })
    model_results.rename(columns={'mean' : 'coef'}, inplace=True)
#     # fix categorical variable names
    cat_var_format = lambda x,y: f'{x}=={y}'
    var_stats = var_stats.assign(**{
        'var_name' : var_stats.apply(lambda x: cat_var_format(x.loc['var_name'], x.loc['var_val']) if x.loc['var_val']!='' else x.loc['var_name'], axis=1)
    })
    model_results = model_results.assign(**{
        'var_name' : model_results.loc[:, 'var_name'].apply(lambda x: fix_cat_var_name(x))
    })
    vif_data = vif_data.assign(**{
        'var_name' : vif_data.loc[:, 'var_name'].apply(lambda x: fix_cat_var_name(x))
    })
#     print(var_stats.head())
#     print(model_results.head())
    combined_stats = pd.merge(var_stats, model_results, on='var_name')
#     print(vif_data.head())
    combined_stats = pd.merge(combined_stats, vif_data, on='var_name')
    combined_stat_cols = ['var_name', 'var_count_combined', 'coef', 'SE', 'p', 'p_significant', 'VIF']
    clean_combined_stats = combined_stats.loc[:, combined_stat_cols]
    combined_stat_out_file = os.path.join(out_dir, f'{model_name}_full_stats.tsv')
    clean_combined_stats.to_csv(combined_stat_out_file, sep='\t', index=False)
    ## also write tex table
    mini_stat_cols = ['var_name', 'coef', 'SE', 'p_significant']
    mini_combined_stats = combined_stats.loc[:, mini_stat_cols]
    mini_combined_stat_out_file = os.path.join(out_dir, f'{model_name}_mini_stats.tex')
    mini_combined_stats.to_latex(mini_combined_stat_out_file, index=False)
    
if __name__ == '__main__':
    main()