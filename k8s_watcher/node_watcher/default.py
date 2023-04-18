

EXTRA_NODES_DF_COLUMNS = ['spine', 'leaf', 'current_category']


def get_extra_columns(df):
    df['spine'] = None
    df['leaf'] = None
    df['cluster'] = 'default_cluster'
    df['current_category'] = 'err'
    df.loc[df.use.str.startswith('dev', na=False), 'current_category'] = 'dev'
    df.loc[df.use.str.startswith('service', na=False), 'current_category'] = 'service'
    df.loc[df.use.str.startswith('training', na=False), 'current_category'] = 'training'
    df.loc[df.group.str.contains('_dedicated', na=False).astype(bool), 'current_category'] = 'exclusive'
    return df
