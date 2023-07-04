def variable_to_name(var):
    """Helper function with takes in a variable and return a string of that variables name.
    If the variable doesn't exist, will return null.
    Might go wrong if there are multiple declared variables with identical values!
    """
    for name in globals():
        if (eval(name) is var) and (not name.startswith("_")): #Omit variables in globals with begin with _, as __ is shorthand for last evaluated result and in jupyter notebooks _1 return output of cell evaluation 1 etc.
            return(name)

#Combine various clusters into a single data frame.
def combine_clusters(sets, labels, cluster_col='Cluster'):
    #Get the column labels from the names of the clusters
    #labels = [variable_to_name(sets[i]) for i in range(len(sets))]
    left = sets[0]
    right = sets[1]
    combined = left[['AREACD',cluster_col]].merge(right[['AREACD',cluster_col]], on='AREACD')
    for i in range(2,len(sets)):
        right = sets[i]
        combined = combined.merge(right[['AREACD',cluster_col]], on='AREACD')
    labels.insert(0, 'AREACD')
    combined.columns = [labels]        
    combined.columns = combined.columns.get_level_values(0)
    return(combined)    
    
#Function to generate bubble plots -- scatters with the point size given by number obs.
def plot_bubbles(x, y, clusters, normalise=False):
    """USAGE: This will produce a bubble plot looking at the correspondance of two sets of clusters.
    The inputs required are x and y, the names of the cluster groups.
    """
    import pandas as pd
    weights=pd.DataFrame(clusters[[x,y]].value_counts())
    weights=weights.reset_index()
    weights.columns=[x,y,'count']
    if normalise:
        weights2=pd.DataFrame(clusters[[x]].value_counts())
        weights2=weights2.reset_index()
        weights2.columns=[x,'total']
        weights = weights.merge(weights2, on=x)
        weights['weight']=weights['count']/weights['total']
    else:
        weights['weight']=weights['count']
    weights['weight']/=max(weights['weight'])*0.005
    weights.plot.scatter(x, y, s='weight', c='count', cmap='viridis')
