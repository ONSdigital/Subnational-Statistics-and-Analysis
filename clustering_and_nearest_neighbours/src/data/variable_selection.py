from typing import Dict
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.metrics import pairwise_distances
from sklearn.decomposition import PCA


def get_correlation_matrix(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    

    Parameters
    ----------
    df : pd.DataFrame
        The data to be fed into the clustering model.

    Returns
    -------
    df_corr : pd.dataframe
        A correlation matrix for all selected variables

    """
    df = df.set_index("AREACD")
    df_corr = df.corr()
    return df_corr

def pca_analysis(
        loaded_config: Dict,
        df:pd.DataFrame,
        Area_code: str="AREACD",
        threshold: float=0.25,
        ):
        path=loaded_config["outputs_file_path"]
        df = df.set_index(Area_code)
        df.fillna(0, inplace=True)
        scaler = StandardScaler()
        df_scaled = scaler.fit_transform(df)
        
        pca = PCA()
        pca.fit(df_scaled)
        
        explained_variance = pca.explained_variance_ratio_
        
        plt.figure(figsize=(15, 6))
        plt.plot(range(1, len(explained_variance) + 1), explained_variance, marker='o')
        plt.title('Explained Variance by Principal Components')
        plt.xlabel('Principal Component')
        plt.ylabel('Explained Variance Ratio')
        plt.xticks(range(1, len(explained_variance) + 1))
        plt.grid()
        plt.savefig(f"{path}/Explained Variance by Principal Components.jpeg")
        pcs = pd.DataFrame(range(1, len(explained_variance) + 1), explained_variance)
        
        loadings = pca.components_.T
        loading_df = pd.DataFrame(loadings, index=df.columns, columns=[f'PC{i+1}' for i in range(loadings.shape[1])])
        
        important_features = loading_df[loading_df.abs() > threshold]
        
        return pcs, loading_df, important_features
    
    
def variance_analysis(
        loaded_config: Dict,
        df:pd.DataFrame,
        Area_code: str="AREACD",
                      ):
        path=loaded_config["outputs_file_path"]
        df = df.drop(Area_code, axis=1)
        scaler = MinMaxScaler()
        df_minmax_scaled = pd.DataFrame(scaler.fit_transform(df), columns=df.columns)
        variance = df_minmax_scaled.var()
        plt.figure(figsize=(10, 8))
        variance.plot(kind='barh', color='skyblue')
        plt.title('Variance of Min_max Scaled Features')
        plt.xlabel('Variance')
        plt.ylabel('Features')
        plt.savefig(f"{path}/Variance of Min_max Scaled Features.jpeg")
        return variance

def visualize_pairwise_distances(
        loaded_config: Dict,
        df:pd.DataFrame,
        Area_code: str="AREACD",
                      ):
        path=loaded_config["outputs_file_path"]
        df = df.set_index(Area_code)
        df.fillna(0, inplace=True)
        scaler = StandardScaler()
        df_scaled = scaler.fit_transform(df)
        distances = pairwise_distances(df_scaled, metric='euclidean')
        plt.figure(figsize=(10, 6))
        plt.hist(distances.flatten(), bins=50, color='blue', alpha=0.7)
        plt.title('Histogram of Pairwise Distances')
        plt.xlabel('Distance')
        plt.ylabel('Frequency')
        plt.savefig(f"{path}/Histogram of Pairwise Distances.jpeg")
        return distances

