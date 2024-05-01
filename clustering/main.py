import yaml
from yaml.loader import SafeLoader

from clustering_refactor.pipeline import pipeline


def run(
    config_path: str,
):
    """Reads config file and runs pipeline.

    Parameters
    ----------
    config_path
        Contains the path to the config to load.

    Returns
    -------
    None
    """
    with open(config_path, encoding="utf-8") as f:
        loaded_config = yaml.load(f, Loader=SafeLoader)

    pipeline(
        loaded_config=loaded_config,
    )

    return


if __name__ == '__main__':

    config_path = "config.yaml"
    run(config_path=config_path)
