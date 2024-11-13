################################
## github_pipeline.py
##
import dlt
from github import github_reactions, github_repo_events, github_stargazers

def load_dlthub_dlt_all_data() -> None:
    """Loads all issues, pull requests and comments for dlthub dlt repo"""
    pipeline = dlt.pipeline(
        "github_reactions",
        destination='duckdb',
        dataset_name="dlthub_reactions",
        dev_mode=False,
    )
    data = github_reactions("dlt-hub", "dlt")
    print(pipeline.run(data))

if __name__ == "__main__":
    load_dlthub_dlt_all_data()






