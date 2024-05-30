import json
import sys
import os
from github import Github, RateLimitExceededException
from github.GithubException import GithubException


def initialize_github_client(access_token):
    """Initialize GitHubClient."""
    try:
        return Github(access_token)
    except GithubException as e:
        print(f"Error initializing GitHub client: {e}")
        raise


def get_organization_repos(github_client, organization_name):
    """Get all repositories."""
    try:
        org = github_client.get_organization(organization_name)
        return org.get_repos()
    except GithubException as e:
        print(f"Error fetching repositories for organization '{organization_name}': {e}")
        raise


def extract_pull_requests(repo, organization_name):
    """Get pull requests data from repository."""
    try:
        pulls = repo.get_pulls(state='all')
        pull_data = []
        for pull in pulls:
            pull_data.append({
                'organization_name': organization_name,
                'repository_id': repo.id,
                'repository_owner': repo.owner.login,
                'merged_at': pull.merged_at.isoformat() if pull.merged_at else None,
                'id': pull.id,
            })
        return pull_data
    except RateLimitExceededException:
        print("Rate limit exceeded. Exiting...")
        sys.exit(1)
    except GithubException as e:
        print(f"Error fetching pull requests for repository '{repo.name}': {e}")
        raise


def save_to_json(data, output_path):
    """Save data to JSON."""
    try:
        with open(output_path, 'w') as outfile:
            for entry in data:
                json.dump(entry, outfile)
                outfile.write('\n')
    except IOError as e:
        print(f"Error saving data to {output_path}: {e}")
        raise


def process_repository(repo, output_directory, organization_name):
    """Process a single repository."""
    try:
        print(f"Processing repository: {repo.name}")
        pull_data = extract_pull_requests(repo, organization_name)
        output_path = os.path.join(output_directory, f"{repo.name}_pull_requests.json")
        save_to_json(pull_data, output_path)
        print(f"Saved pull requests to {output_path}")
    except Exception as e:
        print(f"An error occurred while processing repository '{repo.name}': {e}")


def extract_and_save_pr_data(access_token, organization_name, output_directory):
    """Main function to fetch repository and pull request data to JSON"""
    try:
        github_client = initialize_github_client(access_token)
        repos = get_organization_repos(github_client, organization_name)

        if not os.path.exists(output_directory):
            os.makedirs(output_directory)

        for repo in repos:
            process_repository(repo, output_directory, organization_name)

    except RateLimitExceededException:
        print("Rate limit exceeded. Waiting for rate limit reset...")
        sys.exit(1)
    except Exception as e:
        print(f"An error occurred: {e}")
