"""
This module provides utilities for deploying Python wheel files as custom libraries to Microsoft Fabric environments via the Fabric REST API.

It includes functions to:
- Authenticate and obtain an API token using Azure AD client credentials.
- Interact with Fabric environments to query state, upload, delete, and publish custom libraries.
- Manage the deployment lifecycle, including cancelling ongoing publishes, waiting for publish completion, and handling errors.

Typical usage involves setting required environment variables for authentication and deployment targets, then running the script to deploy a specified wheel file to a Fabric environment.

Functions:
    _get_fabric_api_token(client_id: str, client_secret: str, tenant_id: str) -> str

    _fabric_api_request(request_type: str, token: str, request_url: str, files: dict | None = None, headers: dict | None = None, max_retries: int = 3) -> str

    _get_fabric_environment_state(token: str, workspace_id: str, environment_id: str) -> str

    _get_fabric_environment_custom_libraries(token: str, workspace_id: str, environment_id: str) -> dict

    _delete_fabric_environment_custom_library(token: str, workspace_id: str, environment_id: str, library_name: str) -> dict

    _upload_fabric_environment_custom_library(token: str, workspace_id: str, environment_id: str, file_path: str) -> dict
        Uploads a wheel file as a custom library to a Fabric environment.

    _delete_fabric_environment_published_custom_libraries(token: str, workspace_id: str, environment_id: str) -> None
        Deletes all published custom libraries from a Fabric environment.

    _cancel_fabric_environment_publish(token: str, workspace_id: str, environment_id: str) -> None

    _publish_fabric_environment(token: str, workspace_id: str, environment_id: str) -> None

    _is_fabric_environment_published(token: str, workspace_id: str, environment_id: str, allow_cancelled: bool = False) -> bool

    _wait_until_fabric_environment_publish_finished(token: str, workspace_id: str, environment_id: str, allow_cancelled: bool = False, timeout_in_minutes: int = 40) -> bool

    run_wheel_deployment_to_fabric(token: str, workspace_id: str, environment_id: str, file_path: str) -> None

Example:
    Set the following environment variables before running the script:
        FABRIC_CLIENT_ID, FABRIC_CLIENT_SECRET, FABRIC_TENANT_ID,
        FABRIC_WORKSPACE_ID, FABRIC_ENVIRONMENT_ID, FABRIC_FILE_PATH

    Then execute the script to deploy the wheel file to the Fabric environment.

    Exception: If any step in the deployment process fails.
    TimeoutError: If publishing the environment exceeds the specified timeout.
    ValueError: If required environment variables are not set.
"""
import os
import time
import urllib.parse
from pathlib import Path

import requests
from azure.identity import ClientSecretCredential


def _get_fabric_api_token(client_id: str, client_secret: str, tenant_id: str) -> str:
    """
    Obtains an access token for the Microsoft Fabric API using client credentials.

    Args:
        client_id (str): The client ID of the Azure AD application.
        client_secret (str): The client secret of the Azure AD application.
        tenant_id (str): The Azure AD tenant ID.

    Returns:
        str: The access token for authenticating with the Microsoft Fabric API.
    """
    token_credential = ClientSecretCredential(
        client_id=client_id, client_secret=client_secret, tenant_id=tenant_id)
    token = token_credential.get_token(
        "https://api.fabric.microsoft.com/.default")
    return token.token


def _fabric_api_request(request_type: str, token: str, request_url: str, files: dict | None = None, headers: dict | None = None, max_retries: int = 3) -> str:
    """
    Sends an HTTP request to the Microsoft Fabric API with optional retries.

    Args:
        request_type (str): The HTTP method to use (e.g., 'GET', 'POST').
        token (str): Bearer token for API authentication.
        request_url (str): The endpoint path or URL to send the request to.
        files (dict | None, optional): Files to include in the request (for multipart/form-data). Defaults to None.
        headers (dict | None, optional): Additional headers to include in the request. Defaults to None.
        max_retries (int, optional): Maximum number of retry attempts on failure. Defaults to 3.

    Returns:
        str: The JSON response from the API as a string.

    Raises:
        Exception: If the request fails after the specified number of retries, or if the response status code is not 200.
    """
    if headers is None:
        headers = {
            "Content-Type": "application/json",
        }
        headers["Authorization"] = f"Bearer {token}"

    request_url = f"https://api.fabric.microsoft.com/{request_url.lstrip('/')}"
    for attempt in range(1, max_retries + 1):
        response = requests.request(
            request_type,
            request_url,
            headers=headers,
            files=files,
        )
        if response.status_code == 200:
            return response.json()
        if attempt < max_retries:
            print(
                f"Request failed (attempt {attempt}), retrying in 3 seconds...")
            time.sleep(3)
        else:
            msg = f"Fabric API request failed after {max_retries} attempts with status code {response.status_code}: {response.text}. URL: {request_url}"
            raise Exception(
                msg
            )
    return None


def _get_fabric_environment_state(token: str, workspace_id: str, environment_id: str) -> str:
    """
    Retrieves the state of a specific Fabric environment.

    Args:
        token (str): The authentication token for the Fabric API.
        workspace_id (str): The ID of the workspace containing the environment.
        environment_id (str): The ID of the environment whose state is to be retrieved.

    Returns:
        str: The state of the specified Fabric environment.

    Raises:
        Exception: If the API response does not contain the expected keys.
    """
    environment_details = _fabric_api_request(
        "GET", token, f"workspaces/{workspace_id}/environments/{environment_id}")
    try:
        return environment_details["properties"]["publishDetails"]["state"]
    except KeyError as e:
        msg = f"Incorrect API response {environment_details}"
        raise Exception(
            msg) from e


def _get_fabric_environment_custom_libraries(token: str, workspace_id: str, environment_id: str) -> dict:
    """
    Retrieves the custom libraries for a specific Fabric environment.

    Args:
        token (str): The authentication token to access the Fabric API.
        workspace_id (str): The ID of the workspace containing the environment.
        environment_id (str): The ID of the environment whose libraries are to be retrieved.

    Returns:
        dict: A dictionary containing information about the custom libraries in the specified environment.
    """
    return _fabric_api_request("GET", token, f"workspaces/{workspace_id}/environments/{environment_id}/libraries")


def _delete_fabric_environment_custom_library(token: str, workspace_id: str, environment_id: str, library_name: str) -> dict:
    """
    Deletes a custom library from a specified Fabric environment.

    Args:
        token (str): The authentication token used for API requests.
        workspace_id (str): The ID of the workspace containing the environment.
        environment_id (str): The ID of the environment from which the library will be deleted.
        library_name (str): The name of the custom library to delete.

    Returns:
        dict: The response from the Fabric API after attempting to delete the library.

    Raises:
        requests.HTTPError: If the API request fails.
    """
    encoded_library_name = urllib.parse.quote(library_name)
    return _fabric_api_request("DELETE", token,
                               f"workspaces/{workspace_id}/environments/{environment_id}/staging/libraries?libraryToDelete={encoded_library_name}"
                               )


def _upload_fabric_environment_custom_library(token: str, workspace_id: str, environment_id: str, file_path: str) -> dict:
    with Path(file_path).open('rb') as file:
        files = {'file': (Path(file_path).name, file,
                          'application/octet-stream')}

        response = _fabric_api_request("POST", token,
                                       f"workspaces/{workspace_id}/environments/{environment_id}/staging/libraries",
                                       files=files,
                                       headers={}
                                       )
        return response.json()


def _delete_fabric_environment_published_custom_libraries(token: str, workspace_id: str, environment_id: str) -> None:
    libraries = _get_fabric_environment_custom_libraries(
        token, workspace_id, environment_id)
    if "customLibraries" in libraries and "wheelFiles" in libraries["customLibraries"]:
        for library_name in libraries["customLibraries"]['wheelFiles']:
            _delete_fabric_environment_custom_library(
                token, workspace_id, environment_id, library_name)


def _cancel_fabric_environment_publish(token: str, workspace_id: str, environment_id: str) -> None:
    """
    Cancels the publish operation for a specific Fabric environment.

    Args:
        token (str): The authentication token to access the Fabric API.
        workspace_id (str): The unique identifier of the workspace containing the environment.
        environment_id (str): The unique identifier of the environment whose publish operation should be canceled.

    Raises:
        requests.HTTPError: If the API request fails.
    """
    _fabric_api_request("POST", token,
                        f"workspaces/{workspace_id}/environments/{environment_id}/staging/cancelPublish")


def _publish_fabric_environment(token: str, workspace_id: str, environment_id: str) -> None:
    """
    Publishes a Fabric environment to staging.

    This function sends a POST request to the Fabric API to publish the specified environment
    within a given workspace. Upon successful publishing, a confirmation message is printed.

    Args:
        token (str): The authentication token for the Fabric API.
        workspace_id (str): The ID of the workspace containing the environment.
        environment_id (str): The ID of the environment to be published.

    Raises:
        Any exceptions raised by the underlying _fabric_api_request function.
    """
    _fabric_api_request("POST", token,
                        f"workspaces/{workspace_id}/environments/{environment_id}/staging/publish"
                        )
    print(f"Environment {environment_id} published successfully")


def _is_fabric_environment_published(token: str, workspace_id: str, environment_id: str, allow_cancelled: bool = False) -> bool:
    """
    Checks if a Fabric environment has been published successfully.

    This function retrieves the current state of the specified Fabric environment and determines
    whether it has been published. If the environment is in a "Success" state, it returns True.
    If the environment is in a "Failed" state, or in a "Cancelled" state (unless `allow_cancelled`
    is True), it raises an Exception. Otherwise, it returns False, indicating the environment
    is not yet published.

    Args:
        token (str): The authentication token to access the Fabric API.
        workspace_id (str): The ID of the workspace containing the environment.
        environment_id (str): The ID of the environment to check.
        allow_cancelled (bool, optional): Whether to treat a "Cancelled" state as a non-error.
            Defaults to False.

    Returns:
        bool: True if the environment is published successfully, False if it is still in progress.

    Raises:
        Exception: If the environment state is "Failed" or "Cancelled" (unless `allow_cancelled` is True).
    """
    state = _get_fabric_environment_state(token, workspace_id, environment_id)
    if state == "Success":
        return True

    if state == "Failed" or (not allow_cancelled and state == "Cancelled"):
        msg = f"Environment {environment_id} failed to publish with state: {state}"
        raise Exception(
            msg
        )
    return False  # Else, the environment is not published yet


def _wait_until_fabric_environment_publish_finished(token: str, workspace_id: str, environment_id: str, allow_cancelled: bool = False, timeout_in_minutes: int = 40) -> bool:
    """
    Waits until the specified Fabric environment is published or until a timeout is reached.

    This function repeatedly checks the publish status of a Fabric environment and waits until it is published,
    or until the specified timeout period elapses. Optionally, environments that are cancelled can be considered as published.

    Args:
        token (str): Authentication token for accessing the Fabric environment.
        workspace_id (str): The ID of the workspace containing the environment.
        environment_id (str): The ID of the environment to check.
        allow_cancelled (bool, optional): If True, treat cancelled environments as published. Defaults to False.
        timeout_in_minutes (int, optional): Maximum time to wait for the environment to be published, in minutes. Defaults to 40.

    Returns:
        bool: True if the environment is published (or cancelled if allowed) before the timeout.

    Raises:
        TimeoutError: If the environment is not published within the specified timeout period.
    """
    start_time = time.time()
    while True:
        if _is_fabric_environment_published(token, workspace_id, environment_id, allow_cancelled):
            print(f"Environment {environment_id} is published successfully.")
            return True
        if time.time() - start_time > timeout_in_minutes * 60:
            msg = f"Timeout reached while waiting for environment {environment_id} to be published"
            raise TimeoutError(
                msg
            )
        print("Waiting for environment to be published, checking again in 30 seconds...")
        time.sleep(30)  # Wait 30 seconds before checking again


def run_wheel_deployment_to_fabric(token: str, workspace_id: str, environment_id: str, file_path: str) -> None:
    """
    Deploys a wheel file to a specified Fabric environment.

    This function manages the deployment process of a Python wheel file to a Fabric environment.
    It ensures the environment is in the correct state, cancels any ongoing publish operations if necessary,
    deletes existing custom libraries, uploads the new wheel file, and publishes the environment.

    Args:
        token (str): The authentication token for accessing the Fabric API.
        workspace_id (str): The ID of the workspace containing the environment.
        environment_id (str): The ID of the Fabric environment to deploy to.
        file_path (str): The local file path to the wheel file to be deployed.

    Raises:
        Exception: If any error occurs during the deployment process.
    """
    try:
        # First, check if the environment is in published state
        if _get_fabric_environment_state(token, workspace_id, environment_id) != "Success":
            print("Cancelling earlier publish....")
            _cancel_fabric_environment_publish(
                token, workspace_id, environment_id)
            _wait_until_fabric_environment_publish_finished(
                token, workspace_id, environment_id, allow_cancelled=True)

        # We need to delete the custom libraries already in the environment
        _delete_fabric_environment_published_custom_libraries(
            token, workspace_id, environment_id)
        _upload_fabric_environment_custom_library(
            token, workspace_id, environment_id, file_path)

        _publish_fabric_environment(token, workspace_id, environment_id)

        _wait_until_fabric_environment_publish_finished(
            token, workspace_id, environment_id)

        print(
            f"Deployment of {file_path} to environment {environment_id} completed successfully.")
    except Exception as e:
        print(f"An error occurred during deployment: {e}")
        raise e


if __name__ == "__main__":
    CLIENT_ID = os.getenv("FABRIC_CLIENT_ID")
    CLIENT_SECRET = os.getenv("FABRIC_CLIENT_SECRET")
    TENANT_ID = os.getenv("FABRIC_TENANT_ID")
    WORKSPACE_ID = os.getenv("FABRIC_WORKSPACE_ID")
    ENVIRONMENT_ID = os.getenv("FABRIC_ENVIRONMENT_ID")
    FILE_PATH = os.getenv("FABRIC_FILE_PATH")

    print("CLIENT_ID:", CLIENT_ID)
    print("CLIENT_SECRET:", CLIENT_SECRET)
    print("TENANT_ID:", TENANT_ID)
    print("WORKSPACE_ID:", WORKSPACE_ID)
    print("ENVIRONMENT_ID:", ENVIRONMENT_ID)
    print("FILE_PATH:", FILE_PATH)

    if not all([CLIENT_ID, CLIENT_SECRET, TENANT_ID, WORKSPACE_ID, ENVIRONMENT_ID, FILE_PATH]):
        error_message = "One or more environment variables are not set."
        raise ValueError(error_message)

    token = _get_fabric_api_token(CLIENT_ID, CLIENT_SECRET, TENANT_ID)
    run_wheel_deployment_to_fabric(
        token, WORKSPACE_ID, ENVIRONMENT_ID, FILE_PATH)
