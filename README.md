# Kris VaultSpeed Generic FMC to Dagster orchestration tool

Containerized python application which takes a GitHub FMC deployment from Vaultspeed and generates dagster project artifacts needed for orchistration

Containers are stored in AWS ECR for distribution via access key

## Adding access to repo should follow the steps below
1. Log into the AWS console and navigate to IAM
2. Open the users page under Access Management on the left side of the screen
3. Create a new user with the name of the organization we are giving access to
4. On the settings page, select attach policies directly and search for "ECR_Pull_Access". Select the radio button and click next
5. Verify settings and create the user
6. Click on the new username to open the settings and click the security credentials tab
7. Navigate down to access keys and create access key
8. Select Command Line Interface and confirm at the bottom of the page before selecting next
9. Confirm creation of the key and copy the key name and key secret.  These will be provided to the customer to configure their access and will only be displayed while you are on the current page.
10. Once the client has the information, they can configure their VS/Dagster repository to convert their files.

## Revision History
0.2 - Initial release with single source handling (does not work correctly with more than 1 source)
0.3 - Multisource Updates: Complex sensor definition for multisource, Cleanup of asset flow to integrate load cycle ID into intial flow stored procedure. Does not support object based loading.
