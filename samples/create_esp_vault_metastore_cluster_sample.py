from azure.mgmt.hdinsight import HDInsightManagementClient
from azure.common.credentials import ServicePrincipalCredentials
from hdinsights.settings import *
from azure.mgmt.hdinsight.models import *

def main():
    # Authentication
    credentials = ServicePrincipalCredentials(
        client_id=CLIENT_ID,
        secret=CLIENT_SECRET,
        tenant=TENANT_ID
    )

    client = HDInsightManagementClient(credentials, SUBSCRIPTION_ID)

    # Parse AAD-DS DNS Domain name from resource id
    aadds_dns_domain_name = AADDS_RESOURCE_ID.split('/')[-1]

    # Prepare cluster create parameters
    create_params = ClusterCreateParametersExtended(
        location=LOCATION,
        tags={},
        properties=ClusterCreateProperties(
            cluster_version="3.6",
            os_type=OSType.linux,
            tier=Tier.premium,
            cluster_definition=ClusterDefinition(
                kind="Spark",
                configurations={
                    "gateway": {
                        "restAuthCredential.isEnabled": "true",
                        "restAuthCredential.username": CLUSTER_LOGIN_USER_NAME,
                        "restAuthCredential.password": PASSWORD
                    },
                    "hive-site": {
                        "javax.jdo.option.ConnectionDriverName": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                        "javax.jdo.option.ConnectionURL":        "jdbc:sqlserver://%s;database=%s;encrypt=true;trustServerCertificate=true;create=false;loginTimeout=300".format(METASTORE_SQL_SERVER, METASTORE_SQL_DATABASE),
                        "javax.jdo.option.ConnectionUserName":   METASTORE_SQL_USERNAME,
                        "javax.jdo.option.ConnectionPassword":   METASTORE_SQL_PASSWORD,
                    },
                    "hive-env": {
                        "hive_database":                       "Existing MSSQL Server database with SQL authentication",
                        "hive_database_name":                  METASTORE_SQL_DATABASE,
                        "hive_database_type":                  "mssql",
                        "hive_existing_mssql_server_database": METASTORE_SQL_DATABASE,
                        "hive_existing_mssql_server_host":     METASTORE_SQL_SERVER,
                        "hive_hostname":                       METASTORE_SQL_SERVER,
                    },
                    "ambari-conf": {
                        "database-server":        METASTORE_SQL_SERVER,
                        "database-name":          AMBARI_SQL_DATABASE,
                        "database-user-name":     AMBARI_SQL_USERNAME,
                        "database-user-password": AMBARI_SQL_PASSWORD,
                    },
                    "admin-properties": {
                        "audit_db_name": METASTORE_SQL_DATABASE,
                        "audit_db_user": METASTORE_SQL_USERNAME,
                        "audit_db_password": METASTORE_SQL_PASSWORD,
                        "db_name": METASTORE_SQL_DATABASE,
                        "db_user": METASTORE_SQL_USERNAME,
                        "db_password": METASTORE_SQL_PASSWORD,
                        "db_host": METASTORE_SQL_SERVER,
                        "db_root_user": "",
                        "db_root_password": ""
                    },
                    "ranger-admin-site": {
                        "ranger.jpa.jdbc.url": "jdbc:sqlserver://%s;databaseName==%s".format(METASTORE_SQL_SERVER, METASTORE_SQL_DATABASE)
                    },
                    "ranger-env": {
                        "ranger_privelege_user_jdbc_url": "jdbc:sqlserver://%s;databaseName==%s".format(METASTORE_SQL_SERVER, METASTORE_SQL_DATABASE)
                    },
                    "ranger-hive-security": {
                        "ranger.plugin.hive.service.name": RANGER_HIVE_PLUGIN_SERVICE_NAME
                    },
                    "ranger-yarn-security": {
                        "ranger.plugin.yarn.service.name": RANGER_HIVE_PLUGIN_SERVICE_NAME
                    }
                }
            ),
            compute_profile=ComputeProfile(
                roles=[
                    Role(
                        name="headnode",
                        target_instance_count=2,
                        hardware_profile=HardwareProfile(vm_size="Large"),
                        os_profile=OsProfile(
                            linux_operating_system_profile=LinuxOperatingSystemProfile(
                                username=SSH_USER_NAME,
                                password=PASSWORD
                            )
                        ),
                        virtual_network_profile=VirtualNetworkProfile(
                            id=VIRTUAL_NETWORK_RESOURCE_ID,
                            subnet='{}/subnets/{}'.format(VIRTUAL_NETWORK_RESOURCE_ID, SUBNET_NAME)
                        )
                    ),
                    Role(
                        name="workernode",
                        target_instance_count=3,
                        hardware_profile=HardwareProfile(vm_size="Large"),
                        os_profile=OsProfile(
                            linux_operating_system_profile=LinuxOperatingSystemProfile(
                                username=SSH_USER_NAME,
                                password=PASSWORD
                            )
                        ),
                        virtual_network_profile=VirtualNetworkProfile(
                            id=VIRTUAL_NETWORK_RESOURCE_ID,
                            subnet='{}/subnets/{}'.format(VIRTUAL_NETWORK_RESOURCE_ID, SUBNET_NAME)
                        )
                    )
                ]
            ),
            storage_profile=StorageProfile(
                storageaccounts=[
                    StorageAccount(
                        name=STORAGE_ACCOUNT_NAME + BLOB_ENDPOINT_SUFFIX,
                        key=STORAGE_ACCOUNT_KEY,
                        container=CONTAINER_NAME.lower(),
                        is_default=True
                    )
                ]
            ),
            security_profile=SecurityProfile(
                directory_type=DirectoryType.active_directory,
                ldaps_urls=[LDAPS_URL],
                domain_username=DOMAIN_USER_NAME,
                domain=aadds_dns_domain_name,
                cluster_users_group_dns=[CLUSTER_ACCESS_GROUP],
                aadds_resource_id=AADDS_RESOURCE_ID,
                msi_resource_id=MANAGED_IDENTITY_RESOURCE_ID
            ),
            disk_encryption_properties=DiskEncryptionProperties(
                vault_uri=ENCRYPTION_VAULT_URI,
                key_name=ENCRYPTION_KEY_NAME,
                key_version=ENCRYPTION_KEY_VERSION,
                encryption_algorithm=ENCRYPTION_ALGORITHM,
                msi_resource_id=ASSIGN_IDENTITY
            )
        ),
        identity=ClusterIdentity(
            type=ResourceIdentityType.user_assigned,
            user_assigned_identities={MANAGED_IDENTITY_RESOURCE_ID: {}}
        )
    )

    print('Starting to create HDInsight Spark cluster {} with Enterprise Security Package'.format(CLUSTER_NAME))
    create_poller = client.clusters.create(RESOURCE_GROUP_NAME, CLUSTER_NAME, create_params)
    cluster_response = create_poller.result()

    if CLUSTER_NAME == cluster_response.name & cluster_response.id.endswith(CLUSTER_NAME) & "Running" == cluster_response.properties.cluster_state & "Microsoft.HDInsight/clusters" & cluster_response.type:
        return 0
    return 1

if __name__ == "__main__":
    main()