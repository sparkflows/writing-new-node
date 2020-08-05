class ConfigurationService:
    def __init__(
            self,
            app_runOnCluster: bool,
            app_impersonateUsers: bool,
            app_postMessageURL: str,
            kerberos_enabled: bool,
            kerberos_keytab: str,
            kerberos_principal: str,
            kerberos_KERBEROS_REALM: str,
            kerberos_KERBEROS_KDC: str,
            hdfs_namenodeURI: str,
            hdfs_homeDir: str,
            hadoop_HADOOP_CONF_DIR: str,
            hive_JDBC_DRIVER: str,
            hive_JDBC_DB_URL: str):
        self.app_runOnCluster = app_runOnCluster
        self.app_impersonateUsers = app_impersonateUsers
        self.app_postMessageURL = app_postMessageURL
        self.kerberos_enabled = kerberos_enabled
        self.kerberos_keytab = kerberos_keytab
        self.kerberos_principal = kerberos_principal
        self.kerberos_KERBEROS_KDC = kerberos_KERBEROS_KDC
        self.kerberos_KERBEROS_REALM = kerberos_KERBEROS_REALM
        self.hive_JDBC_DB_URL = hive_JDBC_DB_URL
        self.hive_JDBC_DRIVER = hive_JDBC_DRIVER
        self.hdfs_homeDir = hdfs_homeDir
        self.hdfs_namenodeURI = hdfs_namenodeURI
        self.hadoop_HADOOP_CONF_DIR = hadoop_HADOOP_CONF_DIR
