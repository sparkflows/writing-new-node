# hdfs://development-build-machine:8020/user/sparkflows/data/Lokad_Items.tsv


class SparkPathUtil:

    @staticmethod
    def getSparkPath(configurationService, path: str, userName: str):
        temp = path.lower()

        if not configurationService.app_runOnCluster:
            return path

        if path.startswith("/"):
            temp = configurationService.hdfs_namenodeURI + path
            return temp

        temp = configurationService.hdfs_namenodeURI + \
            configurationService.hdfs_homeDir + "/" + userName + path

        return path
