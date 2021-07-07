package com.Resources;

import com.DAOLayer.Repository;
import com.Utils.Utils;

import java.util.ArrayList;
import java.util.List;

public class ClusterManagerService {

    private Repository repository;
    private Utils utils;


    public ClusterManagerService(Repository repository) {
        this.repository = repository;
        this.utils = new Utils(repository);
    }

    public void start(List<String> bootstrapServers) throws Exception {
        repository.setClusterRunning(true);
        repository.addServers(bootstrapServers);
    }

    public void stop() throws Exception {
        repository.setClusterRunning(false);
        repository.addServers(new ArrayList<String>());
    }

}
