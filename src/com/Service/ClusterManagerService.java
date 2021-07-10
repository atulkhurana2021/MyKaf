package com.Service;

import com.DAL.Repository;
import com.Utils.GenericHelper;

import java.util.ArrayList;
import java.util.List;

public class ClusterManagerService {

    private Repository repository;
    private GenericHelper genericHelper;


    public ClusterManagerService(Repository repository) {
        this.repository = repository;
        this.genericHelper = new GenericHelper(repository);
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
