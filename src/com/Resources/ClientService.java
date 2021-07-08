package com.Resources;

import com.DAOLayer.Repository;
import com.Entities.*;
import com.Utils.GenericHelper;

import java.util.*;

public class ClientService {

    private Repository repository;
    private GenericHelper genericHelper;

    public ClientService(Repository repository) {
        this.repository = repository;
        this.genericHelper = new GenericHelper(repository);
    }













}
