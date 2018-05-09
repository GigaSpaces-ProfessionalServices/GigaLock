package com.avaya.workflow.impl.edp.lock;

import com.gigaspaces.annotation.pojo.SpaceId;

import java.io.Serializable;

public class GSLockEntry {

    private Serializable key;

    public GSLockEntry(){
    }

    public GSLockEntry(Serializable key){
        this.key = key;
    }

    @SpaceId
    public Serializable getKey() {
        return key;
    }

    public void setKey(Serializable key) {
        this.key = key;
    }
}