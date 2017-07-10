package test;

import eu.trentorise.opendata.jackan.CkanClient;
import eu.trentorise.opendata.jackan.model.CkanDataset;

public class TestCKAN2 {

    public static void main(String[] args) {
        CkanClient cc = new CkanClient("http://linkeddatacatalog.dws.informatik.uni-mannheim.de");
        String name = "rkb-explorer-acm";
        name = "3f3a5534-bafd-451f-9c2e-587a823c5f09";
        CkanDataset d = cc.getDataset(name);
        System.out.println(cc.getCatalogUrl());
        System.out.println(d.getId());
        System.out.println(d.getName());
        System.out.println(d.getNotes());

    }
}
